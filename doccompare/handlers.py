import json
import uuid
import base64
import os
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .config import bucket, lambda_client, s3_client
from .db import DatabaseManager
from .storage import StorageManager
from .pdf_extractor import PDFExtractor
from .comparator import DocumentComparator

logger = logging.getLogger(__name__)


@dataclass
class EventHandler:
    db: DatabaseManager
    storage: StorageManager
    extractor: PDFExtractor
    comparator: DocumentComparator

    def _token_optix_insertion(self, payload):
        try:
            lambda_client.invoke(
                FunctionName='prod-token-optix-writer',
                InvocationType='Event',
                Payload=json.dumps(payload).encode('utf-8')
            )
            return {"status": "success"}
        except Exception as e:
            print("Exception:", e)
            return {"status": "failure"}

    def _detect_text(self, unique_id, folder_prefix, document_type):
        self.db.update_time((f"triggering time of {document_type}", unique_id))
        payload = {
            "event_type": "extract-pdf",
            "bucket": bucket,
            "object_key": folder_prefix,
            "unique_id": unique_id,
            "document_type": document_type
        }
        lambda_client.invoke(
            FunctionName=os.environ.get('LAMBDA_FUNCTION_NAME', 'doccompare-field-extract-v3-prod'),
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

    def _invoke_compare_operation(self, unique_id, path_type, path):
        Digital_doc = ""
        Handscanned_doc = ""
        file_need_to_upload = "Hand Scanned"
        sub_query = "Digital_doc=%s"
        if path_type == 'Handscanned':
            Handscanned_doc = path
            file_need_to_upload = "Digital Scanned"
            sub_query = "Handscanned_doc=%s"
        else:
            Digital_doc = path
        sub_query += ", status = 'In progress'"
        try:
            query = "select * from document_compare_results where conversation_id=%s"
            output = self.db.fetch(query, (unique_id,))
            if len(output) == 0:
                raise ValueError("cannot update field")
            query = f"UPDATE document_compare_results SET {sub_query} WHERE conversation_id=%s"
            self.db.update(query, (path, unique_id))
        except Exception as e:
            logger.warning("Exception on update file path, inserting new row: %s", e)
            test_type = "v3"
            status = f'{file_need_to_upload} document yet to upload.'
            query = '''insert into document_compare_results(conversation_id, digital_doc, handscanned_doc, "timestamp", status, test_type)values(%s,%s,%s,NOW(), %s, %s)'''
            self.db.insert(query, (unique_id, Digital_doc, Handscanned_doc, status, test_type))
        if self.db.try_initiate_extraction(unique_id):
            self._invoke_extract_both(unique_id)

    def _invoke_extract_both(self, unique_id):
        self.db.update_time(("triggering time of extract-both", unique_id))
        payload = {"event_type": "extract-both", "bucket": bucket, "unique_id": unique_id}
        lambda_client.invoke(
            FunctionName=os.environ.get('LAMBDA_FUNCTION_NAME', 'doccompare-field-extract-v3-prod'),
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

    # ── file-upload ───────────────────────────────────────────────────────────

    def handle_file_upload(self, event):
        unique_id = event.get('unique_id', '')
        try:
            file_type = event['file_type']
            get_file_content = event["content"]
            message = 'It is not a valid file format'
            statusCode = 500

            if file_type == 'DigitalScanned' or file_type == 'Handscanned':
                object_key = file_type
                file = object_key + '_' + unique_id + '.pdf'
                decode_content = base64.b64decode(get_file_content)

                if len(unique_id) == 0:
                    message = "upload failed"
                    raise ValueError(message)

                if self.storage.is_base64_image(decode_content):
                    message = "Unsupported image format. Please upload a PDF."
                    logger.warning("Upload rejected for unique_id=%s: %s", unique_id, message)
                    raise ValueError(message)

                prefix = object_key + '/' + unique_id + '/' + file
                logger.info("Uploading file: prefix=%s", prefix)

                s3_client.put_object(Bucket=bucket, Key=prefix, Body=decode_content)
                document_valid = self.storage.validate_document(bucket, prefix)

                logger.info("Document validation result for unique_id=%s: %s", unique_id, document_valid)
                if "Invalid document" in document_valid:
                    message = 'It is not a valid file format'
                    raise ValueError(message)

                statusCode = 200
                message = 'uploaded successfully'
                self._invoke_compare_operation(unique_id, file_type, prefix)

            self.db.update_time((f"upload time of {file_type}", unique_id))
        except Exception as e:
            logger.error("File upload error for unique_id=%s: %s", unique_id, e, exc_info=True)
            statusCode = 500

        logger.info("Upload result for unique_id=%s: %s", unique_id, message)
        return {
            'statusCode': statusCode,
            'unique_id': unique_id,
            'body': json.dumps(message)
        }

    # ── extract-pdf ───────────────────────────────────────────────────────────

    def handle_extract_pdf(self, event):
        unique_id = event.get('unique_id', '')
        detect_text_insights = {}

        try:
            bucket_name = event['bucket']
            object_key = event['object_key']
            self.db.update_time((f"extraction arrival time: {object_key}", unique_id))

            DOWNLOAD_PATH = f"/tmp/{uuid.uuid4()}.pdf"
            logger.info("extract-pdf: unique_id=%s object_key=%s", unique_id, object_key)

            self.storage.download_file(bucket_name, object_key, DOWNLOAD_PATH)
            input_tokens = 0
            output_tokens = 0
            document_word_count = {}
            page_wise_scanned_text = []
            combined_input_prompt = ""

            if 'DigitalScanned' in object_key:
                document_word_count, page_wise_scanned_text = self.extractor.extract_digital(DOWNLOAD_PATH, unique_id)
                self.db.update_time(("end of digital", unique_id))
            else:
                document_word_count, page_wise_scanned_text, input_tokens, output_tokens, combined_input_prompt = \
                    self.extractor.extract_scanned(DOWNLOAD_PATH, unique_id)
                self.db.update_time(("end of qwen", unique_id))

            os.remove(DOWNLOAD_PATH)

            detect_text_insights = {
                'document_word_count': document_word_count,
                'document_page_count': len(page_wise_scanned_text),
                'page_wise_scanned_text': page_wise_scanned_text,
                'object_key': object_key,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "combined_input_prompt": combined_input_prompt
            }

            document_type = event['document_type']
            sub_query = 'hand_scanned_results=%s' if document_type == 'Handscanned' else 'digital_scanned_results=%s'
            query = f"UPDATE document_compare_results SET {sub_query} WHERE conversation_id=%s"
            self.db.update(query, (json.dumps(detect_text_insights), unique_id))

            self.db.update_time(("try initate compare start", unique_id))
            if self.db.try_initiate_compare(unique_id):
                self.db.update_time(("Invoke document compare", unique_id))
                self.handle_document_compare_insights(unique_id)

            self.db.update_time(("try initate end", unique_id))
            return {'statusCode': 200, 'body': json.dumps(detect_text_insights)}

        except Exception as e:
            error_trace = traceback.format_exc()
            logger.exception("extract-pdf failed for unique_id=%s", unique_id)
            error_message = f"[ERROR]: {str(e)}\nError Trace: {error_trace}"
            self.db.update(
                "UPDATE document_compare_results SET status=%s, error_message = %s WHERE conversation_id=%s",
                ('failed', error_message, unique_id)
            )
            return {'statusCode': 500, 'body': json.dumps(detect_text_insights)}

    # ── extract-both ──────────────────────────────────────────────────────────

    def handle_extract_both(self, event):
        unique_id = event.get('unique_id', '')
        bucket_name = event.get('bucket', bucket)
        self.db.update_time(("Reaching lambda function: extract-both for unique_id=", unique_id)) 
        try:
            row = self.db.fetch(
                "SELECT digital_doc, handscanned_doc FROM document_compare_results WHERE conversation_id=%s",
                (unique_id,)
            )
            if not row:
                raise ValueError(f"No DB row for unique_id={unique_id}")
            digital_key, handscanned_key = row[0][0], row[0][1]

            digital_path     = f"/tmp/{uuid.uuid4()}_digital.pdf"
            handscanned_path = f"/tmp/{uuid.uuid4()}_handscanned.pdf"
            self.storage.download_file(bucket_name, digital_key,     digital_path)
            self.storage.download_file(bucket_name, handscanned_key, handscanned_path)

            def run_digital():
                wc, pages = self.extractor.extract_digital(digital_path, unique_id)
                self.db.update_time(("end of digital", unique_id))
                return {'document_word_count': wc, 'document_page_count': len(pages),
                        'page_wise_scanned_text': pages, 'object_key': digital_key,
                        'input_tokens': 0, 'output_tokens': 0, 'combined_input_prompt': ''}

            def run_handscanned():
                wc, pages, in_tok, out_tok, prompt = self.extractor.extract_scanned(handscanned_path, unique_id)
                self.db.update_time(("end of qwen", unique_id))
                return {'document_word_count': wc, 'document_page_count': len(pages),
                        'page_wise_scanned_text': pages, 'object_key': handscanned_key,
                        'input_tokens': in_tok, 'output_tokens': out_tok, 'combined_input_prompt': prompt}

            with ThreadPoolExecutor(max_workers=2) as executor:
                fut_d = executor.submit(run_digital)
                fut_h = executor.submit(run_handscanned)
                digital_result     = fut_d.result()
                handscanned_result = fut_h.result()

            self.db.update(
                "UPDATE document_compare_results SET digital_scanned_results=%s WHERE conversation_id=%s",
                (json.dumps(digital_result), unique_id)
            )
            self.db.update(
                "UPDATE document_compare_results SET hand_scanned_results=%s WHERE conversation_id=%s",
                (json.dumps(handscanned_result), unique_id)
            )
            for p in (digital_path, handscanned_path):
                self.storage.delete_file(p)

            self.db.update_time(("try initiate compare start", unique_id))
            if self.db.try_initiate_compare(unique_id):
                self.db.update_time(("Invoke document compare", unique_id))
                self.handle_document_compare_insights(unique_id)
            self.db.update_time(("try initiate end", unique_id))
            return {'statusCode': 200, 'body': json.dumps('extract-both complete')}

        except Exception as e:
            error_trace = traceback.format_exc()
            logger.exception("extract-both failed for unique_id=%s", unique_id)
            self.db.update(
                "UPDATE document_compare_results SET status=%s, error_message=%s WHERE conversation_id=%s",
                ('failed', f"[ERROR]: {str(e)}\nError Trace: {error_trace}", unique_id)
            )
            return {'statusCode': 500, 'body': json.dumps(str(e))}

    # ── document_compare_insights ─────────────────────────────────────────────

    def handle_document_compare_insights(self, unique_id):
        hand_scanned_document_word_count = {}
        digital_scanned_document_word_count = {}
        digital_scanned_page_count = 0
        handscanned_page_count = 0
        total_page_count = 0
        input_token = {"qwen_comparator": 0, "qwen": 0}
        output_token = {"qwen_comparator": 0, "qwen": 0}
        
        load_hand = []
        result = ""
        load_digital = []
        Digital_doc = ""
        Handscanned_doc = ""
        model_execution = True
        minimum_similarity_score = 10000
        output_file_path = ""
        status = "completed"
        answer = ""
        output_dict = {
            "generic": {
                "total_page_count": 0,
                "handscanned_page_count": 0,
                "digital_scanned_page_count": 0,
                "jumbled_status": False
            },
            "pages": [],
            "failure_message": ""
        }
        qwen_comparator_combined_input_prompt = ""
        qwen_combined_input_prompt = ""
        qwen_combined_output_prompt = ""

        try:
            self.db.update_time(("iniating qwen_comparator", unique_id))
            query = """select hand_scanned_results, digital_scanned_results from document_compare_results
                    where conversation_id=%s"""
            output = self.db.fetch(query, (unique_id,))

            hand_scanned_info = json.loads(output[0][0].strip())
            digital_doc_info = json.loads(output[0][1].strip())

            hand_scanned_document_word_count = hand_scanned_info['document_word_count']
            handscanned_page_count = hand_scanned_info['document_page_count']
            load_hand = hand_scanned_info['page_wise_scanned_text']
            hand_scanned = "".join(load_hand)
            Handscanned_doc = hand_scanned_info['object_key']
            hand = hand_scanned_info['document_page_count']
            input_token['qwen'] = hand_scanned_info['input_tokens']
            output_token['qwen'] = hand_scanned_info['output_tokens']
            qwen_combined_input_prompt = hand_scanned_info['combined_input_prompt']
            qwen_combined_output_prompt = hand_scanned

            logger.info("Hand scanned: page_count=%s doc=%s", hand, Handscanned_doc)

            digital_scanned_document_word_count = digital_doc_info['document_word_count']
            digital_scanned_page_count = digital_doc_info['document_page_count']
            load_digital = digital_doc_info['page_wise_scanned_text']
            digital_scanned = "".join(load_digital)
            Digital_doc = digital_doc_info['object_key']
            digital = digital_doc_info['document_page_count']
            logger.info("Digital scanned: page_count=%s doc=%s", digital, Digital_doc)

            response = self.comparator.check_similarity(unique_id, load_hand, load_digital)
            raw_payload = response['Payload'].read().decode('utf-8')
            minimum_similarity_score = json.loads(json.loads(raw_payload)['body'])['status']
            logger.info("minimum_similarity_score=%s digital_pages=%s hand_pages=%s",
                        minimum_similarity_score, digital_scanned_page_count, handscanned_page_count)

            total_page_count = digital_scanned_page_count + handscanned_page_count

            output_dict = {
                "generic": {
                    "total_page_count": total_page_count,
                    "handscanned_page_count": handscanned_page_count,
                    "digital_scanned_page_count": digital_scanned_page_count,
                    "jumbled_status": True
                },
                "pages": [],
                "failure_message": ""
            }

            if digital_scanned_page_count == 0:
                Digital_doc = ""
                output_dict["failure_message"] += "\ndigital document not found"

            if handscanned_page_count == 0:
                Handscanned_doc = ""
                output_dict["failure_message"] += "\nhand scanned document not found"

            if digital_scanned_page_count != 0 and handscanned_page_count != 0 and len(digital_scanned) != 0 and len(hand_scanned) != 0:
                if minimum_similarity_score < 0.7:
                    logger.warning("Similarity score %.4f below threshold — skipping model execution", minimum_similarity_score)
                    model_execution = False
                    output_dict = {
                        "generic": {
                            "total_page_count": total_page_count,
                            "handscanned_page_count": handscanned_page_count,
                            "digital_scanned_page_count": digital_scanned_page_count,
                            "jumbled_status": True
                        },
                        "pages": []
                    }

                if model_execution:
                    results, input_token['qwen_comparator'], output_token['qwen_comparator'], qwen_comparator_combined_input_prompt = \
                        self.comparator.process_documents(load_digital, load_hand)
                    
                    result_text = "\n\n".join(text for _, text in results)
                    query = "select prompt, model_id from prompt_management where task = 'comparison_validator' and prompt_status = 'Active';"
                    output = self.db.fetch(query)
                    system_prompt = output[0][0]
                    model_id = output[0][1]

                    output_dict, result = self.comparator.parse_comparison_result(
                        result_text, hand, digital,
                        hand_scanned_document_word_count, digital_scanned_document_word_count
                    )
                    status = "completed"

            output_file_path = f'doc_output/{unique_id}/output.json'
            answer = json.dumps(output_dict)
            self.db.update_time(("end after qwen_comparator", unique_id))

            logger.info("Token optix insertions complete for unique_id=%s", unique_id)
            ans = {"conversation_id": unique_id, "answer": output_dict}

            query = """UPDATE document_compare_results
                        SET result = %s, input_token = %s, output_token = %s, status = %s, pages = %s, output_file_path = %s, end_time = NOW()
                        where conversation_id=%s"""
            self.db.update(query, (str(answer), str(input_token), str(output_token), status, total_page_count, output_file_path, unique_id))

            logger.info("Document compare insights complete: unique_id=%s digital=%s hand=%s",
                        unique_id, Digital_doc, Handscanned_doc)
            return {'statusCode': 200, 'body': json.dumps(ans)}

        except Exception as e:
            logger.error("File processing exception for unique_id=%s: %s", unique_id, e, exc_info=True)
            status = 'failed'
            error_trace = traceback.format_exc()
            error_message = f"[ERROR]: {str(e)}\nError Trace: {error_trace}"

            query = """UPDATE document_compare_results
                        SET result = %s, input_token = %s, output_token = %s, status = %s, pages = %s, output_file_path = %s, error_message = %s, end_time = NOW()
                        where conversation_id=%s"""
            self.db.update(query, (str(answer), str(input_token), str(output_token), status, total_page_count, output_file_path, error_message, unique_id))

            try:
                output_dict["failure_message"] += str(e)
            except Exception:
                output_dict["failure_message"] = str(e)

            ans = {"conversation_id": unique_id, "answer": output_dict}
            return {'statusCode': 200, 'body': json.dumps(ans)}

        finally:
            conversation_id = 'YkZ2tGJ0Hg'  # qwen llm
            user_name = 'fivestaruser'
            app_id = 7
            self._token_optix_insertion((
                conversation_id, qwen_combined_input_prompt, qwen_combined_output_prompt,
                0, len(qwen_combined_input_prompt), len(qwen_combined_output_prompt),
                user_name, unique_id, app_id, input_token['qwen'], output_token['qwen']
            ))
            conversation_id = 'YkZ2tGJ0Hg'  # qwen_comparator llm
            self._token_optix_insertion((
                conversation_id, qwen_comparator_combined_input_prompt, result,
                0, len(qwen_comparator_combined_input_prompt), len(result),
                user_name, unique_id, app_id, input_token['qwen_comparator'], output_token['qwen_comparator']
            ))

    # ── document_compare (status poll) ───────────────────────────────────────

    def handle_document_compare_status(self, unique_id):
        query = """select result, input_token, output_token, status from document_compare_results
                    where conversation_id=%s"""
        try:
            res = self.db.fetch(query, (unique_id,))
            if len(res) == 0:
                result = {"conversation_id": unique_id, "answer": '', "input_token": '', "output_token": '', "status": 'Files yet to upload'}
            else:
                result = {"conversation_id": unique_id, "answer": res[0][0], "input_token": res[0][1], "output_token": res[0][2], "status": res[0][3]}
        except Exception as e:
            logger.error("DB exception in document_compare: %s", e)
            result = {"conversation_id": unique_id, "answer": '', "input_token": '', "output_token": '', "status": 'DB exception'}
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
