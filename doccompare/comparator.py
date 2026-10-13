import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from .config import bedrock_client, lambda_client
from .db import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class DocumentComparator:
    db: DatabaseManager

    def _compare_page(self, page_no, hand_scanned, digital_text, system_prompt, model_id):
        input_prompt = (f"page_no: {page_no}\n") + str(system_prompt).replace(
            "{digital_scanned}", digital_text
        ).replace("{hand_scanned}", hand_scanned).replace("{page_no}", str(page_no))
        response = bedrock_client.converse(
            modelId=model_id,
            messages=[{"role": "user", "content": [{"text": input_prompt}]}],
            inferenceConfig={"temperature": 0.0, "maxTokens": 7000},
            additionalModelRequestFields={"include_reasoning": True, "reasoning_effort": "medium"}
        )

        output_text = ""
        for block in response["output"]["message"]["content"]:
            if "text" in block:
                output_text += block["text"]
        usage = response.get("usage", {})
        print("output_text:", output_text)
        return page_no, output_text, usage, input_prompt

    def process_documents(self, digital_pages, scanned_pages):
        MAX_WORKERS = 5
        query = "select prompt, model_id from prompt_management where task = 'compare_document' and prompt_status = 'Active';"
        output = self.db.fetch(query)
        system_prompt = output[0][0]
        model_id = output[0][1]
        results = []
        total_input_tokens = 0
        total_output_tokens = 0
        total_tokens = 0
        aggregated_input_prompt = ""
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(self._compare_page, i + 1, scanned_pages[i], digital_pages[i], system_prompt, model_id)
                for i in range(min(len(scanned_pages), len(digital_pages)))
            ]
            for future in as_completed(futures):
                page_no, comparison_json, usage, temp_input_prompt = future.result()
                total_input_tokens += usage.get("inputTokens", 0)
                total_output_tokens += usage.get("outputTokens", 0)
                total_tokens += usage.get("totalTokens", 0)
                aggregated_input_prompt += temp_input_prompt
                results.append((page_no, comparison_json))
        results.sort(key=lambda x: x[0])
        
        return results, total_input_tokens, total_output_tokens, aggregated_input_prompt

    def check_similarity(self, unique_id, handscanned_documents, digital_documents):
        response = lambda_client.invoke(
            FunctionName='doccompare-similarity-prod',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "event_type": "check_similarity",
                "documents": [handscanned_documents, digital_documents]
            })
        )
        logger.info("check_similarity invoked (RequestResponse)")
        return response

    def parse_document_fields(self, text):
        result = {}
        current_key = None
        current_value_lines = []

        for line in text.splitlines():
            if line.strip().startswith("Page ") and "=>" in line:
                page_match = line.strip().split("Page ")[-1].replace("=>", "").strip()
                result["page"] = int(page_match)
                current_key = None
                current_value_lines = []
                continue

            if " => " in line:
                if current_key is not None:
                    result[current_key] = "\n".join(current_value_lines).strip()
                parts = line.split(" => ", 1)
                current_key = parts[0].strip()
                current_value_lines = [parts[1].strip()]
            else:
                if current_key is not None and line.strip():
                    current_value_lines.append(line.strip())

        if current_key is not None:
            result[current_key] = "\n".join(current_value_lines).strip()

        return result

    def parse_comparison_result(self, results, hand_page_count, digital_page_count,
                                hand_word_count, digital_word_count):
        total_page_count = digital_page_count + hand_page_count
        results = results
        temp_list = results.split('\n\n')
        output_dict = {
            'generic': {
                'total_page_count': total_page_count,
                'handscanned_page_count': hand_page_count,
                'digital_scanned_page_count': digital_page_count,
                'jumbled_status': False
            },
            'pages': []
        }

        logger.debug("Parsed %s page blocks from result", len(temp_list))

        for i in range(len(temp_list)):
            try:
                # temp = temp_list[i].split('\n')
                # page = str(temp[0].replace('=>', "").strip()).lower()
                # difference_field = temp[1].strip().split('=>')[-1].replace('"', "").strip()
                # table_name = temp[2].strip().split('=>')[-1].replace('"', "").strip()
                # digital_scanned_document = temp[3].strip().split('=>')[-1].replace('"', "").strip()
                # handscanned_document = temp[4].strip().split('=>')[-1].replace('"', "").strip()
                # rule =  temp[5].strip().split('=>')[-1].replace('"', "").strip() or ""
                # reason =  temp[6].strip().split('=>')[-1].replace('"', "").strip() or ""
                # include = temp[7].strip().split('=>')[-1].replace('"', "").strip() or ""
                parsed = self.parse_document_fields(temp_list[i])
                page                   = str(parsed.get("page"))
                difference_field       = parsed.get("difference_field")
                table_name             = parsed.get("table_name")
                digital_scanned_document    = parsed.get("digital scanned document")
                handscanned_document        = parsed.get("handscanned document")
                reason                 = parsed.get("reason")
                rule                   = parsed.get("rule")
                include               = parsed.get("required")

                if "page" not in page:
                    page = f"page {page}"
                if include == 'REMOVE':
                    continue
                
                if difference_field == 'FIELD_NAME_HERE':
                    continue
                    
                if digital_scanned_document.strip() == handscanned_document.strip() or ('page' in digital_scanned_document.lower()) or ('livestar' in handscanned_document) or ('spelling' in difference_field.lower()) or ('wording' in difference_field.lower()) :
                    continue

                # if ('pre-closure' in difference_field.lower()):
                #     continue

                if ''.join((digital_scanned_document.strip().replace(' ', '').lower()).split()) == \
                   ''.join((handscanned_document.strip().lower()).replace(' ', '').split()):
                    continue

                if page not in [p['page'] for p in output_dict['pages']]:
                    output_dict['pages'].append({
                        'page': page,
                        'grid_difference': [],
                        'common_difference': [],
                        'Signature': "No",
                        'digital_scanned_document_word_count': 0,
                        'hand_scanned_document_word_count': 0
                    })

                page_dict = next(p for p in output_dict['pages'] if p['page'] == page)
                common_difference_iter = len(page_dict['common_difference']) + 1
                grid_difference_iter = len(page_dict['grid_difference']) + 1

                if difference_field.lower() == 'signature presence':
                    page_dict['Signature'] = "Yes"
                    # page_dict['common_difference'].append({
                    #     'error_no': common_difference_iter,
                    #     'handscanned_document': handscanned_document
                    # })
                elif table_name == "":
                    page_dict['common_difference'].append({
                        'error_no': common_difference_iter,
                        'difference_field': difference_field,
                        'digital_scanned_document': digital_scanned_document,
                        'handscanned_document': handscanned_document,
                        # 'reason': reason,
                        # 'rule': rule
                    })
                else:
                    page_dict['grid_difference'].append({
                        'difference_no': grid_difference_iter,
                        'table_name': table_name,
                        'difference_field': difference_field,
                        'digital_scanned_document': digital_scanned_document,
                        'handscanned_document': handscanned_document,
                        # 'reason': reason,
                        # 'rule': rule
                    })

            except Exception as e:
                logger.warning("Failed to parse page block at index %s: %s", i, e)

        for page_no in range(1, max(hand_page_count, digital_page_count) + 1):
            page = f'page {page_no}'
            if page not in [p['page'] for p in output_dict['pages']]:
                output_dict['pages'].append({
                    'page': page,
                    'grid_difference': [],
                    'common_difference': [],
                    'Signature': "No",
                    'digital_scanned_document_word_count': 0,
                    'hand_scanned_document_word_count': 0
                })
            page_dict = next(p for p in output_dict['pages'] if p['page'] == page)
            try:
                page_dict['digital_scanned_document_word_count'] = digital_word_count[str(page_no)]
            except (KeyError, TypeError):
                page_dict['digital_scanned_document_word_count'] = 0
            try:
                page_dict['hand_scanned_document_word_count'] = hand_word_count[str(page_no)]
            except (KeyError, TypeError):
                page_dict['hand_scanned_document_word_count'] = 0

        logger.info("output_dict assembled: %s pages", len(output_dict.get('pages', [])))
        return output_dict, results



