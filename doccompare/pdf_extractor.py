import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from io import BytesIO

import pymupdf4llm
import pypdfium2 as pdfium

from .config import bedrock_client, SCANNED_MAX_WORKERS
from .db import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class PDFExtractor:
    db: DatabaseManager

    def count_words(self, input_string):
        date_pattern = r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b'
        dates = re.findall(date_pattern, input_string)
        temp_string = re.sub(date_pattern, 'DATEPLACEHOLDER', input_string)
        cleaned_string = re.sub(r'[^\w\s]|_', '', temp_string)
        for date in dates:
            cleaned_string = cleaned_string.replace('DATEPLACEHOLDER', date, 1)
        return len(cleaned_string.split())

    def clean_text(self, text):
        text = re.sub(r'\n+', '\n', text)
        text = re.sub(r'[ \t]*\n[ \t]*', '\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def extract_digital(self, pdf_path, unique_id):
        self.db.update_time(("start of digital", unique_id))
        document_word_count = {}
        page_wise_scanned_text = []
        page_no = 0
        doc = pymupdf4llm.to_markdown(pdf_path, page_chunks=True)
        for page in doc:
            page_no += 1
            text = page.get("text", "")
            if not text.strip():
                continue
            logger.info("Processing digital page %s", page_no)
            cleaned_text = self.clean_text(text)
            clean_for_count = cleaned_text.replace("\n", " ")
            document_word_count[str(page_no)] = self.count_words(clean_for_count)
            page_wise_scanned_text.append(cleaned_text)
        logger.info("Extraction complete: %s pages", len(page_wise_scanned_text))
        return document_word_count, page_wise_scanned_text

    def _extract_images_from_pdf(self, pdf_path, dpi=250):
        pdf = pdfium.PdfDocument(pdf_path)
        scale = dpi / 120
        pages = []
        for page_no, page in enumerate(pdf, start=1):
            bitmap = page.render(scale=scale)
            image = bitmap.to_pil()
            buffer = BytesIO()
            image.save(buffer, format="jpeg")
            pages.append((page_no, buffer.getvalue()))
        return pages

    def _ocr_page(self, page_no, image_bytes, ocr_system_prompt, model_id):
        response = bedrock_client.converse(
            modelId=model_id,
            system=[{"text": ocr_system_prompt}],
            messages=[{
                "role": "user",
                "content": [{"image": {"format": "jpeg", "source": {"bytes": image_bytes}}}]
            }],
            inferenceConfig={"temperature": 0.0, "maxTokens": 5000},
            additionalModelRequestFields={"include_reasoning": True, "reasoning_effort": "medium"}
        )
        output_text = ""
        for block in response["output"]["message"]["content"]:
            if "text" in block:
                # print(block['text'])
                output_text += block["text"]
        usage = response.get("usage", {})
        return page_no, output_text, usage

    def extract_scanned(self, pdf_path, unique_id):
        self.db.update_time(("start of qwen", unique_id))
        query = "select prompt, model_id from prompt_management where task = 'detect_text' and prompt_status = 'Active';"
        output = self.db.fetch(query)
        ocr_system_prompt = output[0][0]
        ocr_model_id = output[0][1]
        aggregated_input_prompt = ""
        scanned_pages = self._extract_images_from_pdf(pdf_path)
        results = []
        with ThreadPoolExecutor(max_workers=SCANNED_MAX_WORKERS) as executor:
            futures = [
                executor.submit(self._ocr_page, page_no, image_bytes, ocr_system_prompt, ocr_model_id)
                for page_no, image_bytes in scanned_pages
            ]
            for future in as_completed(futures):
                aggregated_input_prompt += ocr_system_prompt
                aggregated_input_prompt += "[IMAGE]"
                results.append(future.result())
        results.sort(key=lambda x: x[0])
        doc_word_count = {}
        doc_page_wise_text = []
        total_input_tokens = 0
        total_output_tokens = 0
        for page_no, page_text, usage in results:
            total_input_tokens += usage.get("inputTokens", 0)
            total_output_tokens += usage.get("outputTokens", 0)
            doc_word_count[str(page_no)] = self.count_words(page_text)
            doc_page_wise_text.append(page_text)
        logger.info("OCR complete: %s pages, input_tokens=%s, output_tokens=%s",
                    len(doc_page_wise_text), total_input_tokens, total_output_tokens)
        return doc_word_count, doc_page_wise_text, total_input_tokens, total_output_tokens, aggregated_input_prompt
