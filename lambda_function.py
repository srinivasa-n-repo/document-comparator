import json
import logging

from doccompare.db import DatabaseManager
from doccompare.storage import StorageManager
from doccompare.pdf_extractor import PDFExtractor
from doccompare.comparator import DocumentComparator
from doccompare.handlers import EventHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Module-level singletons — reused across warm Lambda invocations
_db = DatabaseManager()
_storage = StorageManager()
_extractor = PDFExtractor(_db)
_comparator = DocumentComparator(_db)
_handler = EventHandler(_db, _storage, _extractor, _comparator)


def lambda_handler(event, context):
    event_type = event.get('event_type', '')
    unique_id = event.get('unique_id', '')
    logger.info("Event received: event_type=%s unique_id=%s", event_type, unique_id)

    if event_type == 'file-upload':
        return _handler.handle_file_upload(event)

    if event_type == 'extract-pdf':
        return _handler.handle_extract_pdf(event)

    if event_type == 'extract-both':
        return _handler.handle_extract_both(event)

    if event_type == 'document_compare_insights':
        return _handler.handle_document_compare_insights(unique_id)

    if event_type == 'document_compare':
        return _handler.handle_document_compare_status(unique_id)

    logger.warning("Unknown event_type=%s for unique_id=%s", event_type, unique_id)
    return {'statusCode': 400, 'body': json.dumps(f"Unknown event_type: {event_type}")}
