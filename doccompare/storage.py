import uuid
import os
import logging

import fitz

from .config import s3_client

logger = logging.getLogger(__name__)


class StorageManager:

    def download_file(self, bucket_name, object_key, download_path):
        s3_client.download_file(bucket_name, object_key, download_path)
        logger.info("File downloaded: s3://%s/%s → %s", bucket_name, object_key, download_path)

    def delete_file(self, file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug("File deleted: %s", file_path)
        else:
            logger.debug("File not found, skipping delete: %s", file_path)

    def is_base64_image(self, image_data):
        """Detect common image formats by magic bytes."""
        signatures = [
            (b'\xff\xd8\xff', 'jpeg'),
            (b'\x89PNG\r\n\x1a\n', 'png'),
            (b'GIF87a', 'gif'),
            (b'GIF89a', 'gif'),
            (b'BM', 'bmp'),
            (b'II\x2a\x00', 'tiff'),
            (b'MM\x00\x2a', 'tiff'),
        ]
        for sig, _ in signatures:
            if image_data[:len(sig)] == sig:
                return True
        if image_data[:4] == b'RIFF' and image_data[8:12] == b'WEBP':
            return True
        return False

    def validate_document(self, bucket_name, object_key):
        path = f"/tmp/{uuid.uuid4()}.pdf"
        self.download_file(bucket_name, object_key, path)
        try:
            doc = fitz.open(path)
            doc.close()
            return "Valid document"
        except Exception as e:
            logger.error("Document validation failed for %s: %s", object_key, e)
            return "Invalid document"
        finally:
            self.delete_file(path)

    def upload_file(self, bucket_name, key, body):
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
