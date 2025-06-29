from Logger import LoggerManager
from google.cloud import storage
from google.api_core.exceptions import TooManyRequests
import time
import random

class GCSManager:
    def __init__(self, service_account_json_path, image_bucket_name, pdf_bucket_name):
        self.logger = LoggerManager().get_logger(self.__class__.__name__)
        self.client = storage.Client.from_service_account_json(service_account_json_path)
        self.image_bucket = self.client.get_bucket(image_bucket_name)
        self.pdf_bucket = self.client.get_bucket(pdf_bucket_name)

    def _retry_upload(self, upload_func, max_retries=5, initial_delay=2, max_delay=60):
        retries = 0
        delay = initial_delay

        while True:
            try:
                return upload_func()
            except TooManyRequests as e:
                if retries >= max_retries:
                    self.logger.error(f"Exceeded max retries due to rate limiting: {e}")
                    raise
                sleep_time = delay + random.uniform(0, 1)  
                self.logger.warning(f"Rate limit hit (429). Retrying in {sleep_time:.2f} seconds... (Attempt {retries + 1})")
                time.sleep(sleep_time)
                delay = min(delay * 2, max_delay)
                retries += 1
            except Exception as e:
                self.logger.error(f"Upload failed due to unexpected error: {e}")
                raise

    def upload_image(self, file_obj, destination_blob_name):
        def upload():
            file_obj.seek(0)
            blob = self.image_bucket.blob(destination_blob_name)
            blob.upload_from_file(file_obj, content_type='image/png')
            self.logger.info(f"Uploaded image {destination_blob_name} to bucket {self.image_bucket.name}")
            return f"https://storage.googleapis.com/{self.image_bucket.name}/{destination_blob_name}"

        return self._retry_upload(upload)

    def upload_pdf(self, file_obj, destination_blob_name):
        def upload():
            file_obj.seek(0)
            blob = self.pdf_bucket.blob(destination_blob_name)
            blob.upload_from_file(file_obj, content_type='application/pdf')
            self.logger.info(f"Uploaded PDF {destination_blob_name} to bucket {self.pdf_bucket.name}")
            return f"https://storage.googleapis.com/{self.pdf_bucket.name}/{destination_blob_name}"

        return self._retry_upload(upload)
