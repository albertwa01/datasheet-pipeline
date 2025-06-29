import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from slugify import slugify
from pathlib import Path
from Logger import LoggerManager
from image_text_extractor import save_pdf_page_as_image,extract_text_by_page

class PDFProcessor:
    def __init__(self, db_manager, gcs_manager):
        self.logger = LoggerManager().get_logger(self.__class__.__name__)
        self.db_manager = db_manager
        self.gcs_manager = gcs_manager

    def process_and_upload_pdf(self, pdf_path, dpi=200, batch_size=100):
        # Insert PDF record in the database
        pdf_file_name = os.path.basename(pdf_path)
        pdf_uuid = self.db_manager.get_pdf_uuid(pdf_file_name=pdf_file_name)
        upload_pdf_file_name = slugify(pdf_file_name)[:50]

        if not pdf_uuid:
            with open(pdf_path, "rb") as pdf_file_data:
                public_uri = self.gcs_manager.upload_pdf(pdf_file_data, upload_pdf_file_name)

            self.db_manager.insert_pdf_files(pdf_file_name,pdf_path,public_uri)

            pdf_uuid = self.db_manager.get_pdf_uuid(pdf_file_name=pdf_file_name)

        # Process each page: convert to image, upload, extract text, and insert to DB
        # Use ThreadPoolExecutor to upload images in batches
       
        for image_batch in save_pdf_page_as_image(pdf_path, dpi=dpi, batch_size=batch_size):
            self.logger.info(f"Length of image list received: {len(image_batch)}")
            self.process_image_batch(image_batch, upload_pdf_file_name,pdf_uuid)


        text_pages = extract_text_by_page(Path(pdf_path))
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for text_entry in text_pages:
                futures.append(executor.submit(self.update_image_text, text_entry, upload_pdf_file_name,pdf_uuid))

            # Wait for all futures to complete
            for future in futures:
                future.result()

        # Update PDF status after all processing
        self.db_manager.update_pdf_status(pdf_uuid)


    def update_image_text(self,text_entry,upload_pdf_file_name,pdf_uuid):
        image_index = text_entry['file_id'][0]
        image_uuid = self.db_manager.get_image_uuid(f"{upload_pdf_file_name}_{image_index}.png",pdf_uuid)
        self.db_manager.update_extracted_text(image_uuid, text_entry['payload'])



    def upload_image(self, image_data, upload_pdf_file_name, pdf_uuid):
        image_index = image_data['file_id'][0]
        image = image_data['payload']

        image_file_name = f"{upload_pdf_file_name}/{image_index}.png"
        image_bytes = BytesIO()
        image.save(image_bytes, format="PNG")
        image_bytes.seek(0)  # Reset buffer position to the beginning

        # Upload the image file to GCS directly from the bytes buffer
        public_uri = self.gcs_manager.upload_image(image_bytes, image_file_name)

        # Insert into DB
        self.db_manager.insert_image_record(pdf_uuid, f"{upload_pdf_file_name}_{image_index}.png", image_index, public_uri)

    def process_image_batch(self, image_batch, upload_pdf_file_name, pdf_uuid):
        with ThreadPoolExecutor(max_workers=9) as executor:
            # Submit all the image uploads to the thread pool
            futures = []
            for image_data in image_batch:
                futures.append(executor.submit(self.upload_image, image_data, upload_pdf_file_name, pdf_uuid))

            # Wait for all futures to complete
            for future in futures:
                future.result()
