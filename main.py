import os
from datetime import datetime
from DBManager import DBManager
from GCSManager import GCSManager
from DriveManager import DriveManager
from PDFProcessor import PDFProcessor
from slugify import slugify
from utils import create_connection_string_from_json
from Logger import LoggerManager

logger = LoggerManager().get_logger("main")

def main(pdf_file_path, db_manager, gcs_manager):
    try:
        # Initialize PDF Processor
        pdf_processor = PDFProcessor(db_manager, gcs_manager)

        # Process and upload the PDF
        pdf_processor.process_and_upload_pdf(pdf_path=pdf_file_path, dpi=100, batch_size=100)

        logger.info(f"PDF processing completed successfully for {pdf_file_path}")

    except Exception as e:
        logger.error(f"An error occurred while processing {pdf_file_path}: {e}")

def process_pending_pdfs(db_manager, gcs_manager, base_path,temp_path, batch_size=100):
    # Fetch PDFs with 'pending' status
    pending_pdfs = db_manager.get_pending_pdfs()

    if not pending_pdfs:
        logger.info("No pending PDFs to process.")
        return

    # Process PDFs in batches
    for pdf_batch in batch_iterator(pending_pdfs, batch_size):
        logger.info(f"Processing a batch of {len(pdf_batch)} PDFs.")

        for pdf in pdf_batch:
            pdf_file_path = os.path.join(temp_path, pdf.pdf_file_name) if 'drive.google.com' in pdf.pdf_file_path else pdf.pdf_file_path
            logger.info(f"Started processing for PDF: {pdf.pdf_file_name}")
            main(pdf_file_path, db_manager, gcs_manager)
            logger.info(f"Processing completed for PDF: {pdf.pdf_file_name}")

def batch_iterator(iterable, batch_size):
    """Yield successive batches from iterable."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def insert_and_process_in_batches(files, db_manager, gcs_manager, from_drive=False, base_path="",temp_path='', batch_size=15):
    for file_batch in batch_iterator(files, batch_size):
        inserted_pdfs = []

        for file_info in file_batch:
            filename, file_path, file_url = file_info
            print(filename,file_path,file_url)
            pdf_uuid = db_manager.get_pdf_uuid(filename)

            if not pdf_uuid:
                logger.info(f"Inserting PDF {filename} into DB and uploading to GCS.")
                try:
                    upload_pdf_file_name = slugify(filename)
                    upload_pdf_file_name.replace('-pdf','.pdf')
                    with open(file_path, "rb") as pdf_file_data:
                        public_uri = gcs_manager.upload_pdf(pdf_file_data, upload_pdf_file_name)

                    db_manager.insert_pdf_files(
                        filename=filename,
                        pdf_file_path=file_path,
                        pdf_public_url=public_uri,
                        pdf_gdrive_url=file_url if from_drive else None
                    )
                    inserted_pdfs.append((filename, file_path, file_url))
                except Exception as e:
                    logger.error(f"Error uploading/inserting PDF {filename}: {e}")
            else:
                logger.info(f"PDF {filename} already exists in DB. Skipping.")

        # Process the inserted PDFs for this batch
        for filename, file_path, file_url in inserted_pdfs:
            print(filename, file_path, file_url)
            logger.info(f"Started processing PDF: {filename}")
            logger.info(f"fetching  PDF status: {filename}")
            status=db_manager.get_pdf_status(filename)
            if status=='Pending':
                main(file_path, db_manager, gcs_manager)
                logger.info(f"Finished processing PDF: {filename}")
            else:
                logger.info(f"Skipping PDF: {filename}")
        
    logger.info(f"Processing pending PDFs:")
    process_pending_pdfs(db_manager, gcs_manager, base_path,temp_path, batch_size=100)

def process_pdfs(folder_path=None, drive_manager=None, db_url=None, image_bucket_name=None, service_account_json_path=None, pdf_bucket_name=None,temp_path=None,max_allowed_page=20):
    db_manager = DBManager(db_url,max_allowed_page)
    logger.info("DB Manager initialized")
    gcs_manager = GCSManager(service_account_json_path, image_bucket_name, pdf_bucket_name)
    logger.info("GCS Manager initialized")

    if folder_path:
        logger.info("Processing files from local folder.")
        local_files = [(file, os.path.join(folder_path, file), "") for file in os.listdir(folder_path) if file.endswith(".pdf")]
        insert_and_process_in_batches(local_files, db_manager, gcs_manager, from_drive=False, base_path=folder_path,temp_path=temp_path)

    elif drive_manager:
        logger.info("Processing files from Google Drive.")
        drive_files = drive_manager.check_and_download_new_files(db_manager)
        insert_and_process_in_batches(drive_files, db_manager, gcs_manager, from_drive=True, base_path=folder_path,temp_path=temp_path)

    else:
        logger.error("No source specified for PDFs. Please provide either folder_path or drive_manager.")


if __name__ == "__main__":

    # Configuration values
    max_allowed_page=20
    postgres_db_url = create_connection_string_from_json(r"G:\Mini_projects\datasheet_pipeline\db-credt.json")
    datasheet_image_bucket_name = "datasheet-image-files"
    datasheet_pdf_bucket_name='datasheet-pdf-files'
    service_account_json_path = r"G:\Mini_projects\datasheet_pipeline\pdf-processing-439709-01d95268a63b.json"
    
    # Select source
    folder_path = r"G:\Mini_projects\datasheet_pipeline\input_folder"  # Leave as None if you want to process from Google Drive
    # folder_path=None
    # folder_path = None # Leave as None if you want to process from Google Drive
    drive_folder_id = '1fI7Zxxxxxxxx-xxxxxxxxxxxxDv'
    tmp_folder_path = r"G:\Mini_projects\datasheet_pipeline\tmp"
    
    if folder_path:
        # Process from local folder
        process_pdfs(folder_path=folder_path, db_url=postgres_db_url, pdf_bucket_name=datasheet_pdf_bucket_name,image_bucket_name=datasheet_image_bucket_name, service_account_json_path=service_account_json_path,max_allowed_page=max_allowed_page,temp_path=tmp_folder_path)
    # else:
    #     # Process from Google Drive
    #     drive_manager = DriveManager(credentials_json_path=service_account_json_path, drive_folder_id=drive_folder_id, tmp_folder_path=tmp_folder_path)
    #     process_pdfs(drive_manager=drive_manager, db_url=postgres_db_url, pdf_bucket_name=datasheet_pdf_bucket_name,image_bucket_name=datasheet_image_bucket_name,service_account_json_path=service_account_json_path,temp_path=tmp_folder_path,max_allowed_page=max_allowed_page)



