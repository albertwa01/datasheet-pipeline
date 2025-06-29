from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from Logger import LoggerManager
from googleapiclient.http import MediaIoBaseDownload
import time


class DriveManager:
    def __init__(self, credentials_json_path, drive_folder_id, tmp_folder_path):
        self.tmp_folder_path = tmp_folder_path
        self.drive_folder_id = drive_folder_id
        self.service_account_file = credentials_json_path
        self.drive_service = self._init_drive_service()
        self.logger = LoggerManager().get_logger(self.__class__.__name__)

    def _init_drive_service(self):
        # Initialize Google Drive service
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive", "v3", credentials=credentials)


    def list_files(self):
        all_files = []
        page_token = None

        while True:
            # Fetch a page of files in the specified Google Drive folder
            results = self.drive_service.files().list(
                q=f"'{self.drive_folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token
            ).execute()

            # Extract the files from the current page and add to all_files
            files = results.get('files', [])
            all_files.extend([(file['name'], file['id']) for file in files])

            # Get the next page token, if there is one
            page_token = results.get('nextPageToken', None)

            # If there is no next page, break the loop
            if not page_token:
                break

        return all_files

    def get_file_url(self, file_id):
        # Construct URL for accessing Google Drive file
        return f"https://drive.google.com/uc?id={file_id}"

    def check_and_download_new_files(self, db_manager):
        # Get all files in the Google Drive folder
        drive_files = self.list_files()
        drive_filenames = {filename for filename, _ in drive_files}
        original_to_modified = {}
        for filename, file_id in drive_files:
            modified_filename = filename.split('?')[0]  # Remove query parameters if any
            modified_filename = modified_filename.split(".pdf")[0] + ".pdf"  # Ensure .pdf extension
            if '.pdf' not in modified_filename:
                modified_filename += ".pdf"
            
            # Map original filename to modified filename
            original_to_modified[filename] = modified_filename

        modified_drive_filenames = set(original_to_modified.values())
        # Get all filenames in the database
        # Assuming db_manager has a method to retrieve all filenames
        db_filenames = set(db_manager.get_all_pdf_filenames())

        # Find new files by difference
        # new_files = drive_filenames - db_filenames
        # new_files = [(filename, file_id) for filename,
                    #  file_id in drive_files if filename in new_filenames]
        new_modified_filenames = modified_drive_filenames - db_filenames
        new_files = [
                (original_filename, file_id)
                for original_filename, (modified_filename, file_id) in zip(
                    original_to_modified.keys(), drive_files
                )
                if modified_filename in new_modified_filenames
            ]
        self.logger.info(f"number of new files found :{len(new_files)}")

        downloaded_files = []
        for filename, file_id in new_files:
            file_url = self.get_file_url(file_id)
            filename,file_path = self.download_file(file_id, filename)
            downloaded_files.append((filename, file_path, file_url))
            self.logger.info(
                f"Downloaded new file {filename} from Google Drive.")

        return downloaded_files

    def download_file(self, file_id, filename):
        # Clean up filename and create temporary path
        if '?' in filename:
            filename = filename.split('?')[0]
        filename=filename.split(".pdf")[0]+".pdf"
        
        if '.pdf' not in filename:
            filename=filename+".pdf"
        tmp_path = os.path.join(self.tmp_folder_path, filename)

        # Handle possible '?' in tmp_path
        if '?' in tmp_path:
            tmp_path = tmp_path.split('?')[0]

        # Request file content from Google Drive
        request = self.drive_service.files().get_media(fileId=file_id)
        retries = 5  # Maximum retry attempts

        with open(tmp_path, 'wb') as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False

            while not done:
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        self.logger.info(
                            f"Downloading {filename}: {int(status.progress() * 100)}%"
                        )
                except Exception as e:
                    retries -= 1
                    self.logger.error(
                        f"Error downloading {filename}: {e}. Retrying {retries} more times."
                    )
                    if retries == 0:
                        # Clean up partial file if retries are exhausted
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                        raise
                    time.sleep(2)  # Wait before retrying

        self.logger.info(f"Download completed: {tmp_path}")
        return filename,tmp_path
