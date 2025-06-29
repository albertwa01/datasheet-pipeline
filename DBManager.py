from sqlalchemy import create_engine, Column, String, ForeignKey, UUID, JSON, func, DateTime, TEXT, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import uuid
from slugify import slugify
from sqlalchemy import desc
from Logger import LoggerManager
import time
from image_text_extractor import count_pdf_pages

Base = declarative_base()


class PDFFile(Base):
    __tablename__ = 'datasheet_files'
    __table_args__ = {'schema': 'chatmro_db'}

    pdf_file_id = Column(UUID(as_uuid=True),
                         primary_key=True, default=uuid.uuid4)
    pdf_file_name = Column(String, unique=True, nullable=False)
    pdf_file_name_slug_value = Column(String(50), unique=True, nullable=False)
    pdf_file_path = Column(String, nullable=False)
    pdf_public_url = Column(String)
    is_datasheet = Column(Boolean, nullable=True)
    is_mpn_specific = Column(Boolean, nullable=True)
    is_series_specific = Column(Boolean, nullable=True)
    has_mpn_builder = Column(Boolean, nullable=True)
    extra_tags = Column(TEXT)
    tagger_raw_response = Column(TEXT)
    tagger_error = Column(Integer, default=0)
    jsonify_raw_response = Column(TEXT)
    jsonify_json = Column(TEXT)
    jsonify_error = Column(Integer, default=0)
    pd_ext_raw_response = Column(TEXT)
    pd_ext_list = Column(TEXT)
    pd_ext_error = Column(Integer, default=0)
    total_pages = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    status = Column(String, default='Pending')
    last_changed_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now())


class ImageFile(Base):
    __tablename__ = 'datasheet_image_files'
    __table_args__ = {'schema': 'chatmro_db'}
    image_file_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pdf_file_id = Column(UUID(as_uuid=True), ForeignKey(
        'chatmro_db.datasheet_files.pdf_file_id'), nullable=False)
    image_file_order = Column(Integer, nullable=False)
    image_file_name = Column(String, nullable=False)
    image_public_uri = Column(String)
    extracted_text = Column(String)
    text_status = Column(String, default="Pending")
    created_at = Column(DateTime, server_default=func.now())


class DBManager:

    def __init__(self, db_url, max_allowed_page):
        self.db_url = db_url
        self.max_allowed_page = max_allowed_page
        self.logger = LoggerManager().get_logger(self.__class__.__name__)
        self.engine = None
        self.Session = None
        self.initialize_db()

    def initialize_db(self, max_retries=4, wait_time=2):
        _retry = 0
        while _retry < max_retries:
            try:
                self.engine = create_engine(
                    self.db_url,
                    pool_size=10,
                    max_overflow=20,
                    pool_timeout=30,
                    pool_recycle=1800,
                    pool_pre_ping=True  # Enable pre-ping to check and maintain connections
                )
                Base.metadata.create_all(self.engine)
                self.Session = sessionmaker(bind=self.engine)
                self.logger.info(
                    "Database connection established successfully.")
                break  # Exit the loop if successful
            except Exception as e:
                _retry += 1
                self.logger.error(
                    f"Database connection attempt {_retry} failed: {e}")
                if _retry < max_retries:
                    time.sleep(300)
                else:
                    self.logger.critical(
                        "Max retry attempts reached. Unable to establish database connection.")
                    raise Exception("Database connection failed")

    def get_new_session(self):
        """Always returns a new session, reinitializing the DB connection if needed."""
        self.logger.info("Creating a new session.")
        try:
            return self.Session()
        except Exception as e:
            self.logger.error(f"Failed to create a new session: {e}")
            self.logger.info(
                "Reinitializing database connection and retrying session creation.")
            self.initialize_db()
            return self.Session()

    def insert_pdf_files(self, filename, pdf_file_path, pdf_public_url,pdf_gdrive_url=None):
        session = self.get_new_session()
        existing_file = session.query(PDFFile).filter_by(
            pdf_file_path=pdf_file_path).first()
        pdf_file_name = filename  # os.path.basename(pdf_file_path)
        slug_value = slugify(pdf_file_name)[:50]
        if existing_file:
            # print(f"File '{filename}' already exists, skipping insert.")
            self.logger.info(
                f"File '{pdf_file_path}' already exists, skipping insert.")
            return
        total_pages = count_pdf_pages(pdf_file_path)
        status = 'failed' if total_pages is not None and total_pages > self.max_allowed_page else 'Pending'
        pdf_file = PDFFile(
            pdf_file_name=pdf_file_name,
            pdf_file_name_slug_value=slug_value,
            pdf_file_path=pdf_gdrive_url if pdf_gdrive_url else pdf_file_path,
            pdf_public_url=pdf_public_url,
            total_pages=total_pages,
            status=status
        )
        session.add(pdf_file)
        # print(f"Inserting new file: {filename}")
        self.logger.info(f"Inserting new file: {pdf_file_name}")

        session.commit()
        session.close()
        # print("PDF files inserted successfully.")
        self.logger.info("PDF files inserted successfully.")

    def insert_image_record(self, pdf_uuid, image_file_name, image_file_order, public_uri):
        session = self.get_new_session()
        image_record = ImageFile(
            image_file_id=str(uuid.uuid4()),  # Generate a new UUID
            pdf_file_id=pdf_uuid,
            image_file_name=image_file_name,
            image_file_order=image_file_order,
            image_public_uri=public_uri
        )
        session.add(image_record)
        session.commit()
        session.close()
        # print(f"Inserted image record for: {image_file_name}")
        self.logger.info(f"Inserted image record for: {image_file_name}")

    def update_extracted_text(self, image_uuid, text):
        session = self.get_new_session()
        image_record = session.query(ImageFile).filter_by(
            image_file_id=image_uuid).first()
        if image_record:
            image_record.extracted_text = text
            image_record.text_status = 'done'
            session.commit()
            # print(f"Updated extracted text for image ID: {image_uuid}")
            self.logger.info(
                f"Updated extracted text for image ID: {image_uuid}")
        else:
            # print(f"No image record found for ID: {image_uuid}")
            self.logger.info(f"No image record found for ID: {image_uuid}")
        session.commit()
        session.close()

    def update_pdf_status(self, pdf_uuid):
        session = self.get_new_session()
        query = session.query(PDFFile)
        if pdf_uuid:
            pdf_record = query.filter_by(pdf_file_id=pdf_uuid).first()
        if pdf_record:
            pdf_record.status = 'done'
            self.logger.info(
                f"updating pdf status to done- pdf uuid- {pdf_uuid}")
        session.commit()
        session.close()

    def get_pending_pdfs(self):
        session = self.get_new_session()

        # query = session.query(PDFFile).filter(PDFFile.status == 'Pending')
        query = (
            session.query(PDFFile)
            .filter(PDFFile.status == 'Pending')
        )
        pending_pdfs = query.all()

        session.close()
        return pending_pdfs

    def get_all_pdf_filenames(self):
        session = self.get_new_session()

        query = session.query(PDFFile)
        all_pdfs = query.all()

        session.close()
        all_pdf_file_name_list = [pdf.pdf_file_name for pdf in all_pdfs]
        return all_pdf_file_name_list

    def check_process_status(self, pdf_file_name=None, pdf_file_path=None):
        session = self.get_new_session()

        # Build query based on the provided parameter
        query = session.query(PDFFile).filter(PDFFile.status == 'done')

        if pdf_file_name:
            pdf_record = query.filter_by(pdf_file_name=pdf_file_name).first()
        elif pdf_file_path:
            pdf_record = query.filter_by(pdf_file_path=pdf_file_path).first()
        else:
            self.logger.error(
                "Please provide either pdf_file_name or pdf_file_path.")
            return None

        # If the PDF record is found and has status 'done'
        if pdf_record:
            pdf_uuid = pdf_record.pdf_file_id
            self.logger.info(
                f"PDF '{pdf_file_name or pdf_file_path}' has status 'done'.")
        else:
            self.logger.info(
                f"No PDF file found with status 'done' for '{pdf_file_name or pdf_file_path}'.")
            pdf_uuid = None

        session.close()

        if pdf_uuid:
            self.logger.info(
                f"Retrieved PDF UUID for '{pdf_file_name or pdf_file_path}': {pdf_uuid}")
        else:
            self.logger.info(
                f"No PDF UUID found for '{pdf_file_name or pdf_file_path}'.")

        return pdf_uuid

    def get_pdf_uuid(self, pdf_file_name=None, pdf_file_path=None):
        session = self.get_new_session()
        query = session.query(PDFFile)

        if pdf_file_name:
            pdf_record = query.filter_by(pdf_file_name=pdf_file_name).first()
        elif pdf_file_path:
            pdf_record = query.filter_by(pdf_file_path=pdf_file_path).first()
        else:
            # print("Please provide either pdf_file_name or pdf_file_path.")
            self.logger.error(
                "Please provide either pdf_file_name or pdf_file_path.")
            return None

        pdf_uuid = pdf_record.pdf_file_id if pdf_record else None
        session.close()

        if pdf_uuid:
            # print(f"Retrieved PDF UUID for '{pdf_file_name or pdf_file_path}': {pdf_uuid}")
            self.logger.info(
                f"Retrieved PDF UUID for '{pdf_file_name or pdf_file_path}': {pdf_uuid}")
        else:
            # print(f"No PDF file found for '{pdf_file_name or pdf_file_path}'.")
            self.logger.info(
                f"No PDF file found for '{pdf_file_name or pdf_file_path}'.")

        return pdf_uuid

    def get_pdf_status(self, pdf_file_name=None):
        session = self.get_new_session()
        try:
            query = session.query(PDFFile)

            if pdf_file_name:
                pdf_record = query.filter_by(pdf_file_name=pdf_file_name).first()
                if pdf_record:
                    return pdf_record.status
                else:
                    self.logger.warning(f"No record found for '{pdf_file_name}'")
                    return None
            else:
                self.logger.warning("No pdf_file_name provided.")
                return None
        finally:
            session.close()


    def get_image_uuid(self, image_file_name=None, image_file_path=None, pdf_uuid=None):
        session = self.get_new_session()
        query = session.query(ImageFile)

        if image_file_name:
            image_record = query.filter_by(
                image_file_name=image_file_name).first()
        elif image_file_path:
            image_record = query.filter_by(
                image_file_path=image_file_path).first()
        else:
            # print("Please provide either image_file_name or image_file_path.")
            self.logger.info(
                "Please provide either image_file_name or image_file_path.")
            return None

        image_uuid = image_record.image_file_id if image_record else None
        session.close()

        if image_uuid:
            # print(f"Retrieved Image UUID for '{image_file_name or image_file_path}': {image_uuid}")
            self.logger.info(
                f"Retrieved Image UUID for '{image_file_name or image_file_path}': {image_uuid}")
        else:
            # print(f"No image file found for '{image_file_name or image_file_path}'.")
            self.logger.info(
                f"No image file found for '{image_file_name or image_file_path}'.")

        return image_uuid

    def get_image_uuid(self, image_file_name=None, pdf_uuid=None):
        session = self.get_new_session()
        query = session.query(ImageFile)

        if image_file_name and pdf_uuid:
            image_record = query.filter_by(
                image_file_name=image_file_name, pdf_file_id=pdf_uuid).first()
        elif image_file_name:
            image_record = query.filter_by(
                image_file_name=image_file_name).first()
        else:
            self.logger.info("Please provide image_file_name.")
            session.close()
            return None

        image_uuid = image_record.image_file_id if image_record else None
        session.close()

        if image_uuid:
            self.logger.info(
                f"Retrieved Image UUID for '{image_file_name}' with PDF UUID '{pdf_uuid}': {image_uuid}")
        else:
            self.logger.info(
                f"No image file found for '{image_file_name}' with PDF UUID '{pdf_uuid}'.")

        return image_uuid
