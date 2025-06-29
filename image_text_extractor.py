from pathlib import Path

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from PyPDF2 import PdfReader
from io import StringIO
import pdfplumber

from pdf2image import convert_from_path


from Logger import LoggerManager

logger = LoggerManager().get_logger("image_text_extract")


def count_pdf_pages(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception as e:
        print(f"Error reading PDF '{pdf_path}': {e}")
        return None
    


def extract_text_by_page(pdf_path: Path):
    """
    Extracts text from each page of the given PDF using pdfminer.
    If pdfminer fails (e.g., due to encryption), falls back to using pdfplumber.

    Args:
        pdf_path (Path): The path to the PDF file.

    Returns:
        list of dict: A list where each dictionary contains 'file_id' (int) and 'payload' (str) for each page.
    """
    resource_manager = PDFResourceManager()
    output_string = StringIO()
    laparams = LAParams()
    device = TextConverter(resource_manager, output_string, laparams=laparams)
    interpreter = PDFPageInterpreter(resource_manager, device)

    page_text_dict_list = []

    try:
        with open(pdf_path, 'rb') as pdf_file:
            for page_number, page in enumerate(PDFPage.get_pages(pdf_file, check_extractable=False), 1):
                try:
                    output_string.truncate(0)
                    output_string.seek(0)
                    interpreter.process_page(page)
                    page_text = output_string.getvalue()
                    # Clean the extracted text
                    page_text = page_text.replace("\x00", "").encode('utf-8', errors='replace').decode('utf-8')

                    page_text_dict_list.append({
                        'file_id':( page_number - 1,),  # Changed from tuple to integer
                        'payload': page_text
                    })
                except Exception as page_e:
                    logger.error(f"Error extracting text from page {page_number} using pdfminer: {page_e}")
                    # Attempt to extract text using pdfplumber for this specific page
                    try:
                        with pdfplumber.open(pdf_path) as pdf:
                            page_plumber = pdf.pages[page_number - 1]
                            text = page_plumber.extract_text() or ""
                            page_text_dict_list.append({
                                'file_id':( page_number - 1,),  # Integer
                                'payload': text
                            })
                            logger.info(f"Successfully extracted text from page {page_number} using pdfplumber.")
                    except Exception as plumber_e:
                        logger.error(f"Failed to extract text from page {page_number} using pdfplumber: {plumber_e}")
                        # Append empty payload or handle as needed
                        page_text_dict_list.append({
                            'file_id': (page_number - 1,),
                            'payload': ""
                        })
    except Exception as e:
        logger.exception(f"An error occurred while processing {pdf_path}: {e}")
    finally:
        # Clean up resources
        device.close()
        output_string.close()

    return page_text_dict_list


# Function to save pdf page to image
def save_pdf_page_as_image(pdf_path, dpi:int=200, batch_size:int=100):

    page_count = count_pdf_pages(pdf_path=pdf_path)
    logger.info(f"page count for pdf {pdf_path}: {page_count}")

    # Get list of pages to save as image
    page_index_to_process = list(range(page_count))

    if not batch_size:
        batch_size = page_count

    for i in range(0,len(page_index_to_process), batch_size):
        batch_index = page_index_to_process[i: i+batch_size]
        image_list = convert_from_path(pdf_path, dpi=dpi, first_page=batch_index[0]+1, last_page=batch_index[-1]+1)

        data_list = [{'file_id':(page_index,),'payload':img} for page_index,img in zip(batch_index, image_list)]

        yield data_list
