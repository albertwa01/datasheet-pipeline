# 📄 PDF Datasheet Processing Pipeline

This project implements a structured pipeline to automate the processing of product-related PDFs (e.g., datasheets, spec sheets). The goal is to convert unstructured PDF content into structured, queryable formats for easy consumption by the Data Science team for tasks like tagging, labeling, and feature extraction.

---

## 🔄 Pipeline Workflow

1. **Ingestion**
   - PDFs are sourced either from a local folder or Google Drive.
   - Metadata is recorded in a PostgreSQL table (`datasheet_files`) to track processing stages.

2. **Cloud Upload**
   - Each PDF is uploaded to Google Cloud Storage.
   - A public URL is generated and stored for reference and accessibility.

3. **Page-Level Processing**
   - PDFs are optionally split into individual pages (images).
   - Each page image is uploaded to GCS and linked via a foreign key in the `datasheet_image_files` table.
   - Text is extracted from each page and saved alongside its image.

4. **Structured Access**
   - A SQLAlchemy-based DB interface allows the Data Science team to:
     - Fetch pending rows based on workflow stages.
     - Update tagging, JSONification, and entity extraction results.
     - Query extracted data for model training or analysis.

---

## 🧩 Tech Stack

- **Language**: Python  
- **Database**: PostgreSQL  
- **ORM**: SQLAlchemy  
- **Cloud Storage**: Google Cloud Storage (GCS)  
- **File Access**: Google Drive API  
- **PDF Parsing**: pdfplumber, PyMuPDF  
- **Data Handling**: Pandas  
- **Authentication**: JSON-based credential loading  

---
