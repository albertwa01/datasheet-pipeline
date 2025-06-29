import os
import logging
from datetime import datetime

class LoggerManager:
    def __init__(self, log_dir=None):
        # Set the log directory to 'logs' in the working directory if none is provided
        self.log_dir = log_dir or os.path.join(os.getcwd(), "logs")
        
        # Create the log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Set up the daily log file with date-based filename
        log_file = os.path.join(self.log_dir, f"pdf_processing_{datetime.now().strftime('%Y-%m-%d')}.log")
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        # Configure the base logging setup
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def get_logger(self, name):
        # Return a logger with the specified name
        return logging.getLogger(name)
