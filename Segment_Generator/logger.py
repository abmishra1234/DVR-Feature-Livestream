import logging
import os
from datetime import datetime

def setup_logger():
    # Create logs directory if it does not exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Setting up the filename with date
    date_str = datetime.now().strftime("%Y%m%d")
    log_filename = f"logs/log_sg_{date_str}.log"
    
    # Precisely defined format to match your example
    log_format = ('%(asctime)s - %(filename)s:%(funcName)s:%(lineno)d - '
                  '%(name)s - %(levelname)s - %(message)s')

    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

setup_logger()
