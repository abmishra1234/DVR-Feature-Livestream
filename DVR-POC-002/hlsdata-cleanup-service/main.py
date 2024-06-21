import os
import time
import json
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import threading

def load_config():
    """Loads configuration from a JSON file."""
    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        logging.error("Configuration file not found.")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing the configuration file: {e}")
        raise

def setup_logging():
    """Sets up logging configurations to store logs in a specified directory with date in the filename."""
    logs_directory = 'logs'
    os.makedirs(logs_directory, exist_ok=True)  # Ensure the logs directory exists
    current_date = datetime.now().strftime("%Y-%m-%d")
    logfile_name = "cleanupservice"
    logfile_path = os.path.join(logs_directory, f"{logfile_name}.log")
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'

    handler = TimedRotatingFileHandler(logfile_path, when="midnight", interval=1)
    handler.suffix = "%Y-%m-%d"
    handler.extMatch = r"^\d{4}-\d{2}-\d{2}$"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    handler.setFormatter(logging.Formatter(log_format))

    logging.info("Logging is set up.")

def clean_old_segments(directory, retention_period, exception_set):
    """Deletes files older than the retention period from the specified directory, 
    except for those in the exception set."""
    full_directory_path = os.path.abspath(directory)
    if not os.path.exists(full_directory_path):
        logging.error(f"Directory does not exist: {full_directory_path}")
        return  # Stop if the directory doesn't exist
    
    logging.info(f"Starting cleanup in directory: {full_directory_path}")
    now = datetime.now()
    cutoff_time = now - timedelta(minutes=retention_period)
    
    file_count = 0  # Track the number of files processed

    # Walk through each directory and subdirectory
    for root, dirs, files in os.walk(full_directory_path):
        if not files:
            logging.info(f"No files to process in {root}")
            continue

        logging.info(f"Scanning directory: {root} with {len(files)} files.")
        for file in files:
            if file in exception_set:
                logging.info(f"File in exception list, skipping: {file}")
                continue

            file_path = os.path.join(root, file)
            try:
                file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_mod_time < cutoff_time:
                    os.remove(file_path)
                    logging.info(f"Deleted old file: {file_path}")
                    file_count += 1
                else:
                    logging.debug(f"File retained (not old): {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")

    if file_count == 0:
        logging.info("No old files were deleted during this cycle.")
    else:
        logging.info(f"Total files deleted: {file_count}")

def worker(config):
    """Thread worker function to clean old segments periodically."""
    directory = config['directory']
    retention_period = config['retention_period']
    polling_interval = config['polling_interval']
    exception_list = config.get('exception_list', [])
    exception_set = set(exception_list)  # Use a set for faster membership checks

    while True:
        clean_old_segments(directory, retention_period, exception_set)
        logging.info("Sleeping until next cycle.")
        time.sleep(polling_interval)

def main():
    setup_logging()
    try:
        config = load_config()
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    # Use threading to allow multi-threaded execution
    thread = threading.Thread(target=worker, args=(config,))
    thread.start()
    thread.join()

if __name__ == "__main__":
    main()
