import os
import time
import json
from datetime import datetime, timedelta
import logging

def load_config():
    """Loads configuration from a JSON file."""
    with open('config.json', 'r') as file:
        config = json.load(file)
    return config

def setup_logging():
    """Sets up logging configurations to store logs in a specified directory with date in the filename."""
    logs_directory = 'logs'
    os.makedirs(logs_directory, exist_ok=True)  # Ensure the logs directory exists
    current_date = datetime.now().strftime("%Y-%m-%d")
    logfile_name = f"cleanupservice_{current_date}.log"
    logfile_path = os.path.join(logs_directory, logfile_name)
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    logging.basicConfig(filename=logfile_path, level=logging.INFO, format=log_format)
    logging.info("Logging is set up.\n\n\n")

def clean_old_segments(directory, retention_period):
    """Deletes files older than the retention period from the specified directory."""
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

def main():
    setup_logging()
    config = load_config()
    directory = config['directory']
    retention_period = config['retention_period']
    polling_interval = config['polling_interval']

    while True:
        clean_old_segments(directory, retention_period)
        logging.info("Sleeping until next cycle.")
        time.sleep(polling_interval)

if __name__ == "__main__":
    main()
