# main.py
import os
import time
import json
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

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

###############Utility Methods ##############
def parse_ts_file(file_path):
    """Extract resolution and sequence number from TS file path."""
    try:
        parts = file_path.split(os.sep)
        resolution = parts[-2]
        sequence_number = int(parts[-1].split('_')[-1].split('.')[0])
        return resolution, sequence_number
    except Exception as e:
        logging.error(f"Failed to parse TS file path {file_path}: {e}")
        return None, None

def parse_vtt_file(file_path):
    """Extract language and sequence number from VTT file path."""
    try:
        parts = file_path.split(os.sep)
        language = parts[-2]
        sequence_number = int(parts[-1].split('_')[-1].split('.')[0])
        return language, sequence_number
    except Exception as e:
        logging.error(f"Failed to parse VTT file path {file_path}: {e}")
        return None, None

def remove_ts_metadata(api_base_url, resolution, sequence_number):
    """Call the FastAPI endpoint to remove TS metadata."""
    url = f"{api_base_url}/remove_tsmetadata/{resolution}/{sequence_number}"
    try:
        response = requests.delete(url)
        if response.status_code == 200:
            logging.info(f"Successfully removed TS metadata: {resolution}, {sequence_number}")
        else:
            logging.error(f"Failed to remove TS metadata: {resolution}, {sequence_number}, status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Exception during TS metadata removal: {e}")

def remove_vtt_metadata(api_base_url, language, sequence_number):
    """Call the FastAPI endpoint to remove VTT metadata."""
    url = f"{api_base_url}/remove_vttmetadata/{language}/{sequence_number}"
    try:
        response = requests.delete(url)
        if response.status_code == 200:
            logging.info(f"Successfully removed VTT metadata: {language}, {sequence_number}")
        else:
            logging.error(f"Failed to remove VTT metadata: {language}, {sequence_number}, status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Exception during VTT metadata removal: {e}")

###############Utility Methods Ends here##############

def delete_file(file_path, cutoff_time, file, api_base_url):
    """Deletes a single file if it is older than the cutoff time."""
    try:
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if file_mod_time < cutoff_time:
            os.remove(file_path)
            logging.info(f"Deleted old file: {file_path}")

            # Remove metadata
            if file.endswith('.ts'):
                resolution, sequence_number = parse_ts_file(file_path)
                if resolution and sequence_number:
                    remove_ts_metadata(api_base_url, resolution, sequence_number)
            elif file.endswith('.vtt'):
                language, sequence_number = parse_vtt_file(file_path)
                if language and sequence_number:
                    remove_vtt_metadata(api_base_url, language, sequence_number)
            
            return True
        else:
            logging.debug(f"File retained (not old): {file_path}")
            return False
    except Exception as e:
        logging.error(f"Failed to delete {file_path}: {e}")
        return False

def clean_old_segments(directory, retention_period,
     exception_set, api_base_url):
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
    tasks = []
    with ThreadPoolExecutor() as executor:
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
                tasks.append(executor.submit(delete_file, file_path, cutoff_time, file, api_base_url))

        for future in as_completed(tasks):
            if future.result():
                file_count += 1

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
    API_BASE_URL = config['api_base_url']

    while True:
        clean_old_segments(directory, retention_period, exception_set, API_BASE_URL)
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
    thread.daemon = True  # Make the thread a daemon to exit when the main program exits
    thread.start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive to let the worker thread run
    except KeyboardInterrupt:
        logging.info("Cleanup service interrupted by user.")

if __name__ == "__main__":
    main()