import concurrent.futures
import time
from pathlib import Path
import logging
import requests
from urllib.parse import urljoin
from collections import deque
from utility import load_config, setup_logging, download_file, \
    parse_master_manifest, store_manifestfile

from datetime import datetime, timedelta

def download_and_update_manifest(playlists, subtitles, end_time, download_dir, thread_count,
    segment_timeout, sleep_interval, subtitle_manifest_name, storage_type, s3_config,
    api_base_url):
    task_queue = deque(playlists + subtitles)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_count)

    while time.time() < end_time or end_time == -1:
        futures = []
        for _ in range(len(task_queue)):
            task = task_queue.popleft()
            if isinstance(task, tuple) and len(task) == 2:
                if task in playlists:
                    resolution, playlist_url = task
                    futures.append(executor.submit(download_playlist, resolution,
                        playlist_url, download_dir, segment_timeout, storage_type, s3_config, api_base_url))
                elif task in subtitles:
                    language, subtitle_url = task
                    futures.append(executor.submit(download_subtitle_playlist,
                        subtitle_url, download_dir, language, subtitle_manifest_name, segment_timeout, 
                        storage_type, s3_config, api_base_url))
            task_queue.append(task)

        # Wait for all tasks to complete
        concurrent.futures.wait(futures, timeout=segment_timeout)
       
        # Sleep only after completing one round of task submissions
        time.sleep(sleep_interval)

def call_add_tsmetadata(metadata, api_base_url):
    fastapi_url = f"{api_base_url}/add_tsmetadata"
    try:
        response = requests.post(fastapi_url, json=metadata)
        response.raise_for_status()
        logging.info(f"Successfully added metadata: {metadata}")
    except requests.RequestException as e:
        logging.error(f"Failed to add TS metadata: {e}")

def adjust_datetime(start_time, duration):
    adjusted_time = start_time + timedelta(seconds=duration)
    return adjusted_time

def download_playlist(resolution, playlist_url, download_dir, segment_timeout, storage_type, s3_config, api_base_url):
    try:
        response = requests.get(playlist_url, timeout=segment_timeout)
        response.raise_for_status()
        playlist_content = response.text

        playlist_path = Path(download_dir) / resolution / f'playlist_{resolution}.m3u8'
        store_manifestfile(playlist_content, playlist_path, storage_type, s3_config)

        lines = playlist_content.splitlines()
        download_tasks = []

        start_time = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for i, line in enumerate(lines):
                if line.startswith('#EXTINF'):
                    duration = float(line.split(':')[1].strip(','))
                    segment_line = lines[i + 1] if (i + 1) < len(lines) else ""

                    segment_url = urljoin(playlist_url, segment_line)
                    save_path = Path(download_dir) / resolution / segment_line
                    download_tasks.append(executor.submit(download_file, segment_url, save_path, segment_timeout, storage_type, s3_config))

                    # Extract sequence number from segment file name
                    sequence_number = int(segment_line.split('__')[-1].split('.')[0])

                    # Extract or calculate the start time
                    if start_time is None:
                        if any('#EXT-X-PROGRAM-DATE-TIME' in l for l in lines):
                            start_time_str = next(l.split(':')[1] for l in lines if '#EXT-X-PROGRAM-DATE-TIME' in l)
                            start_time = datetime.fromisoformat(start_time_str.rstrip('Z'))
                        else:
                            # Fallback to current time if no program date time is provided
                            start_time = datetime.utcnow()  

                    # Prepare metadata for each segment
                    metadata = {
                        "resolution": resolution,
                        "date": start_time.date().isoformat(),
                        "start_timestamp": start_time.strftime("%H:%M:%S.%f")[:-3],
                        "sequence_number": sequence_number,
                        "duration": duration,
                        "ts_file": segment_line
                    }
                   
                    # Call the FastAPI endpoint to add TS metadata
                    executor.submit(call_add_tsmetadata, metadata, api_base_url)
                   
                    # Increment start time by the duration of the segment
                    start_time = adjust_datetime(start_time, duration)

            for future in concurrent.futures.as_completed(download_tasks):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in downloading segment: {e}")

    except requests.RequestException as e:
        logging.error(f"Failed to download playlist: {playlist_url}, error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing playlist {playlist_url}: {e}")
        
def call_add_vttmetadata(metadata, api_base_url):
    fastapi_url = f"{api_base_url}/add_vttmetadata"
    try:
        response = requests.post(fastapi_url, json=metadata)
        response.raise_for_status()
        logging.info(f"Successfully added VTT metadata: {metadata}")
    except requests.RequestException as e:
        logging.error(f"Failed to add VTT metadata: {e}")

def download_subtitle_playlist(subtitle_url, download_dir, language, 
    subtitle_manifest_name, timeout, storage_type, s3_config, api_base_url):
    try:
        subtitle_playlist_path = Path(download_dir) / language / subtitle_manifest_name
        response = requests.get(subtitle_url, timeout=timeout)
        response.raise_for_status()
        subtitle_content = response.text

        store_manifestfile(subtitle_content, subtitle_playlist_path, 
            storage_type, s3_config)

        lines = subtitle_content.splitlines()
        download_tasks = []
        start_time = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for i, line in enumerate(lines):
                if line.startswith('#EXTINF'):
                    duration = float(line.split(':')[1].strip(','))
                   
                    segment_line = lines[i + 1] if (i + 1) < len(lines) else ""
                    subtitle_segment_url = urljoin(subtitle_url, segment_line)
                    save_path = Path(download_dir) / language / segment_line
                    download_tasks.append(executor.submit(download_file, 
                        subtitle_segment_url, save_path, timeout, storage_type, s3_config))

                    # Extract sequence number from segment file name
                    sequence_number = int(segment_line.split('__')[-1].split('.')[0])

                    # Extract or calculate the start time
                    if start_time is None:
                        if any('#EXT-X-PROGRAM-DATE-TIME' in l for l in lines):
                            start_time_str = next(l.split(':')[1] for l in lines if '#EXT-X-PROGRAM-DATE-TIME' in l)
                            start_time = datetime.fromisoformat(start_time_str.rstrip('Z'))
                        else:
                            # Fallback to current time if no program date time is provided                            
                            start_time = datetime.utcnow()

                    # Prepare metadata for each segment
                    metadata = {
                        "language": language,
                        "date": start_time.date().isoformat(),
                        "start_timestamp": start_time.strftime("%H:%M:%S.%f")[:-3],
                        "sequence_number": sequence_number,
                        "duration": duration,
                        "vtt_file": segment_line
                    }
                   
                    # Call the FastAPI endpoint to add VTT metadata
                    executor.submit(call_add_vttmetadata, metadata, api_base_url)
                   
                    # Increment start time by the duration of the segment
                    start_time = adjust_datetime(start_time, duration)

            for future in concurrent.futures.as_completed(download_tasks):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in downloading subtitle segment: {e}")

    except requests.RequestException as e:
        logging.error(f"Failed to download subtitle playlist: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing subtitle playlist {subtitle_url}: {e}")

def main():
    config = load_config()
    HLS_URL = config['hls_url']
    DOWNLOAD_DIR = Path(config['download_directory'])
    THREAD_COUNT = config['thread_count']
    SEGMENT_TIMEOUT = config['segment_timeout']
    LOG_FILE = config['log_file']
    DOWNLOAD_DURATION = config['download_duration_minutes'] * 60  # Convert to seconds for duration
    SLEEP_INTERVAL = config['sleep_interval_seconds'] # read the sleep interval
    MASTER_MANIFEST_NAME = config['master_manifest_name'] # read the master manifest name
    SUBTITLE_MANIFEST_NAME = config['subtitle_manifest_name'] # read the subtitle manifest name
    STORAGE_TYPE = config['storage_type']  # 'local' or 's3'
    S3_CONFIG = config.get('s3_config', {}) if STORAGE_TYPE == 's3' else None
    API_BASE_URL = config['api_base_url']

    setup_logging(LOG_FILE) # setup logging is done here
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("HLS downloader process started!!!\n\n\n")
    start_time = time.time()
    end_time = start_time + DOWNLOAD_DURATION if config['download_duration_minutes'] > 0 else -1

    playlists, subtitles, closed_captions = parse_master_manifest(HLS_URL,
        DOWNLOAD_DIR, MASTER_MANIFEST_NAME, STORAGE_TYPE, S3_CONFIG)
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [
            executor.submit(download_and_update_manifest, playlists, subtitles,
                end_time, DOWNLOAD_DIR, THREAD_COUNT, SEGMENT_TIMEOUT,
                SLEEP_INTERVAL, SUBTITLE_MANIFEST_NAME, STORAGE_TYPE, S3_CONFIG, API_BASE_URL)
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except concurrent.futures.TimeoutError:
                logging.error("Download task timed out.")
            except Exception as e:
                logging.error(f"Error in download task: {e}")

    if end_time == -1:
        logging.info("The download process is set to run indefinitely.")
    else:
        while time.time() < end_time + SLEEP_INTERVAL:
            time.sleep(SLEEP_INTERVAL)

        total_time = time.time() - start_time
        logging.info(f"All downloads completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()