import concurrent.futures
import time
from pathlib import Path
import logging
import requests
from urllib.parse import urljoin
from utility import load_config, setup_logging, download_file, \
    parse_master_manifest, update_target_duration

def download_and_update_manifest(playlists, subtitles, end_time, 
    download_dir, thread_count, segment_timeout, sleep_interval,
    subtitle_manifest_name):
    while time.time() < end_time or end_time == -1:
        for resolution, playlist_url in playlists:
            try:
                # Download the current playlist
                response = requests.get(playlist_url, timeout=segment_timeout)
                response.raise_for_status()
                playlist_content = response.text

                playlist_path = download_dir / resolution / f'playlist_{resolution}.m3u8'
                playlist_path.parent.mkdir(parents=True, exist_ok=True)

                # Split the playlist content to process
                lines = playlist_content.splitlines()
                max_duration = 0
                new_manifest_content = ['#EXTM3U', '#EXT-X-VERSION:3']
                download_tasks = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
                    # Process lines to compute max_duration and gather new segments
                    for i in range(len(lines)):
                        line = lines[i]
                        if line.startswith('#EXT-X-TARGETDURATION'):
                            new_manifest_content.append(line)
                        elif line.startswith('#EXT-X-MEDIA-SEQUENCE'):
                            new_manifest_content.append(line)
                        elif line.startswith('#EXT-X-PROGRAM-DATE-TIME'):
                            new_manifest_content.append(line)
                        elif line.startswith('#EXTINF'):
                            duration = float(line.split(':')[1].strip(','))
                            if duration > max_duration:
                                max_duration = duration
                            # Collect segment details
                            segment_line = lines[i + 1] if (i + 1) < len(lines) else ""
                            new_manifest_content.append(line)
                            new_manifest_content.append(segment_line)

                            # Schedule download of the segment
                            segment_url = urljoin(playlist_url, segment_line)
                            save_path = download_dir / resolution / segment_line
                            download_tasks.append(executor.submit(download_file, segment_url, save_path, segment_timeout))

                    # Wait for all segment download tasks to complete
                    for future in concurrent.futures.as_completed(download_tasks):
                        try:
                            future.result()
                        except Exception as e:
                            logging.error(f"Error in downloading segment: {e}")

                # Write new manifest
                with open(playlist_path, 'w') as file:
                    file.write('\n'.join(new_manifest_content))

                # Update the target duration in manifest file
                update_target_duration(playlist_path, max_duration)

                logging.info(f"Updated playlist with latest segments: {playlist_path}")

            except requests.RequestException as e:
                logging.error(f"Failed to download playlist: {playlist_url}, error: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing playlist {playlist_url}: {e}")

        # Handle subtitle downloads
        for language, subtitle_url in subtitles:
            try:
                subtitle_playlist_path = download_dir / language / subtitle_manifest_name #####'playlist_webvtt.m3u8'
                subtitle_playlist_path.parent.mkdir(parents=True, exist_ok=True)
                download_subtitle_playlist(subtitle_url, subtitle_playlist_path, segment_timeout, download_dir, language, thread_count)
            except Exception as e:
                logging.error(f"Error in subtitle processing for {subtitle_url}: {e}")

        time.sleep(sleep_interval)

def download_subtitle_playlist(subtitle_url, subtitle_playlist_path, 
    timeout, download_dir, language, thread_count):
    try:
        response = requests.get(subtitle_url, timeout=timeout)
        response.raise_for_status()
        subtitle_content = response.text

        # Write the subtitle playlist content
        with open(subtitle_playlist_path, 'w') as file:
            file.write(subtitle_content)

        logging.info(f"Downloaded and updated subtitle playlist: {subtitle_playlist_path}")

        # Download subtitle segments
        lines = subtitle_content.splitlines()
        download_tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            for line in lines:
                if line and not line.startswith('#'):
                    subtitle_segment_url = urljoin(subtitle_url, line)
                    save_path = download_dir / language / line
                    download_tasks.append(executor.submit(download_file, subtitle_segment_url, save_path, timeout))

            for future in concurrent.futures.as_completed(download_tasks):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in downloading subtitle segment: {e}")

    except requests.RequestException as e:
        logging.error(f"Failed to download subtitle playlist: {e}")

def main():
    config = load_config()
    HLS_URL = config['hls_url']
    DOWNLOAD_DIR = Path(config['download_directory'])
    THREAD_COUNT = config['thread_count']
    SEGMENT_TIMEOUT = config['segment_timeout']
    LOG_FILE = config['log_file']
    DOWNLOAD_DURATION = config['download_duration_minutes'] * 60  # Convert to seconds for duration
    SLEEP_INTERVAL = config['sleep_interval_seconds'] # resd the sleep interval
    MASTER_MANIFEST_NAME = config['master_manifest_name'] # read the master manifets name
    SUBTITLE_MANIFEST_NAME = config['subtitle_manifest_name'] # read the subtitle manifest name

    setup_logging(LOG_FILE)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("hls downloader process started!!!\n\n\n")
    start_time = time.time()
    end_time = start_time + DOWNLOAD_DURATION if config['download_duration_minutes'] > 0 else -1

    playlists, subtitles, closed_captions = parse_master_manifest(HLS_URL, DOWNLOAD_DIR, MASTER_MANIFEST_NAME)

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [
            executor.submit(download_and_update_manifest, playlists, subtitles, end_time, DOWNLOAD_DIR, THREAD_COUNT, 
                SEGMENT_TIMEOUT, SLEEP_INTERVAL, SUBTITLE_MANIFEST_NAME)
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except concurrent.futures.TimeoutError:
                logging.error("Download task timed out.")
            except Exception as e:
                logging.error(f"Error in download task: {e}")

    # Allow any ongoing downloads to complete or pause
    if end_time == -1:
        logging.info("The download process is set to run indefinitely.")
    else:
        while time.time() < end_time + SLEEP_INTERVAL:
            time.sleep(SLEEP_INTERVAL)

        total_time = time.time() - start_time
        logging.info(f"All downloads completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
