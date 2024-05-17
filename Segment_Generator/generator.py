import logging
import os
from datetime import datetime
import requests
from urllib.parse import urljoin
import time
import concurrent.futures

class SegmentGenerator:
    def __init__(self, input_url, output_path, poll_interval=10):
        logging.debug(f"Initializing SegmentGenerator with input_url={input_url}, output_path={output_path}, poll_interval={poll_interval}")
        self.input_url = input_url
        self.output_path = output_path
        self.poll_interval = poll_interval
        self.segments_downloaded = set()

    def generate_segments(self):
        logging.info("Starting the generation of segments.")
        timestamp = datetime.now().strftime('%H%M%S')
        base_filename = f"playlist_{timestamp}"
        output_dir = os.path.join(self.output_path, base_filename)
        logging.debug(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            while True:
                # Download and process master playlist
                master_playlist_content = self._download_file(self.input_url)
                futures = self._process_master_playlist(master_playlist_content, output_dir, executor)

                # Wait for all futures to complete
                concurrent.futures.wait(futures)

                logging.info("Sleeping before the next poll.")
                time.sleep(self.poll_interval)

    def _download_file(self, url):
        logging.debug(f"Downloading file from URL: {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error downloading file from URL: {url} - {e}", exc_info=True)
            raise

    def _process_master_playlist(self, content, output_dir, executor):
        logging.debug("Processing master playlist.")
        lines = content.splitlines()
        futures = []
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                resolution = self._extract_attribute(line, "RESOLUTION")
                bandwidth = self._extract_attribute(line, "BANDWIDTH")
                if resolution and bandwidth:
                    next_line_index = i + 1
                    if next_line_index < len(lines):
                        playlist_url = lines[next_line_index]
                        full_playlist_url = urljoin(self.input_url, playlist_url)
                        logging.debug(f"Found playlist URL: {full_playlist_url}")
                        future = executor.submit(self._download_playlist_and_segments, full_playlist_url, output_dir, resolution, bandwidth)
                        futures.append(future)
        return futures

    def _extract_attribute(self, line, attribute):
        parts = line.split(",")
        for part in parts:
            if attribute in part:
                return part.split("=")[1].strip('"')
        return None

    def _download_playlist_and_segments(self, playlist_url, output_dir, resolution, bandwidth):
        logging.debug(f"Downloading playlist and segments for resolution={resolution}, bandwidth={bandwidth}")
        playlist_content = self._download_file(playlist_url)
        segment_lines = playlist_content.splitlines()
        
        segment_dir = os.path.join(output_dir, f"{resolution}_{bandwidth}")
        os.makedirs(segment_dir, exist_ok=True)
        
        playlist_filename = os.path.join(segment_dir, "playlist.m3u8")
        with open(playlist_filename, 'w') as f:
            f.write(playlist_content)
        
        with concurrent.futures.ThreadPoolExecutor() as segment_executor:
            futures = []
            for line in segment_lines:
                if not line.startswith("#"):
                    segment_url = urljoin(playlist_url, line)
                    if segment_url not in self.segments_downloaded:
                        segment_filename = os.path.join(segment_dir, f"{os.path.basename(line)}")
                        future = segment_executor.submit(self._download_file_to_disk, segment_url, segment_filename)
                        futures.append(future)
                        self.segments_downloaded.add(segment_url)
            concurrent.futures.wait(futures)

    def _download_file_to_disk(self, url, file_path):
        logging.debug(f"Downloading segment from URL: {url}")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logging.info(f"Successfully downloaded segment to {file_path}")
        except requests.RequestException as e:
            logging.error(f"Error downloading segment from URL: {url} - {e}", exc_info=True)
            raise
