import logging
import os
from datetime import datetime
import requests
from urllib.parse import urljoin, urlparse
import time
import concurrent.futures
import json

# Load configuration
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

from s3_uploader import S3Uploader

class SegmentGenerator:
    def __init__(self, input_url, output_path, s3_bucket_name=None):
        logging.debug(f"Initializing SegmentGenerator with input_url={input_url}, output_path={output_path}, s3_bucket_name={s3_bucket_name}")
        self.input_url = input_url
        self.output_path = output_path
        self.poll_interval = int(config.get("poll_interval"))  # Ensure poll_interval is an integer
        self.segments_downloaded = set()
        self.subtitles_downloaded = set()

        self.s3_bucket_name = s3_bucket_name
        if self.s3_bucket_name:
            self.s3_uploader = S3Uploader(bucket_name=self.s3_bucket_name,
                                          region_name=config.get("region_name"))

        # Extract domain name from URL to use as the root directory name
        self.domain_name = urlparse(input_url).netloc
        self.root_dir = os.path.join(self.output_path, self.domain_name)
        os.makedirs(self.root_dir, exist_ok=True)

    def generate_segments(self):
        logging.info("Starting the generation of segments.")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            while True:
                try:
                    # Download and process master playlist
                    master_playlist_content = self._download_file(self.input_url)
                    master_playlist_path = os.path.join(self.root_dir, "playlist.m3u8")
                    self._save_content_to_file(master_playlist_content, master_playlist_path)
                    futures = self._process_master_playlist(master_playlist_content, self.root_dir, executor)

                    # Upload master playlist to S3 if S3 bucket is configured
                    if self.s3_bucket_name:
                        self._upload_file_to_s3(master_playlist_path, self.domain_name)

                    # Wait for all futures to complete
                    concurrent.futures.wait(futures)
                except Exception as e:
                    logging.error(f"Error in segment generation loop: {e}", exc_info=True)

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

    def _save_content_to_file(self, content, file_path):
        logging.debug(f"Saving content to file: {file_path}")
        try:
            with open(file_path, 'w') as f:
                f.write(content)
            logging.info(f"Successfully saved content to {file_path}")
        except IOError as e:
            logging.error(f"Error saving content to file: {file_path} - {e}", exc_info=True)
            raise

    def _upload_file_to_s3(self, file_path, s3_prefix):
        if self.s3_bucket_name:
            relative_path = os.path.relpath(file_path, self.root_dir)
            s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            try:
                logging.debug(f"Uploading {file_path} to s3://{self.s3_bucket_name}/{s3_path}")
                self.s3_uploader.upload_file(file_path, self.s3_bucket_name, s3_path)
            except Exception as e:
                logging.error(f"Error uploading {file_path} to S3: {e}", exc_info=True)
                raise

    def _process_master_playlist(self, content, output_dir, executor):
        logging.debug("Processing master playlist.")
        lines = content.splitlines()
        futures = []
        for i, line in enumerate(lines):
            try:
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
                elif line.startswith("#EXT-X-MEDIA") and "TYPE=SUBTITLES" in line:
                    subtitle_url = self._extract_attribute(line, "URI")
                    language = self._extract_attribute(line, "LANGUAGE")
                    if subtitle_url and language:
                        full_subtitle_url = urljoin(self.input_url, subtitle_url)
                        logging.debug(f"Found subtitle URL: {full_subtitle_url} for language: {language}")
                        future = executor.submit(self._download_subtitle_playlist, full_subtitle_url, output_dir, language)
                        futures.append(future)
            except Exception as e:
                logging.error(f"Error processing master playlist line: {line} - {e}", exc_info=True)
        return futures

    def _extract_attribute(self, line, attribute):
        parts = line.split(",")
        for part in parts:
            if attribute in part:
                return part.split("=")[1].strip('"')
        logging.warning(f"Attribute {attribute} not found in line: {line}")
        return None

    def _download_playlist_and_segments(self, playlist_url, output_dir, resolution, bandwidth):
        try:
            logging.debug(f"Downloading playlist and segments for resolution={resolution}, bandwidth={bandwidth}")
            playlist_content = self._download_file(playlist_url)
            segment_lines = playlist_content.splitlines()
            
            segment_dir = os.path.join(output_dir, f"{resolution}_{bandwidth}")
            os.makedirs(segment_dir, exist_ok=True)
            
            playlist_filename = os.path.join(segment_dir, f"playlist_{resolution}.m3u8")
            self._save_content_to_file(playlist_content, playlist_filename)
            self._upload_file_to_s3(playlist_filename, self.domain_name)
            
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
        except Exception as e:
            logging.error(f"Error downloading playlist and segments from URL: {playlist_url} - {e}", exc_info=True)

    def _download_subtitle_playlist(self, subtitle_url, output_dir, language):
        try:
            logging.debug(f"Downloading subtitle playlist from URL: {subtitle_url}")
            subtitle_content = self._download_file(subtitle_url)
            subtitle_lines = subtitle_content.splitlines()

            subtitle_dir = os.path.join(output_dir, "subtitles", language)
            os.makedirs(subtitle_dir, exist_ok=True)

            playlist_filename = os.path.join(subtitle_dir, "playlist_webvtt.m3u8")
            self._save_content_to_file(subtitle_content, playlist_filename)
            self._upload_file_to_s3(playlist_filename, self.domain_name)

            with concurrent.futures.ThreadPoolExecutor() as subtitle_executor:
                futures = []
                for line in subtitle_lines:
                    if not line.startswith("#"):
                        subtitle_segment_url = urljoin(subtitle_url, line)
                        if subtitle_segment_url not in self.subtitles_downloaded:
                            subtitle_segment_filename = os.path.join(subtitle_dir, f"{os.path.basename(line)}")
                            future = subtitle_executor.submit(self._download_file_to_disk, subtitle_segment_url, subtitle_segment_filename)
                            futures.append(future)
                            self.subtitles_downloaded.add(subtitle_segment_url)
                concurrent.futures.wait(futures)
        except Exception as e:
            logging.error(f"Error downloading subtitle playlist from URL: {subtitle_url} - {e}", exc_info=True)

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
            # Upload the file to S3 immediately after downloading
            self._upload_file_to_s3(file_path, self.domain_name)
        except requests.RequestException as e:
            logging.error(f"Error downloading segment from URL: {url} - {e}", exc_info=True)
            raise
