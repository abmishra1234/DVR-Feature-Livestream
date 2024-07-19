# utility.py
import os
import json
import logging
import requests
from pathlib import Path
from urllib.parse import urljoin
from logging.handlers import TimedRotatingFileHandler
import boto3
from botocore.exceptions import BotoCoreError, ClientError

def load_config(config_path='config.json'):
    try:
        with open(config_path) as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from config file: {config_path}")
        raise

def setup_logging(log_file):
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, log_file)
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    handler = TimedRotatingFileHandler(log_file_path, when="midnight", interval=1)
    handler.suffix = "%Y-%m-%d.log"
    handler.extMatch = r"^\d{4}-\d{2}-\d{2}.log$"
    logging.basicConfig(level=logging.DEBUG, format=log_format, handlers=[handler])
    logging.info("Configuration loaded and directories set up.")

def get_s3_client(s3_config):
    try:
        s3_client = boto3.client(
            's3',
            region_name=s3_config['region_name']
        )
        return s3_client
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to create S3 client: {e}")
        raise

def store_binaryfile(content, save_path, storage_type, s3_config=None):
    try:
        if storage_type == 'local':
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, 'wb') as file:
                file.write(content)
            logging.info(f"Downloaded file: {save_path}")

        elif storage_type == 's3' and s3_config:
            s3_client = get_s3_client(s3_config)

            # Extract the directory and filename
            directory = save_path.parent.name
            filename = save_path.name

            # Construct the S3 key
            s3_key = f"{directory}/{filename}"

            s3_client.put_object(Bucket=s3_config['bucket_name'], Key=s3_key, Body=content)
            logging.info(f"Uploaded file to S3: {s3_key}")

        else:
            logging.error("Invalid storage type or missing S3 configuration")
            raise ValueError("Invalid storage type or missing S3 configuration")
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to upload to S3: {e}")
        raise
    except OSError as e:
        logging.error(f"OS error when saving file {save_path}: {e}")
        raise

def download_file(url, save_path, timeout, storage_type, s3_config=None):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        content = response.content

        store_binaryfile(content, save_path, storage_type, s3_config)
        # Add the Fast API Call for adding metadata
        # 
       
    except requests.Timeout:
        logging.error(f"Timeout occurred while downloading {url}")
    except requests.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def store_manifestfile(content, save_path, storage_type, s3_config=None, master=False):
    try:
        if storage_type == 'local':
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, 'w') as file:
                file.write(content)
            logging.info(f"Downloaded Manifest file: {save_path}")

        elif storage_type == 's3' and s3_config:
            s3_client = get_s3_client(s3_config)

            # Extract the directory and filename
            directory = save_path.parent.name
            filename = save_path.name

            # Construct the S3 key
            if master:
                s3_key = f"{filename}"
            else:
                s3_key = f"{directory}/{filename}"

            s3_client.put_object(Bucket=s3_config['bucket_name'], Key=s3_key, Body=content)
            logging.info(f"Uploaded Manifest file to S3: {s3_key}")

        else:
            logging.error("Invalid storage type or missing S3 configuration")
            raise ValueError("Invalid storage type or missing S3 configuration")
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to upload to S3: {e}")
        raise
    except OSError as e:
        logging.error(f"OS error when saving file {save_path}: {e}")
        raise

def parse_master_manifest(url, download_dir, master_manifest_name, 
    storage_type, s3_config=None):
    try:
        response = requests.get(url)
        response.raise_for_status()
        manifest_content = response.text
    except requests.RequestException as e:
        logging.error(f"Failed to download master manifest from {url}: {e}")
        raise

    master_manifest_path = download_dir / master_manifest_name
    # called the utility method created for handling master playlist download/upload
    store_manifestfile(manifest_content, master_manifest_path, storage_type, s3_config, True)

    playlists, subtitles, closed_captions = [], [], []
    manifest_lines = manifest_content.splitlines()
    for i, line in enumerate(manifest_lines):
        if line.startswith('#EXT-X-STREAM-INF'):
            resolution, playlist_url = parse_stream_inf(line, manifest_lines, i, url)
            playlists.append((resolution, playlist_url))
        elif line.startswith('#EXT-X-MEDIA') and 'TYPE=SUBTITLES' in line:
            subtitles.append(parse_media_inf(line, url, 'subtitles'))
        elif line.startswith('#EXT-X-MEDIA') and 'TYPE=CLOSED-CAPTIONS' in line:
            closed_captions.append(parse_media_inf(line, url, 'closed-captions'))

    logging.info(f"Parsed playlists from master manifest: {playlists}")
    logging.info(f"Parsed subtitles from master manifest: {subtitles}")
    logging.info(f"Parsed closed captions from master manifest: {closed_captions}")
    return playlists, subtitles, closed_captions

def parse_stream_inf(line, manifest_lines, index, base_url):
    resolution_info = line.split(',')
    resolution = next((info.split('=')[1] for info in resolution_info if 'RESOLUTION' in info), 'unknown')
    playlist_url = urljoin(base_url, manifest_lines[index + 1])
    return resolution, playlist_url

def parse_media_inf(line, base_url, media_type):
    attributes = {attr.split('=')[0]: attr.split('=')[1].strip('"') for attr in line.split(',')}
    uri = attributes.get('URI')
    media_url = urljoin(base_url, uri) if uri else None
    return attributes.get('LANGUAGE', 'unknown'), media_url

def update_target_duration(manifest_path, max_duration):
    try:
        # for moving to next bookmark use the 'alt+ctrl+j'
        with open(manifest_path, 'r') as file:
            lines = file.readlines()
       
        updated_lines = []
        target_duration_updated = False
        for line in lines:
            if line.startswith('#EXT-X-TARGETDURATION'):
                updated_lines.append(f"#EXT-X-TARGETDURATION:{int(max_duration) + 1}\n")
                target_duration_updated = True
            else:
                updated_lines.append(line)
       
        if not target_duration_updated:
            updated_lines.insert(0, f"#EXT-X-TARGETDURATION:{int(max_duration) + 1}\n")
       
        with open(manifest_path, 'w') as file:
            file.writelines(updated_lines)
        logging.info(f"Updated target duration in manifest: {manifest_path}")
    except OSError as e:
        logging.error(f"Failed to update target duration in manifest: {manifest_path}, error: {e}")
