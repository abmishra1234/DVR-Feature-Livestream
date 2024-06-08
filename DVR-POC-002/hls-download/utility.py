import os
import json
import logging
import requests

from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime

def load_config(config_path='config.json'):
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config

def setup_logging(logFile):
    # Define the logs directory
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)

    # Create a timestamped log file
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_directory, f"{logFile}_{date_str}.log")

    # Set up logging configuration
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format=log_format)
    logging.info("Configuration loaded and directories set up.")

def download_file(url, save_path, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as file:
            file.write(response.content)
        logging.info(f"Downloaded file: {save_path}")
    except requests.Timeout:
        logging.error(f"Timeout occurred while downloading {url}")
    except requests.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
    except OSError as e:
        logging.error(f"OS error when saving file {save_path}: {e}")

def parse_master_manifest(url, download_dir, master_manifest_name):
    response = requests.get(url)
    response.raise_for_status()
    manifest_content = response.text
    playlists = []
    subtitles = []
    closed_captions = []

    master_manifest_path = download_dir / master_manifest_name #######'playlist.m3u8'
    with open(master_manifest_path, 'w') as file:
        file.write(manifest_content)
    logging.info(f"Downloaded master manifest: {master_manifest_path}")

    for line in manifest_content.splitlines():
        if line.startswith('#EXT-X-STREAM-INF'):
            resolution_info = line.split(',')
            for info in resolution_info:
                if 'RESOLUTION' in info:
                    resolution = info.split('=')[1]
                    playlists.append((resolution, None))
        elif line.startswith('#EXT-X-MEDIA') and 'TYPE=SUBTITLES' in line:
            attributes = {attr.split('=')[0]: attr.split('=')[1].strip('"') for attr in line.split(',')}
            if 'URI' in attributes:
                subtitle_url = urljoin(url, attributes['URI'])
                subtitles.append((attributes.get('LANGUAGE', 'unknown'), subtitle_url))
        elif line.startswith('#EXT-X-MEDIA') and 'TYPE=CLOSED-CAPTIONS' in line:
            attributes = {attr.split('=')[0]: attr.split('=')[1].strip('"') for attr in line.split(',')}
            closed_captions.append(attributes)
        elif line and not line.startswith('#'):
            if playlists[-1][1] is None:
                playlists[-1] = (playlists[-1][0], urljoin(url, line))

    logging.info(f"Parsed playlists from master manifest: {playlists}")
    logging.info(f"Parsed subtitles from master manifest: {subtitles}")
    logging.info(f"Parsed closed captions from master manifest: {closed_captions}")
    return playlists, subtitles, closed_captions

def update_target_duration(manifest_path, max_duration):
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


