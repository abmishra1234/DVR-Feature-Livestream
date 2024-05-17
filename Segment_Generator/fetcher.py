import requests
import logging

class HLSFetcher:
    def __init__(self, url):
        self.url = url
        logging.debug(f"HLSFetcher initialized with URL: {url}")

    def fetch(self):
        logging.debug(f"Starting fetch method for URL: {self.url}")
        try:
            response = requests.get(self.url, stream=True)
            logging.debug(f"HTTP GET request sent to URL: {self.url}")

            response.raise_for_status()
            logging.info("Successfully fetched the HLS livestream.")
            
            logging.debug(f"Response URL: {response.url}")
            return response.url
        except requests.RequestException as e:
            logging.error(f"Error fetching HLS livestream: {e}", exc_info=True)
            raise

    def parse_m3u8(self):
        logging.debug(f"Parsing m3u8 file from URL: {self.url}")
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            lines = response.text.splitlines()
            for line in lines:
                if line.startswith("#EXT-X-STREAM-INF"):
                    info = line.split(":")[1]
                    parts = info.split(",")
                    bandwidth = [p for p in parts if p.startswith("BANDWIDTH")][0].split("=")[1]
                    resolution = [p for p in parts if p.startswith("RESOLUTION")][0].split("=")[1]
                    return resolution, int(bandwidth) // 1000  # Convert bitrate to kbps
        except requests.RequestException as e:
            logging.error(f"Error parsing m3u8 file: {e}", exc_info=True)
            raise

        logging.error("Failed to parse resolution and bitrate from m3u8 file")
        return None, None
