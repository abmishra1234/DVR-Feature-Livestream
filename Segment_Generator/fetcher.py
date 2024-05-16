import requests
import logging

class HLSFetcher:
    def __init__(self, url):
        self.url = url

    def fetch(self):
        try:
            response = requests.get(self.url, stream=True)
            response.raise_for_status()
            logging.info("Successfully fetched the HLS livestream.")
            return response.url
        except requests.RequestException as e:
            logging.error(f"Error fetching HLS livestream: {e}")
            raise
