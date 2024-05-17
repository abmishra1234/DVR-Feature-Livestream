import logging
import os
from fetcher import HLSFetcher
from generator import SegmentGenerator
from logger import setup_logger

class MainService:
    def __init__(self, hls_url, output_path):
        logging.debug(f"Initializing MainService with hls_url: {hls_url}, output_path: {output_path}")
        self.hls_url = hls_url
        self.output_path = output_path

    def run(self):
        logging.info("MainService run method started.")
        try:
            logging.debug(f"Creating HLSFetcher with URL: {self.hls_url}")
            fetcher = HLSFetcher(self.hls_url)
            logging.debug("Calling fetch method on HLSFetcher.")
            final_url = fetcher.fetch()
            resolution, bitrate = fetcher.parse_m3u8()

            if resolution and bitrate:
                generator = SegmentGenerator(final_url, self.output_path)
                logging.debug("Calling generate_segments method on SegmentGenerator.")
                generator.generate_segments()
                logging.info("Segment generation process completed successfully.")
            else:
                logging.error("Failed to parse resolution and bitrate from m3u8 file.")
        except Exception as e:
            logging.error(f"An error occurred in the main service: {e}", exc_info=True)

if __name__ == "__main__":
    # Replace with actual values
    hls_url = "https://d19y7l1gyy74p9.cloudfront.net/playlist.m3u8"
    output_path = os.path.join(os.getcwd(), "output")

    #setup_logger()
    logging.info("###############Starting the MainService application.###############\n\n")
    service = MainService(hls_url, output_path)
    service.run()
    logging.info("\n\n###################MainService application finished.###################")
