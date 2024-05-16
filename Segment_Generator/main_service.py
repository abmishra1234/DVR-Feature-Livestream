import logging
import os
from fetcher import HLSFetcher
from generator import SegmentGenerator
from logger import setup_logger

class MainService:
    def __init__(self, hls_url, output_path, bitrates):
        self.hls_url = hls_url
        self.output_path = output_path
        self.bitrates = bitrates

    def run(self):
        try:
            fetcher = HLSFetcher(self.hls_url)
            final_url = fetcher.fetch()

            generator = SegmentGenerator(final_url, self.output_path, self.bitrates)
            generator.generate_segments()

            logging.info("Segment generation process completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred in the main service: {e}")

if __name__ == "__main__":
    # Replace with actual values
    hls_url = "https://d3kpx1cp1t9frc.cloudfront.net/index.m3u8"
    output_path = os.path.join(os.getcwd(), "output")
    bitrates = ["500k", "1000k", "1500k"]

    setup_logger()
    service = MainService(hls_url, output_path, bitrates)
    service.run()
