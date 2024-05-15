import logging
from fetcher import HLSFetcher
from generator import SegmentGenerator

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
    hls_url = "https://example.com/live/stream.m3u8"  # Replace with actual URL
    output_path = "/path/to/output"  # Replace with actual output path
    bitrates = ["500k", "1000k", "1500k"]  # Example bitrates

    service = MainService(hls_url, output_path, bitrates)
    service.run()
