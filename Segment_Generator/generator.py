import subprocess
import logging
import os
import platform

class SegmentGenerator:
    def __init__(self, input_url, output_path, bitrates):
        self.input_url = input_url
        self.output_path = output_path
        self.bitrates = bitrates

    def generate_segments(self):
        for bitrate in self.bitrates:
            output_dir = os.path.join(self.output_path, bitrate)
            os.makedirs(output_dir, exist_ok=True)
            self._generate_segment_for_bitrate(bitrate, output_dir)

    def _generate_segment_for_bitrate(self, bitrate, output_dir):
        command = [
            'ffmpeg', '-i', self.input_url,
            '-c:v', 'libx264', '-b:v', bitrate,
            '-c:a', 'aac', '-strict', 'experimental',
            '-f', 'hls', '-hls_time', '10', '-hls_list_size', '0',
            '-hls_segment_filename', os.path.join(output_dir, 'segment%03d.ts'),
            os.path.join(output_dir, 'index.m3u8')
        ]
        try:
            subprocess.run(command, check=True)
            logging.info(f"Successfully generated segments at {bitrate}.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error generating segments for bitrate {bitrate}: {e}")
            raise
