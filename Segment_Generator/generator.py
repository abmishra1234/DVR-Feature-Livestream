import subprocess
import logging
import os
from datetime import datetime

class SegmentGenerator:
    def __init__(self, input_url, output_path, resolution, bitrate):
        logging.debug(f"Initializing SegmentGenerator with input_url={input_url}, output_path={output_path}, resolution={resolution}, bitrate={bitrate}")
        self.input_url = input_url
        self.output_path = output_path
        self.resolution = resolution
        self.bitrate = bitrate

    def generate_segments(self):
        logging.info("Starting the generation of segments.")
        timestamp = datetime.now().strftime('%H%M%S')
        base_filename = f"playlist_{self.resolution}_{timestamp}"
        output_dir = os.path.join(self.output_path, base_filename)
        logging.debug(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        
        self._generate_segment_for_bitrate(base_filename, output_dir)
        self._generate_vtt_files(base_filename, output_dir)
        
        logging.info("Completed the generation of segments.")

    def _generate_segment_for_bitrate(self, base_filename, output_dir):
        segment_filename_pattern = os.path.join(output_dir, f"{base_filename}_%03d.ts")
        playlist_filename = os.path.join(output_dir, f"{base_filename}.m3u8")
        
        command = [
            'ffmpeg', '-i', self.input_url,
            '-c:v', 'libx264', '-b:v', f"{self.bitrate}k",
            '-c:a', 'aac', '-strict', 'experimental',
            '-f', 'hls', '-hls_time', '10', '-hls_list_size', '0',
            '-hls_segment_filename', segment_filename_pattern,
            playlist_filename
        ]
        logging.debug(f"Running command: {' '.join(command)}")
        try:
            subprocess.run(command, check=True)
            logging.info(f"Successfully generated segments for resolution {self.resolution} and bitrate {self.bitrate}k.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error generating segments for resolution {self.resolution} and bitrate {self.bitrate}k: {e}", exc_info=True)
            raise

    def _generate_vtt_files(self, base_filename, output_dir):
        vtt_filename_pattern = os.path.join(output_dir, f"{base_filename}_%03d.vtt")
        vtt_playlist_filename = os.path.join(output_dir, f"{base_filename}_webvtt.m3u8")
        
        command = [
            'ffmpeg', '-i', self.input_url,
            '-f', 'webvtt',
            '-segment_time', '10',
            '-segment_list', vtt_playlist_filename,
            '-segment_list_size', '0',
            '-segment_list_type', 'm3u8',
            '-segment_format', 'webvtt',
            vtt_filename_pattern
        ]
        logging.debug(f"Running command: {' '.join(command)}")
        try:
            subprocess.run(command, check=True)
            logging.info(f"Successfully generated VTT files for resolution {self.resolution} and bitrate {self.bitrate}k.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error generating VTT files for resolution {self.resolution} and bitrate {self.bitrate}k: {e}", exc_info=True)
            raise
