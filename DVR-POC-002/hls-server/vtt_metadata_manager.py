# vtt_metadata_manager.py

import logging
from sortedcontainers import SortedDict
from datetime import datetime, timedelta
from threading import Lock

class VTTMetadataManager:
    def __init__(self, logger):
        self.languages = ['eng']
        self.segment_data = {language: SortedDict() for language in self.languages}
        self.vtt_files = {language: {} for language in self.languages}
        self.sequence_data = {language: {} for language in self.languages}
        self.lock = Lock()
        self.logger = logger

    def add_vttmetadata(self, language, date, start_timestamp, sequence_number, duration, vtt_file):
        try:
            with self.lock:
                end_timestamp = (datetime.strptime(start_timestamp, '%H:%M:%S.%f') + timedelta(seconds=duration)).strftime('%H:%M:%S.%f')
                self.segment_data[language][(date, end_timestamp)] = (sequence_number, start_timestamp, duration)
                self.sequence_data[language][sequence_number] = (date, start_timestamp, duration)
                self.vtt_files[language][sequence_number] = vtt_file
            self.logger.info(f"Added VTT metadata for language - '{language}': date={date}, start_timestamp={start_timestamp}, sequence_number={sequence_number}, duration={duration}, vtt_file={vtt_file}")
        except Exception as e:
            self.logger.error(f"Error adding VTT metadata: {e}")

    def remove_vttmetadata(self, language, sequence_number):
        try:
            with self.lock:
                if sequence_number in self.sequence_data[language]:
                    date, start_timestamp, duration = self.sequence_data[language].pop(sequence_number)
                    end_timestamp = (datetime.strptime(start_timestamp, '%H:%M:%S.%f') + timedelta(seconds=duration)).strftime('%H:%M:%S.%f')
                    self.segment_data[language].pop((date, end_timestamp), None)
                    self.vtt_files[language].pop(sequence_number, None)
                    self.logger.info(f"Removed VTT metadata for language - '{language}': sequence_number={sequence_number}")
                else:
                    self.logger.warning(f"Attempt to remove non-existing segment for language {language}: sequence_number={sequence_number}")
        except Exception as e:
            self.logger.error(f"Error removing VTT metadata: {e}")

    def get_live_playlist(self, language, max_segments=10):
        try:
            with self.lock:
                if language not in self.segment_data:
                    self.logger.error(f"No segment data found for resolution: {language}")
                    return {"error": "No segment data found for the given language"}

                if len(self.segment_data[language]) < 20:
                    self.logger.info(f"Too few segments to return live subtitle playlist \
                        for language {language}, so wait...")
                    return {"error": "Too few segments to return, WAIT..."}

                live_playlist = []
                # Get the last 20 segments
                start_idx = max(0, len(self.segment_data[language]) - 20)
                last_segments = list(self.segment_data[language].items())[start_idx:]
                # Fetch the first `max_segments` from the last segments
                for i in range(min(max_segments, len(last_segments))):
                    (date, end_timestamp), (sequence_number, start_timestamp, duration) = last_segments[i]
                    vtt_file = self.vtt_files[language].get(sequence_number)
                    live_playlist.append({
                        "language": language,
                        "sequence_number": sequence_number,
                        "date": date,
                        "start_timestamp": start_timestamp,
                        "duration": duration,
                        "vtt_file": vtt_file
                    })
                self.logger.info(f"Live playlist of last {max_segments} segments for language {language}: {live_playlist}")
                return live_playlist
        except Exception as e:
            self.logger.error(f"Error getting live playlist: {e}")
            return {"error": f"Error getting live playlist: {e}"}
        
    def get_dvr_playlist(self, language, date, timestamp, max_segments=10):
        try:
            with self.lock:
                # Convert timestamp to datetime for comparison
                timestamp_dt = datetime.strptime(timestamp, '%H:%M:%S.%f')
                dvr_playlist = []

                # Find the starting index using bisect_left
                start_idx = self.segment_data[language].bisect_left((date, timestamp))
                
                # Iterate to find the correct segment
                for i in range(start_idx, len(self.segment_data[language])):
                    (segment_date, end_timestamp), (sequence_number, start_timestamp, duration) = self.segment_data[language].peekitem(i)
                    start_timestamp_dt = datetime.strptime(start_timestamp, '%H:%M:%S.%f')
                    end_timestamp_dt = start_timestamp_dt + timedelta(seconds=duration)

                    if segment_date == date and start_timestamp_dt <= timestamp_dt <= end_timestamp_dt:
                        # Add the found segment and next segments up to max_segments
                        for j in range(i, min(i + max_segments, len(self.segment_data[language]))):
                            (next_segment_date, next_end_timestamp), (next_sequence_number, next_start_timestamp, next_duration) = self.segment_data[language].peekitem(j)
                            next_vtt_file = self.vtt_files[language].get(next_sequence_number)
                            dvr_playlist.append({
                                "language": language,
                                "sequence_number": next_sequence_number,
                                "date": next_segment_date,
                                "start_timestamp": next_start_timestamp,
                                "duration": next_duration,
                                "vtt_file": next_vtt_file
                            })
                        break

                self.logger.info(f"DVR playlist from {timestamp} for language {language}: {dvr_playlist}")
                return dvr_playlist
        except ValueError as e:
            self.logger.error(f"ValueError in timestamp conversion: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting DVR playlist: {e}")
            return []

# Example usage:
# logger = logging.getLogger(__name__)
# vtt_metadata_manager = VTTMetadataManager(logger)
# vtt_metadata_manager.add_vttmetadata('eng', '2023-07-02', '12:00:00.000', 1, 10, 'file1.vtt')
