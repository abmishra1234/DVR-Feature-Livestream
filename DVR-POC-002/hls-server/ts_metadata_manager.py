# ts_metadata_manager.py

import logging
from sortedcontainers import SortedDict
from datetime import datetime, timedelta
from threading import Lock

class TSMetadataManager:
    def __init__(self, logger):
        self.resolutions = ['1920x1080', '1280x720', '1024x576', '640x360', '384x216']
        self.segment_data = {resolution: SortedDict() for resolution in self.resolutions}
        self.ts_files = {resolution: {} for resolution in self.resolutions}
        self.sequence_data = {resolution: {} for resolution in self.resolutions}
        self.lock = Lock()
        self.logger = logger

    def add_tsmetadata(self, resolution, date, start_timestamp, sequence_number, duration, ts_file):
        try:
            with self.lock:
                end_timestamp = (datetime.strptime(start_timestamp, '%H:%M:%S.%f') + timedelta(seconds=duration)).strftime('%H:%M:%S.%f')
                self.segment_data[resolution][(date, end_timestamp)] = (sequence_number, start_timestamp, duration)
                self.sequence_data[resolution][sequence_number] = (date, start_timestamp, duration)
                self.ts_files[resolution][sequence_number] = ts_file
            self.logger.info(f"Added TS metadata for resolution {resolution}: date={date}, start_timestamp={start_timestamp}, sequence_number={sequence_number}, duration={duration}, ts_file={ts_file}")
        except Exception as e:
            self.logger.error(f"Error adding TS metadata: {e}")

    def remove_tsmetadata(self, resolution, sequence_number):
        try:
            with self.lock:
                if sequence_number in self.sequence_data[resolution]:
                    date, start_timestamp, duration = self.sequence_data[resolution].pop(sequence_number)
                    end_timestamp = (datetime.strptime(start_timestamp, '%H:%M:%S.%f') + timedelta(seconds=duration)).strftime('%H:%M:%S.%f')
                    self.segment_data[resolution].pop((date, end_timestamp), None)
                    self.ts_files[resolution].pop(sequence_number, None)
                    self.logger.info(f"Removed segment for resolution {resolution}: sequence_number={sequence_number}")
                else:
                    self.logger.warning(f"Attempt to remove non-existing segment for resolution {resolution}: sequence_number={sequence_number}")
        except Exception as e:
            self.logger.error(f"Error removing segment: {e}")

    def get_live_playlist(self, resolution, max_segments=10):
        try:
            with self.lock:
                live_playlist = []
                # Get the last 20 segments
                start_idx = max(0, len(self.segment_data[resolution]) - 20)
                last_segments = list(self.segment_data[resolution].items())[start_idx:]
                # Fetch the first `max_segments` from the last segments
                for i in range(min(max_segments, len(last_segments))):
                    (date, end_timestamp), (sequence_number, start_timestamp, duration) = last_segments[i]
                    ts_file = self.ts_files[resolution].get(sequence_number)
                    live_playlist.append({
                        "resolution": resolution,
                        "sequence_number": sequence_number,
                        "date": date,
                        "start_timestamp": start_timestamp,
                        "duration": duration,
                        "ts_file": ts_file
                    })
                self.logger.info(f"Live playlist of last {max_segments} segments for resolution {resolution}: {live_playlist}")
                return live_playlist
        except Exception as e:
            self.logger.error(f"Error getting live playlist: {e}")
            return []

    def get_dvr_playlist(self, resolution, date, timestamp, max_segments=10):
        try:
            with self.lock:
                # Convert timestamp to datetime for comparison
                timestamp_dt = datetime.strptime(timestamp, '%H:%M:%S.%f')
                dvr_playlist = []

                # Find the starting index using bisect_left
                start_idx = self.segment_data[resolution].bisect_left((date, timestamp))
                
                # Iterate to find the correct segment
                for i in range(start_idx, len(self.segment_data[resolution])):
                    (segment_date, end_timestamp), (sequence_number, start_timestamp, duration) = self.segment_data[resolution].peekitem(i)
                    start_timestamp_dt = datetime.strptime(start_timestamp, '%H:%M:%S.%f')
                    end_timestamp_dt = start_timestamp_dt + timedelta(seconds=duration)

                    if segment_date == date and start_timestamp_dt <= timestamp_dt <= end_timestamp_dt:
                        # Add the found segment and next segments up to max_segments
                        for j in range(i, min(i + max_segments, len(self.segment_data[resolution]))):
                            (next_segment_date, next_end_timestamp), (next_sequence_number, next_start_timestamp, next_duration) = self.segment_data[resolution].peekitem(j)
                            next_ts_file = self.ts_files[resolution].get(next_sequence_number)
                            dvr_playlist.append({
                                "resolution": resolution,
                                "sequence_number": next_sequence_number,
                                "date": next_segment_date,
                                "start_timestamp": next_start_timestamp,
                                "duration": next_duration,
                                "ts_file": next_ts_file
                            })
                        break

                self.logger.info(f"DVR playlist from {timestamp} for resolution {resolution}: {dvr_playlist}")
                return dvr_playlist
        except ValueError as e:
            self.logger.error(f"ValueError in timestamp conversion: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting DVR playlist: {e}")
            return []

# Example usage:
# logger = logging.getLogger(__name__)
# ts_metadata_manager = TSMetadataManager(logger)
# ts_metadata_manager.add_tsmetadata('1920x1080', '2023-07-02', '12:00:00.000', 1, 10, 'file1.ts')
