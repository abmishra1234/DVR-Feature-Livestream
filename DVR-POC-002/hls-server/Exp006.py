import logging
from sortedcontainers import SortedDict
from datetime import datetime, timedelta
from threading import Lock

# Configure logging with file name and line number
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)

class SegmentManager:
    def __init__(self):
        self.resolutions = ['1920x1080', '1280x720', '1024x576', '640x360', '384x216']
        self.segment_data = {resolution: SortedDict() for resolution in self.resolutions}
        self.ts_files = {resolution: {} for resolution in self.resolutions}
        self.vtt_files = {resolution: {} for resolution in self.resolutions}
        self.sequence_data = {resolution: {} for resolution in self.resolutions}
        self.lock = Lock()

    def add_segment(self, resolution, date, start_timestamp, sequence_number, duration):
        try:
            with self.lock:
                self.segment_data[resolution][(date, start_timestamp)] = (sequence_number, duration)
                self.sequence_data[resolution][sequence_number] = (date, start_timestamp, duration)
            logger.info(f"Added segment for resolution {resolution}: date={date}, start_timestamp={start_timestamp}, sequence_number={sequence_number}, duration={duration}")
        except Exception as e:
            logger.error(f"Error adding segment: {e}")

    def add_ts_file(self, resolution, sequence_number, ts_file):
        try:
            with self.lock:
                self.ts_files[resolution][sequence_number] = ts_file
            logger.info(f"Added TS file for resolution {resolution}: sequence_number={sequence_number}, ts_file={ts_file}")
        except Exception as e:
            logger.error(f"Error adding TS file: {e}")

    def add_vtt_file(self, resolution, sequence_number, vtt_file):
        try:
            with self.lock:
                self.vtt_files[resolution][sequence_number] = vtt_file
            logger.info(f"Added VTT file for resolution {resolution}: sequence_number={sequence_number}, vtt_file={vtt_file}")
        except Exception as e:
            logger.error(f"Error adding VTT file: {e}")

    def remove_segment(self, resolution, sequence_number):
        try:
            with self.lock:
                if sequence_number in self.sequence_data[resolution]:
                    date, start_timestamp, duration = self.sequence_data[resolution].pop(sequence_number)
                    self.segment_data[resolution].pop((date, start_timestamp), None)
                    self.ts_files[resolution].pop(sequence_number, None)
                    self.vtt_files[resolution].pop(sequence_number, None)
                    logger.info(f"Removed segment for resolution {resolution}: sequence_number={sequence_number}")
                else:
                    logger.warning(f"Attempt to remove non-existing segment for resolution {resolution}: sequence_number={sequence_number}")
        except Exception as e:
            logger.error(f"Error removing segment: {e}")

    def get_segment_by_timestamp(self, resolution, date, timestamp):
        try:
            formatted_timestamp = datetime.strptime(timestamp, "%H:%M:%S.%f").time()
            with self.lock:
                idx = self.segment_data[resolution].bisect_right((date, timestamp))
                if idx == 0:
                    logger.info(f"Timestamp is before the first segment for resolution {resolution}")
                    return None
                (segment_date, start_timestamp), (sequence_number, duration) = self.segment_data[resolution].peekitem(idx - 1)
                start_time = datetime.strptime(start_timestamp, "%H:%M:%S.%f").time()
                end_time = (datetime.combine(datetime.min, start_time) + timedelta(seconds=duration)).time()
                if segment_date == date and start_time <= formatted_timestamp < end_time:
                    ts_file = self.ts_files[resolution].get(sequence_number)
                    vtt_file = self.vtt_files[resolution].get(sequence_number)
                    return sequence_number, ts_file, vtt_file
            logger.info(f"No matching segment found for the given timestamp for resolution {resolution}")
            return None
        except Exception as e:
            logger.error(f"Error getting segment by timestamp: {e}")
            return None

    def get_segment_by_sequence_number(self, resolution, sequence_number):
        try:
            with self.lock:
                if sequence_number in self.sequence_data[resolution]:
                    date, start_timestamp, duration = self.sequence_data[resolution][sequence_number]
                    return start_timestamp, duration
            logger.info(f"No matching segment found for sequence number {sequence_number} for resolution {resolution}")
            return None
        except Exception as e:
            logger.error(f"Error getting segment by sequence number: {e}")
            return None

    def get_last_segments(self, resolution, max_segments=10):
        try:
            with self.lock:
                last_segments = []
                start_idx = max(0, len(self.segment_data[resolution]) - max_segments)
                for i in range(start_idx, len(self.segment_data[resolution])):
                    (date, start_timestamp), (sequence_number, duration) = self.segment_data[resolution].peekitem(i)
                    ts_file = self.ts_files[resolution].get(sequence_number)
                    vtt_file = self.vtt_files[resolution].get(sequence_number)
                    last_segments.append({
                        "resolution": resolution,
                        "sequence_number": sequence_number,
                        "ts_file": ts_file,
                        "date": date,
                        "start_timestamp": start_timestamp,
                        "duration": duration,
                        "vtt_file": vtt_file
                    })
                return last_segments
        except Exception as e:
            logger.error(f"Error getting last segments: {e}")
            return []

    def get_segments_from(self, resolution, date, timestamp, max_segments=10):
        try:
            with self.lock:
                start_idx = self.segment_data[resolution].bisect_left((date, timestamp))
                segments = []
                for i in range(start_idx, min(start_idx + max_segments, len(self.segment_data[resolution]))):
                    (segment_date, start_timestamp), (sequence_number, duration) = self.segment_data[resolution].peekitem(i)
                    ts_file = self.ts_files[resolution].get(sequence_number)
                    vtt_file = self.vtt_files[resolution].get(sequence_number)
                    segments.append({
                        "resolution": resolution,
                        "sequence_number": sequence_number,
                        "ts_file": ts_file,
                        "date": segment_date,
                        "start_timestamp": start_timestamp,
                        "duration": duration,
                        "vtt_file": vtt_file
                    })
                return segments
        except Exception as e:
            logger.error(f"Error getting segments from: {e}")
            return []

# Example usage and testing
if __name__ == "__main__":
    manager = SegmentManager()

    # Adding dummy data for testing
    duration = 6.006
    base_dates = [datetime(2024, 6, 15), datetime(2024, 6, 16), datetime(2024, 6, 17)]
    base_time = datetime.strptime("16:00:00.000", "%H:%M:%S.%f")
    for base_date in base_dates:
        for i in range(7):  # Add 7 segments for each date to have a total of 21 segments
            date = base_date.strftime("%Y-%m-%d")
            time_str = (base_time + timedelta(seconds=i * duration)).strftime("%H:%M:%S.%f")
            sequence_number = 914960 + len(manager.segment_data['1280x720'])
            ts_file = f'playlist_1280x720_160154__{sequence_number}.ts'
            vtt_file = f'playlist_1280x720_160154__{sequence_number}.vtt'
            for resolution in manager.resolutions:
                manager.add_segment(resolution, date, time_str, sequence_number, duration)
                manager.add_ts_file(resolution, sequence_number, ts_file)
                manager.add_vtt_file(resolution, sequence_number, vtt_file)

    # Fetch last 10 segments for each resolution
    for resolution in manager.resolutions:
        print(f"\nLast 10 segments for resolution {resolution}:")
        last_segments = manager.get_last_segments(resolution)
        for segment in last_segments:
            print(segment)

    # Fetch segments starting from a given date and timestamp for each resolution
    for resolution in manager.resolutions:
        print(f"\nSegments starting from 16th June 2024, 16:00:12.000 for resolution {resolution}:")
        segments_from = manager.get_segments_from(resolution, "2024-06-16", "16:00:12.000")
        for segment in segments_from:
            print(segment)
