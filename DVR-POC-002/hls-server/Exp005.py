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
        self.segment_data = SortedDict()  # Key: (date, start_timestamp), Value: (sequence_number, duration)
        self.ts_files = {}  # Key: sequence_number, Value: ts_file
        self.vtt_files = {}  # Key: sequence_number, Value: vtt_file
        self.sequence_data = {}  # Key: sequence_number, Value: (date, start_timestamp, duration)
        self.lock = Lock()

    def add_segment(self, date, start_timestamp, sequence_number, duration):
        try:
            with self.lock:
                self.segment_data[(date, start_timestamp)] = (sequence_number, duration)
                self.sequence_data[sequence_number] = (date, start_timestamp, duration)
            logger.info(f"Added segment: date={date}, start_timestamp={start_timestamp}, sequence_number={sequence_number}, duration={duration}")
        except Exception as e:
            logger.error(f"Error adding segment: {e}")

    def add_ts_file(self, sequence_number, ts_file):
        try:
            with self.lock:
                self.ts_files[sequence_number] = ts_file
            logger.info(f"Added TS file: sequence_number={sequence_number}, ts_file={ts_file}")
        except Exception as e:
            logger.error(f"Error adding TS file: {e}")

    def add_vtt_file(self, sequence_number, vtt_file):
        try:
            with self.lock:
                self.vtt_files[sequence_number] = vtt_file
            logger.info(f"Added VTT file: sequence_number={sequence_number}, vtt_file={vtt_file}")
        except Exception as e:
            logger.error(f"Error adding VTT file: {e}")

    def remove_segment(self, sequence_number):
        try:
            with self.lock:
                if sequence_number in self.sequence_data:
                    date, start_timestamp, duration = self.sequence_data.pop(sequence_number)
                    self.segment_data.pop((date, start_timestamp), None)
                    self.ts_files.pop(sequence_number, None)
                    self.vtt_files.pop(sequence_number, None)
                    logger.info(f"Removed segment: sequence_number={sequence_number}")
                else:
                    logger.warning(f"Attempt to remove non-existing segment: sequence_number={sequence_number}")
        except Exception as e:
            logger.error(f"Error removing segment: {e}")

    def get_segment_by_timestamp(self, date, timestamp):
        try:
            formatted_timestamp = datetime.strptime(timestamp, "%H:%M:%S.%f").time()
            with self.lock:
                idx = self.segment_data.bisect_right((date, timestamp))
                if idx == 0:
                    logger.info("Timestamp is before the first segment")
                    return None
                (segment_date, start_timestamp), (sequence_number, duration) = self.segment_data.peekitem(idx - 1)
                start_time = datetime.strptime(start_timestamp, "%H:%M:%S.%f").time()
                end_time = (datetime.combine(datetime.min, start_time) + timedelta(seconds=duration)).time()
                if segment_date == date and start_time <= formatted_timestamp < end_time:
                    ts_file = self.ts_files.get(sequence_number)
                    vtt_file = self.vtt_files.get(sequence_number)
                    return sequence_number, ts_file, vtt_file
            logger.info("No matching segment found for the given timestamp")
            return None
        except Exception as e:
            logger.error(f"Error getting segment by timestamp: {e}")
            return None

    def get_segment_by_sequence_number(self, sequence_number):
        try:
            with self.lock:
                if sequence_number in self.sequence_data:
                    date, start_timestamp, duration = self.sequence_data[sequence_number]
                    return start_timestamp, duration
            logger.info(f"No matching segment found for sequence number: {sequence_number}")
            return None
        except Exception as e:
            logger.error(f"Error getting segment by sequence number: {e}")
            return None

    def get_last_segments(self, max_segments=10):
        try:
            with self.lock:
                last_segments = []
                start_idx = max(0, len(self.segment_data) - max_segments)
                for i in range(start_idx, len(self.segment_data)):
                    (date, start_timestamp), (sequence_number, duration) = self.segment_data.peekitem(i)
                    ts_file = self.ts_files.get(sequence_number)
                    vtt_file = self.vtt_files.get(sequence_number)
                    last_segments.append({
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

    def get_segments_from(self, date, timestamp, max_segments=10):
        try:
            with self.lock:
                start_idx = self.segment_data.bisect_left((date, timestamp))
                segments = []
                for i in range(start_idx, min(start_idx + max_segments, len(self.segment_data))):
                    (segment_date, start_timestamp), (sequence_number, duration) = self.segment_data.peekitem(i)
                    ts_file = self.ts_files.get(sequence_number)
                    vtt_file = self.vtt_files.get(sequence_number)
                    segments.append({
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
            sequence_number = 914960 + len(manager.segment_data)
            ts_file = f'playlist_1280x720_160154__{sequence_number}.ts'
            vtt_file = f'playlist_1280x720_160154__{sequence_number}.vtt'
            manager.add_segment(date, time_str, sequence_number, duration)
            manager.add_ts_file(sequence_number, ts_file)
            manager.add_vtt_file(sequence_number, vtt_file)

    # Fetch last 10 segments
    print("Last 10 segments:")
    last_segments = manager.get_last_segments()
    for segment in last_segments:
        print(segment)

    # Fetch segments starting from a given date and timestamp
    print("\nSegments starting from 16th June 2024, 16:00:12.000:")
    segments_from = manager.get_segments_from("2024-06-16", "16:00:12.000")
    for segment in segments_from:
        print(segment)
