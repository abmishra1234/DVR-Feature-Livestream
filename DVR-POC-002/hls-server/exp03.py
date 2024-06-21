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
            #date = date.strftime("%Y-%m-%d")
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

# Example usage
if __name__ == "__main__":
    manager = SegmentManager()
    manager.add_segment("2024-06-15", "16:06:05.262", 914960, 6.00600)
    manager.add_ts_file(914960, 'playlist_1280x720_160154__914960.ts')
    manager.add_vtt_file(914960, 'playlist_1280x720_160154__914960.vtt')

    manager.add_segment("2024-06-16", "16:06:11.268", 914961, 6.00600)
    manager.add_ts_file(914961, 'playlist_1280x720_160154__914961.ts')
    manager.add_vtt_file(914961, 'playlist_1280x720_160154__914961.vtt')

    # Fetch segment by timestamp
    # Output: (914960, 'playlist_1280x720_160154__914960.ts', 'playlist_1280x720_160154__914960.vtt')
    print(manager.get_segment_by_timestamp("2024-06-15", "16:06:07.000"))  

    # Fetch segment by timestamp (fail case)
    # Output: None
    print(manager.get_segment_by_timestamp("2024-06-15", "16:06:05.000"))  

    # Fetch start timestamp and duration by sequence number
    print(manager.get_segment_by_sequence_number(914961))  # Output: ('16:06:11.268', 6.00600)

    # Fetch start timestamp and duration by sequence number (fail case)
    # Output: None
    print(manager.get_segment_by_sequence_number(61))  

    # Remove segment by sequence number
    manager.remove_segment(914960)
    # Output: None
    print(manager.get_segment_by_sequence_number(914960))  

    # add again
    manager.add_segment("2024-06-15", "16:06:05.266", 914960, 6.00600)
    manager.add_ts_file(914960, 'playlist_1280x720_160154__914960.ts')
    manager.add_vtt_file(914960, 'playlist_1280x720_160154__914960.vtt')

    # search and this time youwill fine the value exist
    # Output: ('16:06:05.265', 6.006)
    print(manager.get_segment_by_sequence_number(914960))  
    # this could be the pass case
    #output : (914960, 'playlist_1280x720_160154__914960.ts', 'playlist_1280x720_160154__914960.vtt')
    print(manager.get_segment_by_timestamp("2024-06-15", "16:06:11.271"))  
    # this could be the fail case
    # output : None
    print(manager.get_segment_by_timestamp("2024-06-15", "16:06:11.272"))  
