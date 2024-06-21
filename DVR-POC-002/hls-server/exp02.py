from sortedcontainers import SortedDict
from datetime import datetime, timedelta

class SegmentManager:
    def __init__(self):
        self.segment_data = SortedDict()  # Key: (date, timestamp), Value: (sequence_number, duration, ts_file, vtt_file)
        self.sequence_data = {}  # Key: sequence_number, Value: (date, timestamp, duration)

    def _current_date(self):
        return datetime.now().strftime("%Y-%m-%d")

    def _parse_time(self, time_str):
        return datetime.strptime(time_str, "%H:%M:%S.%f")

    def _format_time(self, time_obj):
        return time_obj.strftime("%H:%M:%S.%f")

    def add_segment(self, start_time_str, sequence_number, duration, ts_file, vtt_file):
        date = self._current_date()
        start_time = self._parse_time(start_time_str)
        self.segment_data[(date, start_time_str)] = (sequence_number, duration, ts_file, vtt_file)
        self.sequence_data[sequence_number] = (date, start_time_str, duration)

    def get_segment_by_timestamp(self, time_str):
        # Find the segment that covers the given timestamp
        date = self._current_date()
        time_obj = self._parse_time(time_str)
        idx = self.segment_data.bisect_right((date, time_str))
        if idx == 0:
            return None  # Timestamp is before the first segment
        (segment_date, start_time_str), (sequence_number, duration, ts_file, vtt_file) = self.segment_data.peekitem(idx - 1)
        start_time = self._parse_time(start_time_str)
        if segment_date == date and start_time <= time_obj < start_time + timedelta(seconds=duration):
            return sequence_number, ts_file, vtt_file
        return None

    def get_segment_by_sequence_number(self, sequence_number):
        if sequence_number in self.sequence_data:
            date, start_time_str, duration = self.sequence_data[sequence_number]
            return start_time_str, duration
        return None

# Example usage
manager = SegmentManager()
manager.add_segment("16:06:05.262", 1, 10, 'segment1.ts', 'segment1.vtt')
manager.add_segment("16:06:15.262", 2, 10, 'segment2.ts', 'segment2.vtt')

# Fetch segment by timestamp
print(manager.get_segment_by_timestamp("16:06:08.262"))  # Output: (1, 'segment1.ts', 'segment1.vtt')

# Fetch start timestamp and duration by sequence number
print(manager.get_segment_by_sequence_number(2))  # Output: ('16:06:15.262', 10)
