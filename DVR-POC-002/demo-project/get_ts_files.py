import os

class TSFileBatcher:
    def __init__(self, directory):
        self.directory = directory
        self.files = self._get_sorted_ts_files()
        self.index = 0

    def _get_sorted_ts_files(self):
        files = [f for f in os.listdir(self.directory) if f.endswith('.ts')]
        files.sort()  # Sorting based on name, alternatively use os.path.getctime for creation time
        return files

    def get_next_batch(self, batch_size=10):
        if self.index >= len(self.files):
            return []

        batch = self.files[self.index:self.index + batch_size]
        self.index += batch_size
        return batch

'''
# Usage example:
directory =  "../hls-download/hls_data/1280x720"#"/path/to/your/ts/files"
batcher = TSFileBatcher(directory)

# First call
first_batch = batcher.get_next_batch()
print("First batch:", first_batch)

# Second call
second_batch = batcher.get_next_batch()
print("Second batch:", second_batch)

# Third call
third_batch = batcher.get_next_batch()
print("Third batch:", third_batch)

# And so on...
'''
