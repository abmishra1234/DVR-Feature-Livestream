import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
from io import StringIO
import logging
from main import load_config, clean_old_segments
from datetime import datetime, timedelta

class TestCleanupService(unittest.TestCase):
    
    @patch('builtins.open', new_callable=mock_open, 
        read_data='{"directory": "test_dir", "retention_period": 30, \
        "polling_interval": 60, "exception_list": ["keep.me"]}')
    @patch('os.makedirs')
    def test_load_config(self, mock_makedirs, mock_file):
        config = load_config()
        self.assertEqual(config['directory'], 'test_dir')
        self.assertEqual(config['retention_period'], 30)
        self.assertEqual(config['polling_interval'], 60)
        self.assertIn('keep.me', config['exception_list'])
    
    @patch('os.path.exists', return_value=True)
    @patch('os.walk', return_value=[('root', [], ['oldfile.ts', 'keep.me'])])
    @patch('os.path.getmtime', 
        return_value=(datetime.now() - timedelta(minutes=40)).timestamp())
    @patch('os.remove')
    def test_clean_old_segments(self, mock_remove, mock_getmtime, 
            mock_walk, mock_exists):
        directory = 'test_dir'
        retention_period = 30
        exception_set = {'keep.me'}
        
        with self.assertLogs(level='INFO') as log:
            clean_old_segments(directory, retention_period, exception_set)
            self.assertIn('Deleted old file:', log.output[-2])
            self.assertIn('Total files deleted: 1', log.output[-1])

if __name__ == "__main__":
    unittest.main()
