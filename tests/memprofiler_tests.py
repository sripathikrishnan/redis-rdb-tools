import unittest

from rdbtools import RdbParser
from rdbtools import MemoryCallback
import os

class Stats(object):
    def __init__(self):
        self.records = {}
        
    def next_record(self, record):
        self.records[record.key] = record

def get_stats(file_name):
    stats = Stats()
    callback = MemoryCallback(stats, 64)
    parser = RdbParser(callback)
    parser.parse(os.path.join(os.path.dirname(__file__), 'dumps', file_name))
    return stats.records
    
class MemoryCallbackTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_len_largest_element(self):
        stats = get_stats('ziplist_that_compresses_easily.rdb')

        self.assertEqual(stats[b'ziplist_compresses_easily'].len_largest_element, 36, "Length of largest element does not match")
