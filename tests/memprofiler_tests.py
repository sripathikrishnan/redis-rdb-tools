import unittest

from rdbtools import RdbParser
from rdbtools import MemoryCallback,PrintAllKeys,MemoryRecord
import os
import io

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

        self.assertEqual(stats['ziplist_compresses_easily'].len_largest_element, 36, "Length of largest element does not match")

class PrintAllKeysTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_emits_valid_csv(self):
        stream = io.BytesIO()

        printer = PrintAllKeys(stream, None, None)
        printer.next_record(MemoryRecord(0, "string", "First,Second", 104, "string", 8, 8))
        printer.next_record(MemoryRecord(0, "string", 'json:{"key": "value"}', 104, "string", 8, 8))

        expected_csv = b"""database,type,key,size_in_bytes,encoding,num_elements,len_largest_element
0,string,"First,Second",104,string,8,8
0,string,"json:{""key"": ""value""}",104,string,8,8
"""
        self.assertEqual(stream.getvalue(), expected_csv)

