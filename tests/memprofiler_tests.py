import unittest

from rdbtools import RdbParser
from rdbtools import MemoryCallback
import os

from rdbtools.memprofiler import MemoryRecord


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

    def test_rdb_with_module(self):
        stats = get_stats('redis_40_with_module.rdb')

        self.assertTrue('simplekey' in stats)
        self.assertTrue('foo' in stats)
        expected_record = MemoryRecord(database=0, type='module', key='foo',
                                       bytes=101, encoding='ReJSON-RL', size=1,
                                       len_largest_element=101)
        self.assertEquals(stats['foo'], expected_record)
