import sys
import os
from io import BytesIO

import unittest

from rdbtools import RdbParser
from rdbtools import MemoryCallback


from rdbtools.memprofiler import MemoryRecord, PrintAllKeys

CSV_WITH_EXPIRY = """database,type,key,size_in_bytes,encoding,num_elements,len_largest_element,expiry
0,string,expires_ms_precision,128,string,27,27,2022-12-25T10:11:12.573000
"""

CSV_WITHOUT_EXPIRY = """database,type,key,size_in_bytes,encoding,num_elements,len_largest_element,expiry
0,list,ziplist_compresses_easily,301,quicklist,6,36,
"""

CSV_WITH_MODULE = """database,type,key,size_in_bytes,encoding,num_elements,len_largest_element,expiry
0,string,simplekey,72,string,7,7,
0,module,foo,101,ReJSON-RL,1,101,
"""

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

def get_csv(dump_file_name):
    buff = BytesIO()
    callback = MemoryCallback(PrintAllKeys(buff, None, None), 64)
    parser = RdbParser(callback)
    parser.parse(os.path.join(os.path.dirname(__file__), 
                    'dumps', dump_file_name))
    csv = buff.getvalue().decode()
    return csv

class MemoryCallbackTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_csv_with_expiry(self):
        csv = get_csv('keys_with_expiry.rdb')
        self.assertEquals(csv, CSV_WITH_EXPIRY)

    def test_csv_without_expiry(self):
        csv = get_csv('ziplist_that_compresses_easily.rdb')
        self.assertEquals(csv, CSV_WITHOUT_EXPIRY)

    def test_csv_with_module(self):
        csv = get_csv('redis_40_with_module.rdb')
        self.assertEquals(csv, CSV_WITH_MODULE)

    def test_expiry(self):
        stats = get_stats('keys_with_expiry.rdb')

        expiry = stats['expires_ms_precision'].expiry
        self.assertEquals(expiry.year, 2022)
        self.assertEquals(expiry.month, 12)
        self.assertEquals(expiry.day, 25)
        self.assertEquals(expiry.hour, 10)
        self.assertEquals(expiry.minute, 11)
        self.assertEquals(expiry.second, 12)
        self.assertEquals(expiry.microsecond, 573000)        

    def test_len_largest_element(self):
        stats = get_stats('ziplist_that_compresses_easily.rdb')

        self.assertEqual(stats['ziplist_compresses_easily'].len_largest_element, 36, "Length of largest element does not match")

    def test_rdb_with_module(self):
        stats = get_stats('redis_40_with_module.rdb')

        self.assertTrue('simplekey' in stats)
        self.assertTrue('foo' in stats)
        expected_record = MemoryRecord(database=0, type='module', key='foo',
                                       bytes=101, encoding='ReJSON-RL', size=1,
                                       len_largest_element=101, expiry=None)
        self.assertEquals(stats['foo'], expected_record)
