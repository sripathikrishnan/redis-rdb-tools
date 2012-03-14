import unittest
import os
from rdbtools.parser import RdbCallback, RdbParser

class RedisParserTestCase(unittest.TestCase):
    def setUp(self):
        pass
        
    def tearDown(self):
        pass
    
    def load_rdb(self, file_name) :
        r = MockRedis()
        parser = RdbParser(r)
        parser.parse(os.path.join(os.path.dirname(__file__), 'dumps', file_name))
        return r

    def test_empty_rdb(self):
        r = self.load_rdb('empty_database.rdb')
        self.assert_('start_rdb' in r.methods_called)
        self.assert_('end_rdb' in r.methods_called)

class MockRedis(RdbCallback):
    def __init__(self) :
        self.databases = {}
        self.lengths = {}
        self.expiry = {}
        self.methods_called = []
        self.dbnum = 0

    def currentdb(self) :
        return self.databases[self.dbnum]
    
    def store_expiry(self, key, expiry) :
        self.expiry[self.dbnum][key] = expiry
    
    def store_length(self, key, length) :
        if not self.dbnum in self.lengths :
            self.lengths[self.dbnum] = {}
        self.lengths[self.dbnum][key] = length

    def get_length(self, key) :
        if not key in self.lengths[self.dbnum] :
            raise Exception('Key %s does not have a length' % key)
        return self.lengths[self.dbnum][key]
        
    def start_rdb(self):
        self.methods_called.append('start_rdb')
    
    def start_database(self, dbnum):
        self.dbnum = dbnum
        self.databases[dbnum] = {}
        self.expiry[dbnum] = {}
        self.lengths[dbnum] = {}
    
    def set(self, key, value, expiry):
        self.currentdb()[key] = value
        if expiry :
            store_expiry(key, expiry)
    
    def start_hash(self, key, length, expiry):
        if key in currentdb() :
            raise Exception('start_hash called with key %s that already exists' % key)
        else :
            currentdb()[key] = {}
        if expiry :
            store_expiry(key, expiry)
        store_length(key, length)
    
    def hset(self, key, field, value):
        if not key in currentdb() :
            raise Exception('start_hash not called for key = %s', key)
        currentdb()[key][field] = value
    
    def end_hash(self, key):
        if not key in currentdb() :
            raise Exception('start_hash not called for key = %s', key)
        if len(currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on hash %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))
    
    def start_set(self, key, cardinality, expiry):
        if key in currentdb() :
            raise Exception('start_set called with key %s that already exists' % key)
        else :
            currentdb()[key] = []
        if expiry :
            store_expiry(key, expiry)
        store_length(key, length)

    def sadd(self, key, member):
        if not key in currentdb() :
            raise Exception('start_set not called for key = %s', key)
        currentdb()[key].append(value)
    
    def end_set(self, key):
        if not key in currentdb() :
            raise Exception('start_set not called for key = %s', key)
        if len(currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on set %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))

    def start_list(self, key, length, expiry):
        if key in currentdb() :
            raise Exception('start_list called with key %s that already exists' % key)
        else :
            currentdb()[key] = []
        if expiry :
            store_expiry(key, expiry)
        store_length(key, length)
    
    def rpush(self, key, value) :
        if not key in currentdb() :
            raise Exception('start_list not called for key = %s', key)
        currentdb()[key].append(value)
    
    def end_list(self, key):
        if not key in currentdb() :
            raise Exception('start_set not called for key = %s', key)
        if len(currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on list %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))

    def start_sorted_set(self, key, length, expiry):
        if key in currentdb() :
            raise Exception('start_sorted_set called with key %s that already exists' % key)
        else :
            currentdb()[key] = []
        if expiry :
            store_expiry(key, expiry)
        store_length(key, length)
    
    def zadd(self, key, score, member):
        if not key in currentdb() :
            raise Exception('start_sorted_set not called for key = %s', key)
        currentdb()[key][member] = score
    
    def end_sorted_set(self, key):
        if not key in currentdb() :
            raise Exception('start_set not called for key = %s', key)
        if len(currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on sortedset %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))

    def end_database(self, dbnum):
        if self.dbnum != dbnum :
            raise Exception('start_database called with %d, but end_database called %d instead' % (self.dbnum, dbnum))
    
    def end_rdb(self):
        self.methods_called.append('end_rdb')

