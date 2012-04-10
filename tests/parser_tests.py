import unittest
import os
import math
from rdbtools import RdbCallback, RdbParser

class RedisParserTestCase(unittest.TestCase):
    def setUp(self):
        pass
        
    def tearDown(self):
        pass
    
    def load_rdb(self, file_name, filters=None) :
        r = MockRedis()
        parser = RdbParser(r, filters)
        parser.parse(os.path.join(os.path.dirname(__file__), 'dumps', file_name))
        return r

    def test_empty_rdb(self):
        r = self.load_rdb('empty_database.rdb')
        self.assert_('start_rdb' in r.methods_called)
        self.assert_('end_rdb' in r.methods_called)
        self.assertEquals(len(r.databases), 0, msg = "didn't expect any databases")

    def test_multiple_databases(self):
        r = self.load_rdb('multiple_databases.rdb')
        self.assert_(len(r.databases), 2)
        self.assert_(1 not in r.databases)
        self.assertEquals(r.databases[0]["key_in_zeroth_database"], "zero")
        self.assertEquals(r.databases[2]["key_in_second_database"], "second")
        
    def test_keys_with_expiry(self):
        r = self.load_rdb('keys_with_expiry.rdb')
        expiry = r.expiry[0]['expires_ms_precision']
        self.assertEquals(expiry.year, 2022)
        self.assertEquals(expiry.month, 12)
        self.assertEquals(expiry.day, 25)
        #self.assertEquals(expiry.hour, 10)
        #self.assertEquals(expiry.minute, 11)
        self.assertEquals(expiry.second, 12)
        self.assertEquals(expiry.microsecond, 573000)        
        
    def test_integer_keys(self):
        r = self.load_rdb('integer_keys.rdb')
        self.assertEquals(r.databases[0][125], "Positive 8 bit integer")
        self.assertEquals(r.databases[0][0xABAB], "Positive 16 bit integer")
        self.assertEquals(r.databases[0][0x0AEDD325], "Positive 32 bit integer")
        
    def test_negative_integer_keys(self):
        r = self.load_rdb('integer_keys.rdb')
        self.assertEquals(r.databases[0][-123], "Negative 8 bit integer")
        self.assertEquals(r.databases[0][-0x7325], "Negative 16 bit integer")
        self.assertEquals(r.databases[0][-0x0AEDD325], "Negative 32 bit integer")
    
    def test_string_key_with_compression(self):
        r = self.load_rdb('easily_compressible_string_key.rdb')
        key = "".join('a' for x in range(0, 200))
        value = "Key that redis should compress easily"
        self.assertEquals(r.databases[0][key], value)

    def test_zipmap_thats_compresses_easily(self):
        r = self.load_rdb('zipmap_that_compresses_easily.rdb')
        self.assertEquals(r.databases[0]["zipmap_compresses_easily"]["a"], "aa")
        self.assertEquals(r.databases[0]["zipmap_compresses_easily"]["aa"], "aaaa")
        self.assertEquals(r.databases[0]["zipmap_compresses_easily"]["aaaaa"], "aaaaaaaaaaaaaa")
        
    def test_zipmap_that_doesnt_compress(self):
        r = self.load_rdb('zipmap_that_doesnt_compress.rdb')
        self.assertEquals(r.databases[0]["zimap_doesnt_compress"]["MKD1G6"], 2)
        self.assertEquals(r.databases[0]["zimap_doesnt_compress"]["YNNXK"], "F7TI")
    
    def test_zipmap_with_big_values(self):
        ''' See issue https://github.com/sripathikrishnan/redis-rdb-tools/issues/2
            Values with length around 253/254/255 bytes are treated specially in the parser
            This test exercises those boundary conditions
        '''
        r = self.load_rdb('zipmap_with_big_values.rdb')
        self.assertEquals(len(r.databases[0]["zipmap_with_big_values"]["253bytes"]), 253)
        self.assertEquals(len(r.databases[0]["zipmap_with_big_values"]["254bytes"]), 254)
        self.assertEquals(len(r.databases[0]["zipmap_with_big_values"]["255bytes"]), 255)
        self.assertEquals(len(r.databases[0]["zipmap_with_big_values"]["300bytes"]), 300)
        
    def test_hash_as_ziplist(self):
        '''In redis dump version = 4, hashmaps are stored as ziplists'''
        r = self.load_rdb('hash_as_ziplist.rdb')
        self.assertEquals(r.databases[0]["zipmap_compresses_easily"]["a"], "aa")
        self.assertEquals(r.databases[0]["zipmap_compresses_easily"]["aa"], "aaaa")
        self.assertEquals(r.databases[0]["zipmap_compresses_easily"]["aaaaa"], "aaaaaaaaaaaaaa")
        
    def test_dictionary(self):
        r = self.load_rdb('dictionary.rdb')
        self.assertEquals(r.lengths[0]["force_dictionary"], 1000)
        self.assertEquals(r.databases[0]["force_dictionary"]["ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ"], 
                    "T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI")
        self.assertEquals(r.databases[0]["force_dictionary"]["UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491"], 
                    "6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ")
    
    def test_ziplist_that_compresses_easily(self):
        r = self.load_rdb('ziplist_that_compresses_easily.rdb')
        self.assertEquals(r.lengths[0]["ziplist_compresses_easily"], 6)
        for idx, length in enumerate([6, 12, 18, 24, 30, 36]) :
            self.assertEquals(("".join("a" for x in xrange(length))), r.databases[0]["ziplist_compresses_easily"][idx])
    
    def test_ziplist_that_doesnt_compress(self):
        r = self.load_rdb('ziplist_that_doesnt_compress.rdb')
        self.assertEquals(r.lengths[0]["ziplist_doesnt_compress"], 2)
        self.assert_("aj2410" in r.databases[0]["ziplist_doesnt_compress"])
        self.assert_("cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344" 
                        in r.databases[0]["ziplist_doesnt_compress"])
    
    def test_ziplist_with_integers(self):
        r = self.load_rdb('ziplist_with_integers.rdb')
        self.assertEquals(r.lengths[0]["ziplist_with_integers"], 4)
        for num in (63, 16380, 65535, 0x7fffffffffffffff) :
            self.assert_(num in r.databases[0]["ziplist_with_integers"])

    def test_linkedlist(self):
        r = self.load_rdb('linkedlist.rdb')
        self.assertEquals(r.lengths[0]["force_linkedlist"], 1000)
        self.assert_("JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ" in r.databases[0]["force_linkedlist"])
        self.assert_("TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S" in r.databases[0]["force_linkedlist"])

    def test_intset_16(self):
        r = self.load_rdb('intset_16.rdb')
        self.assertEquals(r.lengths[0]["intset_16"], 3)
        for num in (0x7ffe, 0x7ffd, 0x7ffc) :
            self.assert_(num in r.databases[0]["intset_16"])

    def test_intset_32(self):
        r = self.load_rdb('intset_32.rdb')
        self.assertEquals(r.lengths[0]["intset_32"], 3)
        for num in (0x7ffefffe, 0x7ffefffd, 0x7ffefffc) :
            self.assert_(num in r.databases[0]["intset_32"])

    def test_intset_64(self):
        r = self.load_rdb('intset_64.rdb')
        self.assertEquals(r.lengths[0]["intset_64"], 3)
        for num in (0x7ffefffefffefffe, 0x7ffefffefffefffd, 0x7ffefffefffefffc) :
            self.assert_(num in r.databases[0]["intset_64"])

    def test_regular_set(self):
        r = self.load_rdb('regular_set.rdb')
        self.assertEquals(r.lengths[0]["regular_set"], 6)
        for member in ("alpha", "beta", "gamma", "delta", "phi", "kappa") :
            self.assert_(member in r.databases[0]["regular_set"], msg=('%s missing' % member))

    def test_sorted_set_as_ziplist(self):
        r = self.load_rdb('sorted_set_as_ziplist.rdb')
        self.assertEquals(r.lengths[0]["sorted_set_as_ziplist"], 3)
        zset = r.databases[0]["sorted_set_as_ziplist"]
        self.assert_(floateq(zset['8b6ba6718a786daefa69438148361901'], 1))
        self.assert_(floateq(zset['cb7a24bb7528f934b841b34c3a73e0c7'], 2.37))
        self.assert_(floateq(zset['523af537946b79c4f8369ed39ba78605'], 3.423))

    def test_filtering_by_keys(self):
        r = self.load_rdb('parser_filters.rdb', filters={"keys":"k[0-9]"})
        self.assertEquals(r.databases[0]['k1'], "ssssssss")
        self.assertEquals(r.databases[0]['k3'], "wwwwwwww")
        self.assertEquals(len(r.databases[0]), 2)

    def test_filtering_by_type(self):
        r = self.load_rdb('parser_filters.rdb', filters={"types":["sortedset"]})
        self.assert_('z1' in r.databases[0])
        self.assert_('z2' in r.databases[0])
        self.assert_('z3' in r.databases[0])
        self.assert_('z4' in r.databases[0])
        self.assertEquals(len(r.databases[0]), 4)

    def test_filtering_by_database(self):
        r = self.load_rdb('multiple_databases.rdb', filters={"dbs":[2]})
        self.assert_('key_in_zeroth_database' not in r.databases[0])
        self.assert_('key_in_second_database' in r.databases[2])
        self.assertEquals(len(r.databases[0]), 0)
        self.assertEquals(len(r.databases[2]), 1)

    def test_rdb_version_5_with_checksum(self):
        r = self.load_rdb('rdb_version_5_with_checksum.rdb')
        self.assertEquals(r.databases[0]['abcd'], 'efgh')
        self.assertEquals(r.databases[0]['foo'], 'bar')
        self.assertEquals(r.databases[0]['bar'], 'baz')
        self.assertEquals(r.databases[0]['abcdef'], 'abcdef')
        self.assertEquals(r.databases[0]['longerstring'], 'thisisalongerstring.idontknowwhatitmeans')

def floateq(f1, f2) :
    return math.fabs(f1 - f2) < 0.00001
    
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
    
    def set(self, key, value, expiry, info):
        self.currentdb()[key] = value
        if expiry :
            self.store_expiry(key, expiry)
    
    def start_hash(self, key, length, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_hash called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = {}
        if expiry :
            self.store_expiry(key, expiry)
        self.store_length(key, length)
    
    def hset(self, key, field, value):
        if not key in self.currentdb() :
            raise Exception('start_hash not called for key = %s', key)
        self.currentdb()[key][field] = value
    
    def end_hash(self, key):
        if not key in self.currentdb() :
            raise Exception('start_hash not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on hash %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))
    
    def start_set(self, key, cardinality, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_set called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = []
        if expiry :
            self.store_expiry(key, expiry)
        self.store_length(key, cardinality)

    def sadd(self, key, member):
        if not key in self.currentdb() :
            raise Exception('start_set not called for key = %s', key)
        self.currentdb()[key].append(member)
    
    def end_set(self, key):
        if not key in self.currentdb() :
            raise Exception('start_set not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on set %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))

    def start_list(self, key, length, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_list called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = []
        if expiry :
            self.store_expiry(key, expiry)
        self.store_length(key, length)
    
    def rpush(self, key, value) :
        if not key in self.currentdb() :
            raise Exception('start_list not called for key = %s', key)
        self.currentdb()[key].append(value)
    
    def end_list(self, key):
        if not key in self.currentdb() :
            raise Exception('start_set not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on list %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))

    def start_sorted_set(self, key, length, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_sorted_set called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = {}
        if expiry :
            self.store_expiry(key, expiry)
        self.store_length(key, length)
    
    def zadd(self, key, score, member):
        if not key in self.currentdb() :
            raise Exception('start_sorted_set not called for key = %s', key)
        self.currentdb()[key][member] = score
    
    def end_sorted_set(self, key):
        if not key in self.currentdb() :
            raise Exception('start_set not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key] :
            raise Exception('Lengths mismatch on sortedset %s, expected length = %d, actual = %d'
                                 % (key, self.lengths[self.dbnum][key], len(currentdb()[key])))

    def end_database(self, dbnum):
        if self.dbnum != dbnum :
            raise Exception('start_database called with %d, but end_database called %d instead' % (self.dbnum, dbnum))
    
    def end_rdb(self):
        self.methods_called.append('end_rdb')


