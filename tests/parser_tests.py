import unittest
import os
import math
from rdbtools import RdbCallback, RdbParser
from rdbtools.compat import range

class RedisParserTestCase(unittest.TestCase):
    def setUp(self):
        pass
        
    def tearDown(self):
        pass

    def test_empty_rdb(self):
        r = load_rdb('empty_database.rdb')
        self.assert_('start_rdb' in r.methods_called)
        self.assert_('end_rdb' in r.methods_called)
        self.assertEquals(len(r.databases), 0, msg = "didn't expect any databases")

    def test_multiple_databases(self):
        r = load_rdb('multiple_databases.rdb')
        self.assert_(len(r.databases), 2)
        self.assert_(1 not in r.databases)
        self.assertEquals(r.databases[0][b"key_in_zeroth_database"], b"zero")
        self.assertEquals(r.databases[2][b"key_in_second_database"], b"second")

    def test_keys_with_expiry(self):
        r = load_rdb('keys_with_expiry.rdb')
        expiry = r.expiry[0][b'expires_ms_precision']
        self.assertEquals(expiry.year, 2022)
        self.assertEquals(expiry.month, 12)
        self.assertEquals(expiry.day, 25)
        self.assertEquals(expiry.hour, 10)
        self.assertEquals(expiry.minute, 11)
        self.assertEquals(expiry.second, 12)
        self.assertEquals(expiry.microsecond, 573000)
        
    def test_integer_keys(self):
        r = load_rdb('integer_keys.rdb')
        self.assertEquals(r.databases[0][125], b"Positive 8 bit integer")
        self.assertEquals(r.databases[0][0xABAB], b"Positive 16 bit integer")
        self.assertEquals(r.databases[0][0x0AEDD325], b"Positive 32 bit integer")

    def test_negative_integer_keys(self):
        r = load_rdb('integer_keys.rdb')
        self.assertEquals(r.databases[0][-123], b"Negative 8 bit integer")
        self.assertEquals(r.databases[0][-0x7325], b"Negative 16 bit integer")
        self.assertEquals(r.databases[0][-0x0AEDD325], b"Negative 32 bit integer")

    def test_string_key_with_compression(self):
        r = load_rdb('easily_compressible_string_key.rdb')
        key = b"".join(b'a' for x in range(0, 200))
        value = b"Key that redis should compress easily"
        self.assertEquals(r.databases[0][key], value)

    def test_zipmap_that_compresses_easily(self):
        r = load_rdb('zipmap_that_compresses_easily.rdb')
        self.assertEquals(r.databases[0][b"zipmap_compresses_easily"][b"a"], b"aa")
        self.assertEquals(r.databases[0][b"zipmap_compresses_easily"][b"aa"], b"aaaa")
        self.assertEquals(r.databases[0][b"zipmap_compresses_easily"][b"aaaaa"], b"aaaaaaaaaaaaaa")

    def test_zipmap_that_doesnt_compress(self):
        r = load_rdb('zipmap_that_doesnt_compress.rdb')
        self.assertEquals(r.databases[0][b"zimap_doesnt_compress"][b"MKD1G6"], 2)
        self.assertEquals(r.databases[0][b"zimap_doesnt_compress"][b"YNNXK"], b"F7TI")

    def test_zipmap_with_big_values(self):
        ''' See issue https://github.com/sripathikrishnan/redis-rdb-tools/issues/2
            Values with length around 253/254/255 bytes are treated specially in the parser
            This test exercises those boundary conditions

            In order to test a bug with large ziplists, it is necessary to start
            Redis with "hash-max-ziplist-value 21000", create this rdb file,
            and run the test. That forces the 20kbyte value to be stored as a
            ziplist with a length encoding of 5 bytes.
        '''
        r = load_rdb('zipmap_with_big_values.rdb')
        self.assertEquals(len(r.databases[0][b"zipmap_with_big_values"][b"253bytes"]), 253)
        self.assertEquals(len(r.databases[0][b"zipmap_with_big_values"][b"254bytes"]), 254)
        self.assertEquals(len(r.databases[0][b"zipmap_with_big_values"][b"255bytes"]), 255)
        self.assertEquals(len(r.databases[0][b"zipmap_with_big_values"][b"300bytes"]), 300)
        self.assertEquals(len(r.databases[0][b"zipmap_with_big_values"][b"20kbytes"]), 20000)

    def test_hash_as_ziplist(self):
        '''In redis dump version = 4, hashmaps are stored as ziplists'''
        r = load_rdb('hash_as_ziplist.rdb')
        self.assertEquals(r.databases[0][b"zipmap_compresses_easily"][b"a"], b"aa")
        self.assertEquals(r.databases[0][b"zipmap_compresses_easily"][b"aa"], b"aaaa")
        self.assertEquals(r.databases[0][b"zipmap_compresses_easily"][b"aaaaa"], b"aaaaaaaaaaaaaa")

    def test_dictionary(self):
        r = load_rdb('dictionary.rdb')
        self.assertEquals(r.lengths[0][b"force_dictionary"], 1000)
        self.assertEquals(r.databases[0][b"force_dictionary"][b"ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ"],
                    b"T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI")
        self.assertEquals(r.databases[0][b"force_dictionary"][b"UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491"],
                    b"6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ")

    def test_ziplist_that_compresses_easily(self):
        r = load_rdb('ziplist_that_compresses_easily.rdb')
        self.assertEquals(r.lengths[0][b"ziplist_compresses_easily"], 6)
        for idx, length in enumerate([6, 12, 18, 24, 30, 36]) :
            self.assertEquals((b"".join(b"a" for x in range(length))), r.databases[0][b"ziplist_compresses_easily"][idx])

    def test_ziplist_that_doesnt_compress(self):
        r = load_rdb('ziplist_that_doesnt_compress.rdb')
        self.assertEquals(r.lengths[0][b"ziplist_doesnt_compress"], 2)
        self.assert_(b"aj2410" in r.databases[0][b"ziplist_doesnt_compress"])
        self.assert_(b"cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344"
                        in r.databases[0][b"ziplist_doesnt_compress"])

    def test_ziplist_with_integers(self):
        r = load_rdb('ziplist_with_integers.rdb')
        
        expected_numbers = []
        for x in range(0,13):
            expected_numbers.append(x)
        
        expected_numbers += [-2, 13, 25, -61, 63, 16380, -16000, 65535, -65523, 4194304, 0x7fffffffffffffff]
        
        self.assertEquals(r.lengths[0][b"ziplist_with_integers"], len(expected_numbers))
        
        for num in expected_numbers :
            self.assert_(num in r.databases[0][b"ziplist_with_integers"], "Cannot find %d" % num)

    def test_linkedlist(self):
        r = load_rdb('linkedlist.rdb')
        self.assertEquals(r.lengths[0][b"force_linkedlist"], 1000)
        self.assert_(b"JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ" in r.databases[0][b"force_linkedlist"])
        self.assert_(b"TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S" in r.databases[0][b"force_linkedlist"])

    def test_intset_16(self):
        r = load_rdb('intset_16.rdb')
        self.assertEquals(r.lengths[0][b"intset_16"], 3)
        for num in (0x7ffe, 0x7ffd, 0x7ffc) :
            self.assert_(num in r.databases[0][b"intset_16"])

    def test_intset_32(self):
        r = load_rdb('intset_32.rdb')
        self.assertEquals(r.lengths[0][b"intset_32"], 3)
        for num in (0x7ffefffe, 0x7ffefffd, 0x7ffefffc) :
            self.assert_(num in r.databases[0][b"intset_32"])

    def test_intset_64(self):
        r = load_rdb('intset_64.rdb')
        self.assertEquals(r.lengths[0][b"intset_64"], 3)
        for num in (0x7ffefffefffefffe, 0x7ffefffefffefffd, 0x7ffefffefffefffc) :
            self.assert_(num in r.databases[0][b"intset_64"])

    def test_regular_set(self):
        r = load_rdb('regular_set.rdb')
        self.assertEquals(r.lengths[0][b"regular_set"], 6)
        for member in (b"alpha", b"beta", b"gamma", b"delta", b"phi", b"kappa") :
            self.assert_(member in r.databases[0][b"regular_set"], msg=('%s missing' % member))

    def test_sorted_set_as_ziplist(self):
        r = load_rdb('sorted_set_as_ziplist.rdb')
        self.assertEquals(r.lengths[0][b"sorted_set_as_ziplist"], 3)
        zset = r.databases[0][b"sorted_set_as_ziplist"]
        self.assert_(floateq(zset[b'8b6ba6718a786daefa69438148361901'], 1))
        self.assert_(floateq(zset[b'cb7a24bb7528f934b841b34c3a73e0c7'], 2.37))
        self.assert_(floateq(zset[b'523af537946b79c4f8369ed39ba78605'], 3.423))

    def test_listpack_small_numbers(self):
        r = load_rdb('listpack_small_numbers.rdb')
        self.assertEquals(r.lengths[0][b"listpack_small_numbers"], 7)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_1_bit"], 1)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_2_bits"], 3)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_3_bits"], 6)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_4_bits"], 13)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_5_bits"], 26)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_6_bits"], 53)
        self.assertEquals(r.databases[0][b"listpack_small_numbers"][b"unsigned_integer_7_bits"], 107)

    def test_listpack_tiny_strings(self):
        r = load_rdb('listpack_tiny_strings.rdb')
        self.assertEquals(r.lengths[0][b"listpack_tiny_strings"], 4)
        self.assertEquals(len(r.databases[0][b"listpack_tiny_strings"][b"char_0"]), 0)
        self.assertEquals(len(r.databases[0][b"listpack_tiny_strings"][b"char_1"]), 1)
        self.assertEquals(len(r.databases[0][b"listpack_tiny_strings"][b"char_50"]), 50)
        self.assertEquals(len(r.databases[0][b"listpack_tiny_strings"][b"char_63"]), 63)

    def test_listpack_multibyte_encodings_13_bit_signed_integer(self):
        r = load_rdb('listpack_multibyte_encodings_13_bit_signed_integer.rdb')
        self.assertEquals(r.lengths[0][b"listpack_multibyte_encodings_13_bit_signed_integer"], 4)
        expected_numbers = [-1, -4097, 8191, 4097]
        for num in expected_numbers :
            self.assert_(num in r.databases[0][b"listpack_multibyte_encodings_13_bit_signed_integer"], "Cannot find %d" % num)

    def test_listpack_multibyte_encodings_small_strings(self):
        r = load_rdb('listpack_multibyte_encodings_small_strings.rdb')
        self.assertEquals(r.lengths[0][b"listpack_multibyte_encodings_small_strings"], 3)
        self.assertEquals(len(r.databases[0][b"listpack_multibyte_encodings_small_strings"][b"char_64"]), 64)
        self.assertEquals(len(r.databases[0][b"listpack_multibyte_encodings_small_strings"][b"char_500"]), 500)
        self.assertEquals(len(r.databases[0][b"listpack_multibyte_encodings_small_strings"][b"char_4095"]), 4095)

    def test_listpack_multibyte_encodings_large_strings(self):
        r = load_rdb('listpack_multibyte_encodings_large_strings.rdb')
        self.assertEquals(r.lengths[0][b"listpack_multibyte_encodings_large_strings"], 3)
        self.assertEquals(len(r.databases[0][b"listpack_multibyte_encodings_large_strings"][b"255bytes"]), 255)
        self.assertEquals(len(r.databases[0][b"listpack_multibyte_encodings_large_strings"][b"65535bytes"]), 65535)
        self.assertEquals(len(r.databases[0][b"listpack_multibyte_encodings_large_strings"][b"16777215bytes"]), 16777215)

    def test_listpack_multibyte_encodings_integer(self):
        r = load_rdb('listpack_multibyte_encodings_integer.rdb')
        self.assertEquals(r.lengths[0][b"listpack_multibyte_encodings_integer"], 8)
        expected_numbers = [-0x7ffe, 0x7ffe, -0x7ffeff, 0x7ffeff, -0x7ffefffe, 0x7ffefffe, -0x7ffefffefffefffe, 0x7ffefffefffefffe]
        for num in expected_numbers :
            self.assert_(num in r.databases[0][b"listpack_multibyte_encodings_integer"], "Cannot find %d" % num)

    def test_set_as_listpack(self):
        r = load_rdb('set_as_listpack.rdb')
        self.assertEquals(r.lengths[0][b"set_as_listpack"], 6)
        expected_numbers = [-3, 50, -70]
        for member in (b"abc", b"abcdefg", b"abcdefghijklmn") :
            self.assert_(member in r.databases[0][b"set_as_listpack"], msg=('%s missing' % member))
        for num in expected_numbers :
            self.assert_(num in r.databases[0][b"set_as_listpack"], "Cannot find %d" % num)

    def test_streams(self):
        r = load_rdb('streams.rdb')
        self.assertEquals(r.lengths[0][b"streams"], 3)

    def test_filtering_by_keys(self):
        r = load_rdb('parser_filters.rdb', filters={"keys":"k[0-9]"})
        self.assertEquals(r.databases[0][b'k1'], b"ssssssss")
        self.assertEquals(r.databases[0][b'k3'], b"wwwwwwww")
        self.assertEquals(len(r.databases[0]), 2)

    def test_filtering_by_type(self):
        r = load_rdb('parser_filters.rdb', filters={"types":["sortedset"]})
        self.assert_(b'z1' in r.databases[0])
        self.assert_(b'z2' in r.databases[0])
        self.assert_(b'z3' in r.databases[0])
        self.assert_(b'z4' in r.databases[0])
        self.assertEquals(len(r.databases[0]), 4)

    def test_filtering_by_database(self):
        r = load_rdb('multiple_databases.rdb', filters={"dbs":[2]})
        self.assert_(b'key_in_zeroth_database' not in r.databases[0])
        self.assert_(b'key_in_second_database' in r.databases[2])
        self.assertEquals(len(r.databases[0]), 0)
        self.assertEquals(len(r.databases[2]), 1)

    def test_rdb_version_5_with_checksum(self):
        r = load_rdb('rdb_version_5_with_checksum.rdb')
        self.assertEquals(r.databases[0][b'abcd'], b'efgh')
        self.assertEquals(r.databases[0][b'foo'], b'bar')
        self.assertEquals(r.databases[0][b'bar'], b'baz')
        self.assertEquals(r.databases[0][b'abcdef'], b'abcdef')
        self.assertEquals(r.databases[0][b'longerstring'], b'thisisalongerstring.idontknowwhatitmeans')

    def test_rdb_version_8_with_64b_length_and_scores(self):
        r = load_rdb('rdb_version_8_with_64b_length_and_scores.rdb')
        self.assertEquals(r.databases[0][b'foo'], b'bar')
        zset = r.databases[0][b"bigset"]
        self.assertEquals(len(zset), 1000)
        self.assert_(floateq(zset[b'finalfield'], 2.718))

    def test_multiple_databases_stream(self):
        r = load_rdb_stream('multiple_databases.rdb')
        self.assert_(len(r.databases), 2)
        self.assert_(1 not in r.databases)
        self.assertEquals(r.databases[0][b"key_in_zeroth_database"], b"zero")
        self.assertEquals(r.databases[2][b"key_in_second_database"], b"second")

    def test_rdb_version_8_with_module(self):
        r = load_rdb('redis_40_with_module.rdb')
        self.assertEquals(r.databases[0][b'foo']['module_name'], 'ReJSON-RL')

    def test_rdb_version_8_with_module_and_skip(self):
        r = load_rdb('redis_40_with_module.rdb', {"keys": "bar"}) # skip foo module
        self.assert_(b'foo' not in r.databases[0])

    def test_rdb_version_9_with_stream(self):
        r = load_rdb('redis_50_with_streams.rdb')
        self.assertEquals(r.lengths[0][b"mystream"], 4)
        self.assertEquals(len(r.databases[0][b'mystream']), 1)


def floateq(f1, f2) :
    return math.fabs(f1 - f2) < 0.00001

def load_rdb(file_name, filters=None) :
    r = MockRedis()
    parser = RdbParser(r, filters)
    parser.parse(os.path.join(os.path.dirname(__file__), 'dumps', file_name))
    return r

def load_rdb_stream(file_name, filters=None) :
    r = MockRedis()
    parser = RdbParser(r, filters)
    parser.parse_fd(open(os.path.join(os.path.dirname(__file__), 'dumps', file_name), 'rb'))
    return r
    
class MockRedis(RdbCallback):
    def __init__(self):
        super(MockRedis, self).__init__(string_escape=None)
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
                                 % (key, self.lengths[self.dbnum][key], len(self.currentdb()[key])))
    
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
                                 % (key, self.lengths[self.dbnum][key], len(self.currentdb()[key])))

    def start_list(self, key, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_list called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = []
        if expiry :
            self.store_expiry(key, expiry)

    def rpush(self, key, value) :
        if not key in self.currentdb() :
            raise Exception('start_list not called for key = %s', key)
        self.currentdb()[key].append(value)

    def end_list(self, key, info):
        if not key in self.currentdb() :
            raise Exception('start_set not called for key = %s', key)
        self.store_length(key, len(self.currentdb()[key]))

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
                                 % (key, self.lengths[self.dbnum][key], len(self.currentdb()[key])))

    def start_module(self, key, module_name, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_module called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = {'module_name': module_name}
        if expiry :
            self.store_expiry(key, expiry)
        return False

    def end_module(self, key, buffer_size, buffer=None):
        if not key in self.currentdb() :
            raise Exception('start_module not called for key = %s', key)
        self.store_length(key, buffer_size)
        pass

    def start_stream(self, key, listpacks_count, expiry, info):
        if key in self.currentdb() :
            raise Exception('start_stream called with key %s that already exists' % key)
        else :
            self.currentdb()[key] = {}
        if expiry :
            self.store_expiry(key, expiry)
        pass

    def stream_listpack(self, key, entry_id, data):
        if not key in self.currentdb() :
            raise Exception('start_hash not called for key = %s', key)
        self.currentdb()[key][entry_id] = data
        pass

    def end_stream(self, key, items, last_entry_id, cgroups):
        if not key in self.currentdb() :
            raise Exception('start_stream not called for key = %s', key)
        self.store_length(key, items)

    def end_database(self, dbnum):
        if self.dbnum != dbnum :
            raise Exception('start_database called with %d, but end_database called %d instead' % (self.dbnum, dbnum))
    
    def end_rdb(self):
        self.methods_called.append('end_rdb')


