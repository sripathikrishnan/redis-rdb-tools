import sys
import codecs
from collections import namedtuple
import random
import bisect
from distutils.version import StrictVersion
try:
    import ujson as json
except:
    import json

from rdbtools.parser import RdbCallback
from rdbtools.encodehelpers import bytes_to_unicode

from heapq import heappush, nlargest, heappop

ZSKIPLIST_MAXLEVEL=32
ZSKIPLIST_P=0.25
REDIS_SHARED_INTEGERS = 10000

MemoryRecord = namedtuple('MemoryRecord', ['database', 'type', 'key', 'bytes', 'encoding','size', 'len_largest_element', 'expiry'])

class StatsAggregator(object):
    def __init__(self, key_groupings = None):
        self.aggregates = {}
        self.scatters = {}
        self.histograms = {}
        self.metadata = {}

    def next_record(self, record):
        self.add_aggregate('database_memory', record.database, record.bytes)
        self.add_aggregate('database_memory', 'all', record.bytes)
        self.add_aggregate('type_memory', record.type, record.bytes)
        self.add_aggregate('encoding_memory', record.encoding, record.bytes)
        
        self.add_aggregate('type_count', record.type, 1)
        self.add_aggregate('encoding_count', record.encoding, 1)
    
        self.add_histogram(record.type + "_length", record.size)
        self.add_histogram(record.type + "_memory", (record.bytes/10) * 10)
        
        if record.type == 'list':
            self.add_scatter('list_memory_by_length', record.bytes, record.size)
        elif record.type == 'hash':
            self.add_scatter('hash_memory_by_length', record.bytes, record.size)
        elif record.type == 'set':
            self.add_scatter('set_memory_by_length', record.bytes, record.size)
        elif record.type == 'sortedset':
            self.add_scatter('sortedset_memory_by_length', record.bytes, record.size)
        elif record.type == 'string':
            self.add_scatter('string_memory_by_length', record.bytes, record.size)
        elif record.type in ['dict', 'module', 'stream']:
            pass
        else:
            raise Exception('Invalid data type %s' % record.type)

    def add_aggregate(self, heading, subheading, metric):
        if not heading in self.aggregates :
            self.aggregates[heading] = {}
        
        if not subheading in self.aggregates[heading]:
            self.aggregates[heading][subheading] = 0
            
        self.aggregates[heading][subheading] += metric
    
    def add_histogram(self, heading, metric):
        if not heading in self.histograms:
            self.histograms[heading] = {}

        if not metric in self.histograms[heading]:
            self.histograms[heading][metric] = 1
        else :
            self.histograms[heading][metric] += 1
    
    def add_scatter(self, heading, x, y):
        if not heading in self.scatters:
            self.scatters[heading] = []
        self.scatters[heading].append([x, y])

    def set_metadata(self, key, val):
        self.metadata[key] = val
  
    def get_json(self):
        return json.dumps({"aggregates": self.aggregates, "scatters": self.scatters, "histograms": self.histograms, "metadata": self.metadata})
        
class PrintAllKeys(object):
    def __init__(self, out, bytes, largest):
        self._bytes = bytes
        self._largest = largest
        self._out = out
        headers = "%s,%s,%s,%s,%s,%s,%s,%s\n" % (
            "database", "type", "key", "size_in_bytes", "encoding", "num_elements", "len_largest_element", "expiry")
        self._out.write(codecs.encode(headers, 'latin-1'))

        if self._largest is not None:
            self._heap = []
    
    def next_record(self, record) :
        if record.key is None:
            return  # some records are not keys (e.g. dict)
        if self._largest is None:
            if self._bytes is None or record.bytes >= int(self._bytes):
                rec_str = "%d,%s,%s,%d,%s,%d,%d,%s\n" % (
                    record.database, record.type, record.key, record.bytes, record.encoding, record.size,
                    record.len_largest_element,
                    record.expiry.isoformat() if record.expiry else '')
                self._out.write(codecs.encode(rec_str, 'latin-1'))
        else:
            heappush(self._heap, (record.bytes, record))

    def end_rdb(self):
        if self._largest is not None:
            self._heap = nlargest(int(self._largest), self._heap)
            self._largest = None

            while self._heap:
                bytes, record = heappop(self._heap)
                self.next_record(record)

class PrintJustKeys(object):
    def __init__(self, out):
        self._out = out
    
    def next_record(self, record):
        self._out.write(codecs.encode("%s\n" % record.key, 'latin-1'))


class MemoryCallback(RdbCallback):
    '''Calculates the memory used if this rdb file were loaded into RAM
        The memory usage is approximate, and based on heuristics.
    '''
    def __init__(self, stream, architecture, redis_version='5.0', string_escape=None):
        super(MemoryCallback, self).__init__(string_escape)
        self._stream = stream
        self._dbnum = 0
        self._current_size = 0
        self._current_encoding = None
        self._current_length = 0
        self._len_largest_element = 0
        self._key_expiry = None
        self._db_keys = 0
        self._db_expires = 0
        self._aux_used_mem = None
        self._aux_redis_ver = None
        self._aux_redis_bits = None
        self._redis_version = StrictVersion(redis_version)
        self._total_internal_frag = 0
        if architecture == 64 or architecture == '64':
            self._pointer_size = 8
            self._long_size = 8
            self._architecture = 64
        elif architecture == 32 or architecture == '32':
            self._pointer_size = 4
            self._long_size = 4
            self._architecture = 32

    def emit_record(self, record_type, key, byte_count, encoding, size, largest_el, expiry):
        if key is not None:
            key = bytes_to_unicode(key, self._escape, skip_printable=True)
        record = MemoryRecord(self._dbnum, record_type, key, byte_count, encoding, size, largest_el, expiry)
        self._stream.next_record(record)

    def start_rdb(self):
        pass

    def aux_field(self, key, value):
        if key == 'used-mem':
            self._aux_used_mem = int(value)
        if key == 'redis-ver':
            self._aux_redis_ver = value
        if key == 'redis-bits':
            self._aux_redis_bits = int(value)

    def start_database(self, db_number):
        self._dbnum = db_number
        self._db_keys = 0
        self._db_expires = 0

    def end_database(self, db_number):
        self.emit_record("dict", None, self.hashtable_overhead(self._db_keys), None, None, None, None)
        self.emit_record("dict", None, self.hashtable_overhead(self._db_expires), None, None, None, None)
        if hasattr(self._stream, 'end_database'):
            self._stream.end_database(db_number)

    def end_rdb(self):
        if hasattr(self._stream, 'end_rdb'):
            self._stream.end_rdb()
        if hasattr(self._stream, 'set_metadata'):
            self._stream.set_metadata('used_mem', self._aux_used_mem)
            self._stream.set_metadata('redis_ver', self._aux_redis_ver)
            self._stream.set_metadata('redis_bits', self._aux_redis_bits)
            self._stream.set_metadata('internal_frag', self._total_internal_frag)

    def set(self, key, value, expiry, info):
        self._current_encoding = info['encoding']
        size = self.top_level_object_overhead(key, expiry) + self.sizeof_string(value)
        length = self.element_length(value)
        self.emit_record("string", key, size, self._current_encoding, length, length, expiry)
        self.end_key()
    
    def start_hash(self, key, length, expiry, info):
        self._current_encoding = info['encoding']
        self._current_length = length
        self._key_expiry = expiry
        size = self.top_level_object_overhead(key, expiry)
        
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'hashtable':
            size += self.hashtable_overhead(length)
        else:
            raise Exception('start_hash', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
    
    def hset(self, key, field, value):
        if(self.element_length(field) > self._len_largest_element) :
            self._len_largest_element = self.element_length(field)
        if(self.element_length(value) > self._len_largest_element) :
            self._len_largest_element = self.element_length(value)
        
        if self._current_encoding == 'hashtable':
            self._current_size += self.sizeof_string(field)
            self._current_size += self.sizeof_string(value)
            self._current_size += self.hashtable_entry_overhead()
            if self._redis_version < StrictVersion('4.0'):
                self._current_size += 2*self.robj_overhead()
    
    def end_hash(self, key):
        self.emit_record("hash", key, self._current_size, self._current_encoding, self._current_length,
                         self._len_largest_element, self._key_expiry)
        self.end_key()
    
    def start_set(self, key, cardinality, expiry, info):
        # A set is exactly like a hashmap
        self.start_hash(key, cardinality, expiry, info)

    def sadd(self, key, member):
        if(self.element_length(member) > self._len_largest_element) :
            self._len_largest_element = self.element_length(member)
            
        if self._current_encoding == 'hashtable':
            self._current_size += self.sizeof_string(member)
            self._current_size += self.hashtable_entry_overhead()
            if self._redis_version < StrictVersion('4.0'):
                self._current_size += self.robj_overhead()
    
    def end_set(self, key):
        self.emit_record("set", key, self._current_size, self._current_encoding, self._current_length,
                         self._len_largest_element, self._key_expiry)
        self.end_key()
    
    def start_list(self, key, expiry, info):
        self._current_length = 0
        self._list_items_size = 0  # size of all elements in case list ends up using linked list
        self._list_items_zipped_size = 0  # size of all elements in case of ziplist of quicklist
        self._current_encoding = info['encoding']
        size = self.top_level_object_overhead(key, expiry)
        self._key_expiry = expiry

        # ignore the encoding in the rdb, and predict the encoding that will be used at the target redis version
        if self._redis_version >= StrictVersion('3.2'):
            # default configuration of redis 3.2
            self._current_encoding = "quicklist"
            self._list_max_ziplist_size = 8192 # default is -2 which means 8k
            self._list_compress_depth = 0  # currently we only support no compression which is the default
            self._cur_zips = 1
            self._cur_zip_size = 0
        else:
            # default configuration fo redis 2.8 -> 3.0
            self._current_encoding = "ziplist"
            self._list_max_ziplist_entries = 512
            self._list_max_ziplist_value = 64

        self._current_size = size
            
    def rpush(self, key, value):
        self._current_length += 1
        # in linked list, when the robj has integer encoding, the value consumes no memory on top of the robj
        size_in_list = self.sizeof_string(value) if not self.is_integer_type(value) else 0
        # in ziplist and quicklist, this is the size of the value and the value header
        size_in_zip = self.ziplist_entry_overhead(value)

        if(self.element_length(value) > self._len_largest_element):
            self._len_largest_element = self.element_length(value)

        if self._current_encoding == "ziplist":
            self._list_items_zipped_size += size_in_zip
            if self._current_length > self._list_max_ziplist_entries or size_in_zip > self._list_max_ziplist_value:
                self._current_encoding = "linkedlist"
        elif self._current_encoding == "quicklist":
            if self._cur_zip_size + size_in_zip > self._list_max_ziplist_size:
                self._cur_zip_size = size_in_zip
                self._cur_zips += 1
            else:
                self._cur_zip_size += size_in_zip
            self._list_items_zipped_size += self.ziplist_entry_overhead(value)
        self._list_items_size += size_in_list  # not to be used in case of ziplist or quicklist

    def end_list(self, key, info):
        if self._current_encoding == 'quicklist':
            self._current_size += self.quicklist_overhead(self._cur_zips)
            self._current_size += self.ziplist_header_overhead() * self._cur_zips
            self._current_size += self._list_items_zipped_size
        elif self._current_encoding == 'ziplist':
            self._current_size += self.ziplist_header_overhead()
            self._current_size += self._list_items_zipped_size
        else: #  linkedlist
            self._current_size += self.linkedlist_entry_overhead() * self._current_length
            self._current_size += self.linkedlist_overhead()
            if self._redis_version < StrictVersion('4.0'):
                self._current_size += self.robj_overhead() * self._current_length
            self._current_size += self._list_items_size
        self.emit_record("list", key, self._current_size, self._current_encoding, self._current_length,
                         self._len_largest_element, self._key_expiry)
        self.end_key()

    def start_module(self, key, module_id, expiry, info):
        self._key_expiry = expiry
        self._current_encoding = module_id
        self._current_size = 0
        if key is not None:
            self._current_size += self.top_level_object_overhead(key, expiry)
        self._current_size += 8 + 1  # add the module id length and EOF byte

        return False  # don't build the full key buffer

    def end_module(self, key, buffer_size, buffer=None):
        size = self._current_size + buffer_size
        self.emit_record("module", key, size, self._current_encoding, 1, size, self._key_expiry)
        self.end_key()

    def start_stream(self, key, listpacks_count, expiry, info):
        self._key_expiry = expiry
        self._current_encoding = info['encoding']
        self._current_size = self.top_level_object_overhead(key, expiry)
        self._current_size += self.sizeof_pointer()*2 + 8 + 16  # stream struct
        self._current_size += self.sizeof_pointer() + 8*2  # rax struct
        self._listpacks_count = listpacks_count

    def stream_listpack(self, key, entry_id, data):
        self._current_size += self.malloc_overhead(len(data))
        if(len(data) > self._len_largest_element):
            self._len_largest_element = len(data)
        pass

    def sizeof_stream_radix_tree(self, num_elements):
        # This is a very rough estimation. The only alternative to doing an estimation,
        # is to fully build a radix tree of similar design, and count the nodes.
        # There should be at least as many nodes as there are elements in the radix tree (possibly up to 3 times)
        num_nodes = int(num_elements * 2.5)
        # formula for memory estimation copied from Redis's streamRadixTreeMemoryUsage
        return 16*num_elements + num_nodes*4 + num_nodes*30*self.sizeof_long()

    def end_stream(self, key, items, last_entry_id, cgroups):
        # Now after we have some global key+value overheads, and all listpacks sizes,
        # we need to add some estimations for radix tree and consumer groups.
        # The logic for the memory estimation copied from Redis's MEMORY command.
        radix_tree_size = self.sizeof_stream_radix_tree(self._listpacks_count)
        cgroups_size = 0
        for cg in cgroups:
            cgroups_size += self.sizeof_pointer() * 2 + 16  # streamCG
            pending = len(cg['pending'])
            cgroups_size += self.sizeof_stream_radix_tree(pending)
            cgroups_size += pending*(self.sizeof_pointer()+8+8)  # streamNACK
            for c in cg['consumers']:
                cgroups_size += self.sizeof_pointer()*2 + 8  # streamConsumer
                cgroups_size += self.sizeof_string(c['name'])
                pending = len(c['pending'])
                cgroups_size += self.sizeof_stream_radix_tree(pending)
        size = self._current_size + radix_tree_size + cgroups_size
        self._current_length = items
        self.emit_record("stream", key, size, self._current_encoding, 1, self._len_largest_element, self._key_expiry)
        self.end_key()

    def start_sorted_set(self, key, length, expiry, info):
        self._current_length = length
        self._current_encoding = info['encoding']
        size = self.top_level_object_overhead(key, expiry)
        self._key_expiry = expiry

        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'skiplist':
            size += self.skiplist_overhead(length)
        else:
            raise Exception('start_sorted_set', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
    
    def zadd(self, key, score, member):
        if(self.element_length(member) > self._len_largest_element):
            self._len_largest_element = self.element_length(member)
        
        if self._current_encoding == 'skiplist':
            self._current_size += 8 # score (double)
            self._current_size += self.sizeof_string(member)
            if self._redis_version < StrictVersion('4.0'):
                self._current_size += self.robj_overhead()
            self._current_size += self.skiplist_entry_overhead()
    
    def end_sorted_set(self, key):
        self.emit_record("sortedset", key, self._current_size, self._current_encoding, self._current_length,
                         self._len_largest_element, self._key_expiry)
        self.end_key()
        
    def end_key(self):
        self._db_keys += 1
        self._current_encoding = None
        self._current_size = 0
        self._len_largest_element = 0
        self._key_expiry = None
    
    def sizeof_string(self, string):
        # https://github.com/antirez/redis/blob/unstable/src/sds.h
        try:
            num = int(string)
            if num < REDIS_SHARED_INTEGERS :
                return 0
            else :
                return 0  # the integer is part of the robj, no extra memory
        except ValueError:
            pass
        l = len(string)
        if self._redis_version < StrictVersion('3.2'):
            return self.malloc_overhead(l + 8 + 1)
        if l < 2**5:
            return self.malloc_overhead(l + 1 + 1)
        if l < 2**8:
            return self.malloc_overhead(l + 1 + 2 + 1)
        if l < 2**16:
            return self.malloc_overhead(l + 1 + 4 + 1)
        if l < 2**32:
            return self.malloc_overhead(l + 1 + 8 + 1)
        return self.malloc_overhead(l + 1 + 16 + 1)

    def top_level_object_overhead(self, key, expiry):
        # Each top level object is an entry in a dictionary, and so we have to include 
        # the overhead of a dictionary entry
        return self.hashtable_entry_overhead() + self.sizeof_string(key) + self.robj_overhead() + self.key_expiry_overhead(expiry)

    def key_expiry_overhead(self, expiry):
        # If there is no expiry, there isn't any overhead
        if not expiry:
            return 0
        self._db_expires += 1
        # Key expiry is stored in a hashtable, so we have to pay for the cost of a hashtable entry
        # The timestamp itself is stored as an int64, which is a 8 bytes
        return self.hashtable_entry_overhead() + 8
        
    def hashtable_overhead(self, size):
        # See  https://github.com/antirez/redis/blob/unstable/src/dict.h
        # See the structures dict and dictht
        # 2 * (3 unsigned longs + 1 pointer) + int + long + 2 pointers
        # 
        # Additionally, see **table in dictht
        # The length of the table is the next power of 2
        # When the hashtable is rehashing, another instance of **table is created
        # Due to the possibility of rehashing during loading, we calculate the worse 
        # case in which both tables are allocated, and so multiply
        # the size of **table by 1.5
        return 4 + 7*self.sizeof_long() + 4*self.sizeof_pointer() + self.next_power(size)*self.sizeof_pointer()*1.5
        
    def hashtable_entry_overhead(self):
        # See  https://github.com/antirez/redis/blob/unstable/src/dict.h
        # Each dictEntry has 2 pointers + int64
        return 2*self.sizeof_pointer() + 8
    
    def linkedlist_overhead(self):
        # See https://github.com/antirez/redis/blob/unstable/src/adlist.h
        # A list has 5 pointers + an unsigned long
        return self.sizeof_long() + 5*self.sizeof_pointer()

    def quicklist_overhead(self, zip_count):
        quicklist = 2*self.sizeof_pointer()+self.sizeof_long()+2*4
        quickitem = 4*self.sizeof_pointer()+self.sizeof_long()+2*4
        return quicklist + zip_count*quickitem

    def linkedlist_entry_overhead(self):
        # See https://github.com/antirez/redis/blob/unstable/src/adlist.h
        # A node has 3 pointers
        return 3*self.sizeof_pointer()

    def ziplist_header_overhead(self):
        # See https://github.com/antirez/redis/blob/unstable/src/ziplist.c
        # <zlbytes><zltail><zllen><entry><entry><zlend>
        return 4 + 4 + 2 + 1

    def ziplist_entry_overhead(self, value):
        # See https://github.com/antirez/redis/blob/unstable/src/ziplist.c
        if self.is_integer_type(value):
            header = 1
            if value < 12:
                size = 0
            elif value < 2**8:
                size = 1
            elif value < 2**16:
                size = 2
            elif value < 2**24:
                size = 3
            elif value < 2**32:
                size = 4
            else:
                size = 8
        else:
            size = len(value)
            if size <= 63:
                header = 1
            elif size <= 16383:
                header = 2
            else:
                header = 5
        # add len again for prev_len of the next record
        prev_len = 1 if size < 254 else 5
        return prev_len + header + size

    def skiplist_overhead(self, size):
        return 2*self.sizeof_pointer() + self.hashtable_overhead(size) + (2*self.sizeof_pointer() + 16)
    
    def skiplist_entry_overhead(self):
        return self.hashtable_entry_overhead() + 2*self.sizeof_pointer() + 8 + (self.sizeof_pointer() + 8) * self.zset_random_level()
    
    def robj_overhead(self):
        return self.sizeof_pointer() + 8
        
    def malloc_overhead(self, size):
        alloc = get_jemalloc_allocation(size)
        self._total_internal_frag += alloc - size
        return alloc

    def size_t(self):
        return self.sizeof_pointer()
        
    def sizeof_pointer(self):
        return self._pointer_size
        
    def sizeof_long(self):
        return self._long_size

    def next_power(self, size):
        power = 1
        while (power <= size) :
            power = power << 1
        return power
 
    def zset_random_level(self):
        level = 1
        rint = random.randint(0, 0xFFFF)
        while (rint < ZSKIPLIST_P * 0xFFFF):
            level += 1
            rint = random.randint(0, 0xFFFF)        
        if level < ZSKIPLIST_MAXLEVEL :
            return level
        else:
            return ZSKIPLIST_MAXLEVEL

    def is_integer_type(self, ob):
        if isinstance(ob, int):
            return True
        if sys.version_info < (3,):
            if isinstance(ob, long):
                return True
        return False

    def element_length(self, element):
        if self.is_integer_type(element):
            return self._long_size
        return len(element)


# size classes from jemalloc 4.0.4 using LG_QUANTUM=3
jemalloc_size_classes = [
    8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024,
    1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384, 20480, 24576,
    28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688,131072, 163840, 196608, 229376, 262144, 327680,
    393216, 458752, 524288, 655360, 786432, 917504, 1048576, 1310720, 1572864, 1835008, 2097152, 2621440, 3145728,
    3670016, 4194304, 5242880, 6291456, 7340032, 8388608, 10485760, 12582912, 14680064, 16777216, 20971520, 25165824,
    29360128, 33554432, 41943040, 50331648, 58720256, 67108864, 83886080, 100663296, 117440512, 134217728, 167772160,
    201326592, 234881024, 268435456, 335544320, 402653184, 469762048, 536870912, 671088640, 805306368, 939524096,
    1073741824, 1342177280, 1610612736, 1879048192, 2147483648, 2684354560, 3221225472, 3758096384, 4294967296,
    5368709120, 6442450944, 7516192768, 8589934592, 10737418240, 12884901888, 15032385536, 17179869184, 21474836480,
    25769803776, 30064771072, 34359738368, 42949672960, 51539607552, 60129542144, 68719476736, 85899345920,
    103079215104, 120259084288, 137438953472, 171798691840, 206158430208, 240518168576, 274877906944, 343597383680,
    412316860416, 481036337152, 549755813888, 687194767360, 824633720832, 962072674304, 1099511627776,1374389534720,
    1649267441664, 1924145348608, 2199023255552, 2748779069440, 3298534883328, 3848290697216, 4398046511104,
    5497558138880, 6597069766656, 7696581394432, 8796093022208, 10995116277760, 13194139533312, 15393162788864,
    17592186044416, 21990232555520, 26388279066624, 30786325577728, 35184372088832, 43980465111040, 52776558133248,
    61572651155456, 70368744177664, 87960930222080, 105553116266496, 123145302310912, 140737488355328, 175921860444160,
    211106232532992, 246290604621824, 281474976710656, 351843720888320, 422212465065984, 492581209243648,
    562949953421312, 703687441776640, 844424930131968, 985162418487296, 1125899906842624, 1407374883553280,
    1688849860263936, 1970324836974592, 2251799813685248, 2814749767106560, 3377699720527872, 3940649673949184,
    4503599627370496, 5629499534213120, 6755399441055744, 7881299347898368, 9007199254740992, 11258999068426240,
    13510798882111488, 15762598695796736, 18014398509481984, 22517998136852480, 27021597764222976,31525197391593472,
    36028797018963968, 45035996273704960, 54043195528445952, 63050394783186944, 72057594037927936, 90071992547409920,
    108086391056891904, 126100789566373888, 144115188075855872, 180143985094819840, 216172782113783808,
    252201579132747776, 288230376151711744, 360287970189639680, 432345564227567616, 504403158265495552,
    576460752303423488, 720575940379279360, 864691128455135232, 1008806316530991104, 1152921504606846976,
    1441151880758558720, 1729382256910270464, 2017612633061982208, 2305843009213693952, 2882303761517117440,
    3458764513820540928, 4035225266123964416, 4611686018427387904, 5764607523034234880, 6917529027641081856,
    8070450532247928832, 9223372036854775808, 11529215046068469760, 13835058055282163712, 16140901064495857664
]  # TODO: use different table depending oon the redis-version used

def get_jemalloc_allocation(size):
    idx = bisect.bisect_left(jemalloc_size_classes, size)
    alloc = jemalloc_size_classes[idx] if idx < len(jemalloc_size_classes) else size
    return alloc
