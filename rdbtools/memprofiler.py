from collections import namedtuple
import random
import json

from rdbtools.parser import RdbCallback
from rdbtools.callbacks import encode_key

ZSKIPLIST_MAXLEVEL=32
ZSKIPLIST_P=0.25
REDIS_SHARED_INTEGERS = 10000

MemoryRecord = namedtuple('MemoryRecord', ['database', 'type', 'key', 'bytes', 'encoding','size', 'len_largest_element'])

class StatsAggregator():
    def __init__(self, key_groupings = None):
        self.aggregates = {}
        self.scatters = {}
        self.histograms = {}

    def next_record(self, record):
        self.add_aggregate('database_memory', record.database, record.bytes)
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
  
    def get_json(self):
        return json.dumps({"aggregates":self.aggregates, "scatters":self.scatters, "histograms":self.histograms})
        
class PrintAllKeys():
    def __init__(self, out):
        self._out = out
        self._out.write("%s,%s,%s,%s,%s,%s,%s\n" % ("database", "type", "key", 
                                                 "size_in_bytes", "encoding", "num_elements", "len_largest_element"))
    
    def next_record(self, record) :
        self._out.write("%d,%s,%s,%d,%s,%d,%d\n" % (record.database, record.type, encode_key(record.key), 
                                                 record.bytes, record.encoding, record.size, record.len_largest_element))
    
class MemoryCallback(RdbCallback):
    '''Calculates the memory used if this rdb file were loaded into RAM
        The memory usage is approximate, and based on heuristics.
    '''
    def __init__(self, stream, architecture):
        self._stream = stream
        self._dbnum = 0
        self._current_size = 0
        self._current_encoding = None
        self._current_length = 0
        self._len_largest_element = 0
        
        if architecture == 64 or architecture == '64':
            self._pointer_size = 8
        elif architecture == 32 or architecture == '32':
            self._pointer_size = 4
        
    def start_rdb(self):
        pass

    def start_database(self, db_number):
        self._dbnum = db_number

    def end_database(self, db_number):
        pass
        
    def end_rdb(self):
        pass
       
    def set(self, key, value, expiry, info):
        self._current_encoding = info['encoding']
        size = self.sizeof_string(key) + self.sizeof_string(value) + self.top_level_object_overhead()
        size += 2*self.robj_overhead()
        size += self.key_expiry_overhead(expiry)
        
        length = element_length(value)
        record = MemoryRecord(self._dbnum, "string", key, size, self._current_encoding, length, length)
        self._stream.next_record(record)
        self.end_key()
    
    def start_hash(self, key, length, expiry, info):
        self._current_encoding = info['encoding']
        self._current_length = length        
        size = self.sizeof_string(key)
        size += 2*self.robj_overhead()
        size += self.top_level_object_overhead()
        size += self.key_expiry_overhead(expiry)
        
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'hashtable':
            size += self.hashtable_overhead(length)
        else:
            raise Exception('start_hash', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
    
    def hset(self, key, field, value):
        if(element_length(field) > self._len_largest_element) :
            self._len_largest_element = element_length(field)
        if(element_length(value) > self._len_largest_element) :
            self._len_largest_element = element_length(value)
        
        if self._current_encoding == 'hashtable':
            self._current_size += self.sizeof_string(field)
            self._current_size += self.sizeof_string(value)
            self._current_size += self.hashtable_entry_overhead()
            self._current_size += 2*self.robj_overhead()
    
    def end_hash(self, key):
        record = MemoryRecord(self._dbnum, "hash", key, self._current_size, self._current_encoding, self._current_length, self._len_largest_element)
        self._stream.next_record(record)
        self.end_key()
    
    def start_set(self, key, cardinality, expiry, info):
        # A set is exactly like a hashmap
        self.start_hash(key, cardinality, expiry, info)

    def sadd(self, key, member):
        if(element_length(member) > self._len_largest_element) :
            self._len_largest_element = element_length(member)
            
        if self._current_encoding == 'hashtable':
            self._current_size += self.sizeof_string(member)
            self._current_size += self.hashtable_entry_overhead()
            self._current_size += self.robj_overhead()
    
    def end_set(self, key):
        record = MemoryRecord(self._dbnum, "set", key, self._current_size, self._current_encoding, self._current_length, self._len_largest_element)
        self._stream.next_record(record)
        self.end_key()
    
    def start_list(self, key, length, expiry, info):
        self._current_length = length
        self._current_encoding = info['encoding']
        size = self.sizeof_string(key)
        size += 2*self.robj_overhead()
        size += self.top_level_object_overhead()
        size += self.key_expiry_overhead(expiry)
        
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'linkedlist':
            size += self.linkedlist_overhead()
        else:
            raise Exception('start_list', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
            
    def rpush(self, key, value) :
        if(element_length(value) > self._len_largest_element) :
            self._len_largest_element = element_length(value)
        
        if self._current_encoding == 'linkedlist':
            self._current_size += self.sizeof_string(value)
            self._current_size += self.linkedlist_entry_overhead()
            self._current_size += self.robj_overhead()
    
    def end_list(self, key):
        record = MemoryRecord(self._dbnum, "list", key, self._current_size, self._current_encoding, self._current_length, self._len_largest_element)
        self._stream.next_record(record)
        self.end_key()
    
    def start_sorted_set(self, key, length, expiry, info):
        self._current_length = length
        self._current_encoding = info['encoding']
        size = self.sizeof_string(key)
        size += 2*self.robj_overhead()
        size += self.top_level_object_overhead()
        size += self.key_expiry_overhead(expiry)
        
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'skiplist':
            size += self.skiplist_overhead(length)
        else:
            raise Exception('start_sorted_set', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
    
    def zadd(self, key, score, member):
        if(element_length(member) > self._len_largest_element):
            self._len_largest_element = element_length(member)
        
        if self._current_encoding == 'skiplist':
            self._current_size += 8 # self.sizeof_string(score)
            self._current_size += self.sizeof_string(member)
            self._current_size += 2*self.robj_overhead()
            self._current_size += self.skiplist_entry_overhead()
    
    def end_sorted_set(self, key):
        record = MemoryRecord(self._dbnum, "sortedset", key, self._current_size, self._current_encoding, self._current_length, self._len_largest_element)
        self._stream.next_record(record)
        self.end_key()
        
    def end_key(self):
        self._current_encoding = None
        self._current_size = 0
        self._len_largest_element = 0
    
    def sizeof_string(self, string):
        # See struct sdshdr over here https://github.com/antirez/redis/blob/unstable/src/sds.h
        # int len :  4 bytes
        # int free : 4 bytes
        # char buf[] : size will be the length of the string
        # 1 extra byte is used to store the null character at the end of the string
        # Redis internally stores integers as a long
        #  Integers less than REDIS_SHARED_INTEGERS are stored in a shared memory pool
        try:
            num = int(string)
            if num < REDIS_SHARED_INTEGERS :
                return 0
            else :
                return 8
        except ValueError:
            pass
        return len(string) + 8 + 1 + self.malloc_overhead()

    def top_level_object_overhead(self):
        # Each top level object is an entry in a dictionary, and so we have to include 
        # the overhead of a dictionary entry
        return self.hashtable_entry_overhead()

    def key_expiry_overhead(self, expiry):
        # If there is no expiry, there isn't any overhead
        if not expiry:
            return 0
        # Key expiry is stored in a hashtable, so we have to pay for the cost of a hashtable entry
        # The timestamp itself is stored as an int64, which is a 8 bytes
        return self.hashtable_entry_overhead() + 8
        
    def hashtable_overhead(self, size):
        # See  https://github.com/antirez/redis/blob/unstable/src/dict.h
        # See the structures dict and dictht
        # 2 * (3 unsigned longs + 1 pointer) + 2 ints + 2 pointers
        #   = 56 + 4 * sizeof_pointer()
        # 
        # Additionally, see **table in dictht
        # The length of the table is the next power of 2
        # When the hashtable is rehashing, another instance of **table is created
        # We are assuming 0.5 percent probability of rehashing, and so multiply 
        # the size of **table by 1.5
        return 56 + 4*self.sizeof_pointer() + self.next_power(size)*self.sizeof_pointer()*1.5
        
    def hashtable_entry_overhead(self):
        # See  https://github.com/antirez/redis/blob/unstable/src/dict.h
        # Each dictEntry has 3 pointers 
        return 3*self.sizeof_pointer()
    
    def linkedlist_overhead(self):
        # See https://github.com/antirez/redis/blob/unstable/src/adlist.h
        # A list has 5 pointers + an unsigned long
        return 8 + 5*self.sizeof_pointer()
    
    def linkedlist_entry_overhead(self):
        # See https://github.com/antirez/redis/blob/unstable/src/adlist.h
        # A node has 3 pointers
        return 3*self.sizeof_pointer()
    
    def skiplist_overhead(self, size):
        return 2*self.sizeof_pointer() + self.hashtable_overhead(size) + (2*self.sizeof_pointer() + 16)
    
    def skiplist_entry_overhead(self):
        return self.hashtable_entry_overhead() + 2*self.sizeof_pointer() + 8 + (self.sizeof_pointer() + 8) * self.zset_random_level()
    
    def robj_overhead(self):
        return self.sizeof_pointer() + 8
        
    def malloc_overhead(self):
        return self.size_t()

    def size_t(self):
        return self.sizeof_pointer()
        
    def sizeof_pointer(self):
        return self._pointer_size
        
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
        

def element_length(element):
    if isinstance(element, int):
        return 8
    if isinstance(element, long):
        return 16
    else:
        return len(element)
    
