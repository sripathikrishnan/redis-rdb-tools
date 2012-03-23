import re
from decimal import Decimal
import sys
import struct
from rdbtools.parser import RdbCallback, RdbParser

REDIS_SHARED_INTEGERS = 10000

ESCAPE = re.compile(ur'[\x00-\x1f\\"\b\f\n\r\t\u2028\u2029]')
ESCAPE_ASCII = re.compile(r'([\\"]|[^\ -~])')
HAS_UTF8 = re.compile(r'[\x80-\xff]')
ESCAPE_DCT = {
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
    u'\u2028': '\\u2028',
    u'\u2029': '\\u2029',
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), '\\u%04x' % (i,))

def _floatconstants():
    _BYTES = '7FF80000000000007FF0000000000000'.decode('hex')
    # The struct module in Python 2.4 would get frexp() out of range here
    # when an endian is specified in the format string. Fixed in Python 2.5+
    if sys.byteorder != 'big':
        _BYTES = _BYTES[:8][::-1] + _BYTES[8:][::-1]
    nan, inf = struct.unpack('dd', _BYTES)
    return nan, inf, -inf

NaN, PosInf, NegInf = _floatconstants()

def _encode_basestring(s):
    """Return a JSON representation of a Python string"""
    if isinstance(s, str) and HAS_UTF8.search(s) is not None:
        s = s.decode('utf-8')
    def replace(match):
        return ESCAPE_DCT[match.group(0)]
    return u'"' + ESCAPE.sub(replace, s) + u'"'

def _encode_basestring_ascii(s):
    """Return an ASCII-only JSON representation of a Python string

    """
    try :
        if isinstance(s, str) and HAS_UTF8.search(s) is not None:
            s = s.decode('utf-8')
    except:
        pass

    def replace(match):
        s = match.group(0)
        try:
            return ESCAPE_DCT[s]
        except KeyError:
            n = ord(s)
            if n < 0x10000:
                #return '\\u{0:04x}'.format(n)
                return '\\u%04x' % (n,)
            else:
                # surrogate pair
                n -= 0x10000
                s1 = 0xd800 | ((n >> 10) & 0x3ff)
                s2 = 0xdc00 | (n & 0x3ff)
                return '\\u%04x\\u%04x' % (s1, s2)
    return '"' + str(ESCAPE_ASCII.sub(replace, s)) + '"'

def _encode(s, quote_numbers = True):
    if quote_numbers:
        qn = '"'
    else:
        qn = ''
    if isinstance(s, int):
        return qn + str(s) + qn
    elif isinstance(s, float):
        if s != s:
            return "NaN"
        elif s == PosInf:
            return "Infinity"
        elif s == NegInf:
            return "-Infinity"
        else:
            return qn + str(s) + qn
    else:
        return _encode_basestring_ascii(s)

def encode_key(s):
    return _encode(s, quote_numbers=True)

def encode_value(s):
    return _encode(s, quote_numbers=False)

class JSONCallback(RdbCallback):
    def __init__(self, out):
        self._out = out
        self._is_first_db = True
        self._has_databases = False
        self._is_first_key_in_db = True
        self._elements_in_key = 0 
        self._element_index = 0
        
    def start_rdb(self):
        self._out.write('[')
    
    def start_database(self, db_number):
        if not self._is_first_db:
            self._out.write('},')
        self._out.write('{')
        self._is_first_db = False
        self._has_databases = True
        self._is_first_key_in_db = True

    def end_database(self, db_number):
        pass
        
    def end_rdb(self):
        if self._has_databases:
            self._out.write('}')
        self._out.write(']')

    def _start_key(self, key, length):
        if not self._is_first_key_in_db:
            self._out.write(',')
        self._out.write('\r\n')
        self._is_first_key_in_db = False
        self._elements_in_key = length
        self._element_index = 0
    
    def _end_key(self, key):
        pass
    
    def _write_comma(self):
        if self._element_index > 0 and self._element_index < self._elements_in_key :
            self._out.write(',')
        self._element_index = self._element_index + 1
        
    def set(self, key, value, expiry, info):
        self._start_key(key, 0)
        self._out.write('%s:%s' % (encode_key(key), encode_value(value)))
    
    def start_hash(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write('%s:{' % encode_key(key))
    
    def hset(self, key, field, value):
        self._write_comma()
        self._out.write('%s:%s' % (encode_key(field), encode_value(value)))
    
    def end_hash(self, key):
        self._end_key(key)
        self._out.write('}')
    
    def start_set(self, key, cardinality, expiry, info):
        self._start_key(key, cardinality)
        self._out.write('%s:[' % encode_key(key))

    def sadd(self, key, member):
        self._write_comma()
        self._out.write('%s' % encode_value(member))
    
    def end_set(self, key):
        self._end_key(key)
        self._out.write(']')
    
    def start_list(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write('%s:[' % encode_key(key))
    
    def rpush(self, key, value) :
        self._write_comma()
        self._out.write('%s' % encode_value(value))
    
    def end_list(self, key):
        self._end_key(key)
        self._out.write(']')
    
    def start_sorted_set(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write('%s:{' % encode_key(key))
    
    def zadd(self, key, score, member):
        self._write_comma()
        self._out.write('%s:%s' % (encode_key(member), encode_value(score)))
    
    def end_sorted_set(self, key):
        self._end_key(key)
        self._out.write('}')


class DiffCallback(RdbCallback):
    '''Prints the contents of RDB in a format that is unix sort friendly, 
        so that two rdb files can be diffed easily'''
    def __init__(self, out):
        self._out = out
        self._index = 0
        self._dbnum = 0
        
    def start_rdb(self):
        pass
    
    def start_database(self, db_number):
        self._dbnum = db_number

    def end_database(self, db_number):
        pass
        
    def end_rdb(self):
        pass
       
    def set(self, key, value, expiry, info):
        self._out.write('db=%d %s -> %s' % (self._dbnum, encode_key(key), encode_value(value)))
        self.newline()
    
    def start_hash(self, key, length, expiry, info):
        pass
    
    def hset(self, key, field, value):
        self._out.write('db=%d %s . %s -> %s' % (self._dbnum, encode_key(key), encode_key(field), encode_value(value)))
        self.newline()
    
    def end_hash(self, key):
        pass
    
    def start_set(self, key, cardinality, expiry, info):
        pass

    def sadd(self, key, member):
        self._out.write('db=%d %s { %s }' % (self._dbnum, encode_key(key), encode_value(member)))
        self.newline()
    
    def end_set(self, key):
        pass
    
    def start_list(self, key, length, expiry, info):
        self._index = 0
            
    def rpush(self, key, value) :
        self._out.write('db=%d %s[%d] -> %s' % (self._dbnum, encode_key(key), self._index, encode_value(value)))
        self.newline()
        self._index = self._index + 1
    
    def end_list(self, key):
        pass
    
    def start_sorted_set(self, key, length, expiry, info):
        self._index = 0
    
    def zadd(self, key, score, member):
        self._out.write('db=%d %s[%d] -> {%s, score=%s}' % (self._dbnum, encode_key(key), self._index, encode_key(member), encode_value(score)))
        self.newline()
        self._index = self._index + 1
    
    def end_sorted_set(self, key):
        pass

    def newline(self):
        self._out.write('\r\n')
        

class MemoryCallback(RdbCallback):
    '''Calculates the memory used if this rdb file were loaded into RAM
        The memory usage is approximate, and based on heuristics.
        The memory usage is stored in a tree like structure, so you can perform rollups
    '''
    def __init__(self, out, architecture):
        self._out = out
        self._dbnum = 0
        self._current_size = 0
        self._current_encoding = None
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
        size = self.sizeof_string(key) + self.sizeof_string(value) + self.hashtable_entry_overhead()
        self._out.write('%d, %s, %s, %d, %s' % (self._dbnum, "string", encode_key(key), size, self._current_encoding))
        self.end_key()
    
    def start_hash(self, key, length, expiry, info):
        self._current_encoding = info['encoding']
        size = self.sizeof_string(key)
        size += self.hashtable_entry_overhead()
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'hashtable':
            size += self.hashtable_overhead(length)
        else:
            raise Exception('start_hash', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
    
    def hset(self, key, field, value):
        if self._current_encoding == 'hashtable':
            self._current_size += self.sizeof_string(field)
            self._current_size += self.sizeof_string(value)
            self._current_size += self.hashtable_entry_overhead()
    
    def end_hash(self, key):
        self._out.write('%d, %s, %s, %d, %s' % (self._dbnum, "hash", encode_key(key), self._current_size, self._current_encoding))
        self.end_key()
    
    def start_set(self, key, cardinality, expiry, info):
        # A set is exactly like a hashmap
        self.start_hash(key, cardinality, expiry, info)

    def sadd(self, key, member):
        if self._current_encoding == 'hashtable':
            self._current_size += self.sizeof_string(member)
            self._current_size += self.hashtable_entry_overhead()
    
    def end_set(self, key):
        self._out.write('%d, %s, %s, %d, %s' % (self._dbnum, "set", encode_key(key), self._current_size, self._current_encoding))
        self.end_key()
    
    def start_list(self, key, length, expiry, info):
        self._current_encoding = info['encoding']
        size = self.sizeof_string(key)
        size += self.hashtable_entry_overhead()
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'linkedlist':
            size += self.linkedlist_overhead()
        else:
            raise Exception('start_list', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
            
    def rpush(self, key, value) :
        if self._current_encoding == 'linkedlist':
            self._current_size += self.sizeof_string(value)
            self._current_size += self.linkedlist_entry_overhead()
    
    def end_list(self, key):
        self._out.write('%d, %s, %s, %d, %s' % (self._dbnum, "list", encode_key(key), self._current_size, self._current_encoding))
        self.end_key()
    
    def start_sorted_set(self, key, length, expiry, info):
        self._current_encoding = info['encoding']
        size = self.sizeof_string(key)
        size += self.hashtable_entry_overhead()
        if 'sizeof_value' in info:
            size += info['sizeof_value']
        elif 'encoding' in info and info['encoding'] == 'skiplist':
            size += self.skiplist_overhead(length)
        else:
            raise Exception('start_sorted_set', 'Could not find encoding or sizeof_value in info object %s' % info)
        self._current_size = size
    
    def zadd(self, key, score, member):
        if self._current_encoding == 'skiplist':
            self._current_size += self.sizeof_string(value)
            self._current_size += self.skiplist_entry_overhead()
    
    def end_sorted_set(self, key):
        self._out.write('%d, %s, %s, %d, %s' % (self._dbnum, "sortedset", encode_key(key), self._current_size, self._current_encoding, ))
        self.end_key()
        
    def end_key(self):
        self._current_encoding = None
        self._current_size = 0
        self.newline()
        
    def newline(self):
        self._out.write('\r\n')
    
    def sizeof_string(self, string):
        # See struct sdshdr over here https://github.com/antirez/redis/blob/unstable/src/sds.h
        # int len :  4 bytes
        # int free : 4 bytes
        # char buf[] : size will be the length of the string
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
        return len(string) + 8
    
    def hashtable_overhead(self, size):
        # See  https://github.com/antirez/redis/blob/unstable/src/dict.h
        # See the structures dict and dictht
        # 2 * (3 unsigned longs + 1 pointer) + 2 ints + 2 pointers
        #   = 56 + 4 * sizeof_pointer()
        # Additionally, see **table in dictht
        # The length of the table is the next power of 2
        return 56 + 4*self.sizeof_pointer() + self.next_power(size)*self.sizeof_pointer()
        
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
        return self.hashtable_overhead(size)
    
    def skiplist_entry_overhead(self):
        return self.hashtable_entry_overhead()
        
    def sizeof_pointer(self):
        return self._pointer_size
        
    def next_power(self, size):
        power = 1
        while (power < size) :
            power = power << 1
        return power
        
