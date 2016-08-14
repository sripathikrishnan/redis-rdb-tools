import calendar
import re
from decimal import Decimal
import sys
import struct
from rdbtools.parser import RdbCallback, RdbParser

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
    if isinstance(s, int) or isinstance(s, long):
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
    
    def start_list(self, key, expiry, info):
        self._start_key(key, 0)
        self._out.write('%s:[' % encode_key(key))
    
    def rpush(self, key, value) :
        self._elements_in_key += 1
        self._write_comma()
        self._out.write('%s' % encode_value(value))
    
    def end_list(self, key, info):
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
    
    def start_list(self, key, expiry, info):
        self._index = 0
            
    def rpush(self, key, value) :
        self._out.write('db=%d %s[%d] -> %s' % (self._dbnum, encode_key(key), self._index, encode_value(value)))
        self.newline()
        self._index = self._index + 1
    
    def end_list(self, key, info):
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


def _unix_timestamp(dt):
     return calendar.timegm(dt.utctimetuple())


class ProtocolCallback(RdbCallback):
    def __init__(self, out):
        self._out = out
        self.reset()

    def reset(self):
        self._expires = {}

    def set_expiry(self, key, dt):
        self._expires[key] = dt

    def get_expiry_seconds(self, key):
        if key in self._expires:
            return _unix_timestamp(self._expires[key])
        return None

    def expires(self, key):
        return key in self._expires

    def pre_expiry(self, key, expiry):
        if expiry is not None:
            self.set_expiry(key, expiry)

    def post_expiry(self, key):
        if self.expires(key):
            self.expireat(key, self.get_expiry_seconds(key))

    def emit(self, *args):
        self._out.write(u"*" + unicode(len(args)) + u"\r\n")
        for arg in args:
            self._out.write(u"$" + unicode(len(unicode(arg))) + u"\r\n")
            self._out.write(unicode(arg) + u"\r\n")

    def start_database(self, db_number):
        self.reset()
        self.select(db_number)

    # String handling

    def set(self, key, value, expiry, info):
        self.pre_expiry(key, expiry)
        self.emit('SET', key, value)
        self.post_expiry(key)

    # Hash handling

    def start_hash(self, key, length, expiry, info):
        self.pre_expiry(key, expiry)

    def hset(self, key, field, value):
        self.emit('HSET', key, field, value)

    def end_hash(self, key):
        self.post_expiry(key)

    # Set handling

    def start_set(self, key, cardinality, expiry, info):
        self.pre_expiry(key, expiry)

    def sadd(self, key, member):
        self.emit('SADD', key, member)

    def end_set(self, key):
        self.post_expiry(key)

    # List handling

    def start_list(self, key, expiry, info):
        self.pre_expiry(key, expiry)

    def rpush(self, key, value):
        self.emit('RPUSH', key, value)

    def end_list(self, key, info):
        self.post_expiry(key)

    # Sorted set handling

    def start_sorted_set(self, key, length, expiry, info):
        self.pre_expiry(key, expiry)

    def zadd(self, key, score, member):
        self.emit('ZADD', key, score, member)

    def end_sorted_set(self, key):
        self.post_expiry(key)

    # Other misc commands

    def select(self, db_number):
        self.emit('SELECT', db_number)

    def expireat(self, key, timestamp):
        self.emit('EXPIREAT', key, timestamp)
