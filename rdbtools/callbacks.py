import calendar
import codecs
import json

from rdbtools.parser import RdbCallback
from rdbtools import encodehelpers


class JSONCallback(RdbCallback):
    def __init__(self, out, string_escape=None):
        if string_escape is None:
            string_escape = encodehelpers.STRING_ESCAPE_UTF8
        super(JSONCallback, self).__init__(string_escape)
        self._out = out
        self._is_first_db = True
        self._has_databases = False
        self._is_first_key_in_db = True
        self._elements_in_key = 0 
        self._element_index = 0

    def encode_key(self, key):
        key = encodehelpers.bytes_to_unicode(key, self._escape, skip_printable=True)
        return codecs.encode(json.dumps(key), 'utf-8')

    def encode_value(self, val):
        val = encodehelpers.bytes_to_unicode(val, self._escape)
        return codecs.encode(json.dumps(val), 'utf-8')

    def start_rdb(self):
        self._out.write(b'[')
    
    def start_database(self, db_number):
        if not self._is_first_db:
            self._out.write(b'},')
        self._out.write(b'{')
        self._is_first_db = False
        self._has_databases = True
        self._is_first_key_in_db = True

    def end_database(self, db_number):
        pass
        
    def end_rdb(self):
        if self._has_databases:
            self._out.write(b'}')
        self._out.write(b']')

    def _start_key(self, key, length):
        if not self._is_first_key_in_db:
            self._out.write(b',')
        self._out.write(b'\r\n')
        self._is_first_key_in_db = False
        self._elements_in_key = length
        self._element_index = 0
    
    def _end_key(self, key):
        pass
    
    def _write_comma(self):
        if self._element_index > 0 and self._element_index < self._elements_in_key :
            self._out.write(b',')
        self._element_index = self._element_index + 1
        
    def set(self, key, value, expiry, info):
        self._start_key(key, 0)
        self._out.write(self.encode_key(key) + b':' + self.encode_value(value))
    
    def start_hash(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write(self.encode_key(key) + b':{')
    
    def hset(self, key, field, value):
        self._write_comma()
        self._out.write(self.encode_key(field) + b':' + self.encode_value(value))
    
    def end_hash(self, key):
        self._end_key(key)
        self._out.write(b'}')
    
    def start_set(self, key, cardinality, expiry, info):
        self._start_key(key, cardinality)
        self._out.write(self.encode_key(key) + b':[')

    def sadd(self, key, member):
        self._write_comma()
        self._out.write(self.encode_value(member))
    
    def end_set(self, key):
        self._end_key(key)
        self._out.write(b']')
    
    def start_list(self, key, expiry, info):
        self._start_key(key, 0)
        self._out.write(self.encode_key(key) + b':[')
    
    def rpush(self, key, value) :
        self._elements_in_key += 1
        self._write_comma()
        self._out.write(self.encode_value(value))
    
    def end_list(self, key, info):
        self._end_key(key)
        self._out.write(b']')
    
    def start_sorted_set(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write(self.encode_key(key) + b':{')
    
    def zadd(self, key, score, member):
        self._write_comma()
        self._out.write(self.encode_key(member) + b':' + self.encode_value(score))
    
    def end_sorted_set(self, key):
        self._end_key(key)
        self._out.write(b'}')
        

class KeysOnlyCallback(RdbCallback):
    def __init__(self, out, string_escape=None):
        super(KeysOnlyCallback, self).__init__(string_escape)
        self._out = out
    
    def _keyout(self, key):
        self._out.write(self.encode_key(key) + b'\n')

    def set(self, key, value, expiry, info):
        self._keyout(key)
    
    def start_hash(self, key, length, expiry, info):
        self._keyout(key)
    
    def hset(self, key, field, value):
        self._keyout(key)

    def start_set(self, key, cardinality, expiry, info):
        self._keyout(key)

    def sadd(self, key, member):
        self._keyout(key)
    
    def start_list(self, key, expiry, info):
        self._keyout(key)

    def rpush(self, key, value) :
        self._keyout(key)

    def start_sorted_set(self, key, length, expiry, info):
        self._keyout(key)

    def zadd(self, key, score, member):
        self._keyout(key)
        

class KeyValsOnlyCallback(RdbCallback):
    def __init__(self, out, string_escape=None):
        super(KeyValsOnlyCallback, self).__init__(string_escape)
        self._out = out
        self._is_first_db = True
        self._has_databases = False
        self._is_first_key_in_db = True
        self._elements_in_key = 0 
        self._element_index = 0

    def _start_key(self, key, length):
        if not self._is_first_key_in_db:
            self._out.write(b',')
        self._out.write(b'\r\n')
        self._is_first_key_in_db = False
        self._elements_in_key = length
        self._element_index = 0
    
    def _end_key(self, key):
        pass
    
    def _write_comma(self):
        if self._element_index > 0 and self._element_index < self._elements_in_key :
            self._out.write(b',')
        self._element_index = self._element_index + 1
        
    def set(self, key, value, expiry, info):
        self._start_key(key, 0)
        self._out.write(self.encode_key(key) + b' ' + self.encode_value(value))
    
    def start_hash(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write(self.encode_key(key) + b' ')
    
    def hset(self, key, field, value):
        self._write_comma()
        self._out.write(self.encode_key(field) + b' ' + self.encode_value(value))
    
    def end_hash(self, key):
        self._end_key(key)
    
    def start_set(self, key, cardinality, expiry, info):
        self._start_key(key, cardinality)
        self._out.write(self.encode_key(key) + b' ')

    def sadd(self, key, member):
        self._write_comma()
        self._out.write(self.encode_value(member))
    
    def end_set(self, key):
        self._end_key(key)
    
    def start_list(self, key, expiry, info):
        self._start_key(key, 0)
        self._out.write(self.encode_key(key) + b' ')
    
    def rpush(self, key, value) :
        self._elements_in_key += 1
        self._write_comma()
        self._out.write(self.encode_value(value))
    
    def end_list(self, key, info):
        self._end_key(key)
    
    def start_sorted_set(self, key, length, expiry, info):
        self._start_key(key, length)
        self._out.write(self.encode_key(key) + b' ')
    
    def zadd(self, key, score, member):
        self._write_comma()
        self._out.write(self.encode_key(member) + b' ' + self.encode_value(score))
    
    def end_sorted_set(self, key):
        self._end_key(key)


class DiffCallback(RdbCallback):
    '''Prints the contents of RDB in a format that is unix sort friendly, 
        so that two rdb files can be diffed easily'''
    def __init__(self, out, string_escape=None):
        if string_escape is None:
            string_escape = encodehelpers.STRING_ESCAPE_PRINT
        super(DiffCallback, self).__init__(string_escape)
        self._out = out
        self._index = 0
        self._dbnum = 0

    def dbstr(self):
        return b'db=' + encodehelpers.num2bytes(self._dbnum) + b' '
    def start_rdb(self):
        pass
    
    def start_database(self, db_number):
        self._dbnum = db_number

    def end_database(self, db_number):
        pass
        
    def end_rdb(self):
        pass
       
    def set(self, key, value, expiry, info):
        self._out.write(self.dbstr() + self.encode_key(key) + b' -> ' + self.encode_value(value))
        self.newline()
    
    def start_hash(self, key, length, expiry, info):
        pass
    
    def hset(self, key, field, value):
        self._out.write(
            self.dbstr() + self.encode_key(key) + b' . ' + self.encode_key(field) + b' -> ' + self.encode_value(value))
        self.newline()
    
    def end_hash(self, key):
        pass
    
    def start_set(self, key, cardinality, expiry, info):
        pass

    def sadd(self, key, member):
        self._out.write(self.dbstr() + self.encode_key(key) + b' { ' + self.encode_value(member) + b' }')
        self.newline()
    
    def end_set(self, key):
        pass
    
    def start_list(self, key, expiry, info):
        self._index = 0
            
    def rpush(self, key, value) :
        istr = encodehelpers.num2bytes(self._index)
        self._out.write(self.dbstr() + self.encode_key(key) + b'[' + istr + b'] -> ' + self.encode_value(value))
        self.newline()
        self._index = self._index + 1
    
    def end_list(self, key, info):
        pass
    
    def start_sorted_set(self, key, length, expiry, info):
        pass

    def zadd(self, key, score, member):
        self._out.write(self.dbstr() + self.encode_key(key) +
                        b' -> {' + self.encode_key(member) + b', score=' + self.encode_value(score) + b'}')
        self.newline()
    
    def end_sorted_set(self, key):
        pass

    def newline(self):
        self._out.write(b'\r\n')


def _unix_timestamp(dt):
     return calendar.timegm(dt.utctimetuple())


class ProtocolCallback(RdbCallback):
    def __init__(self, out, string_escape=None):
        super(ProtocolCallback, self).__init__(string_escape)
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
        self._out.write(codecs.encode("*%s\r\n" % len(args), 'ascii'))
        for arg in args:
            val = encodehelpers.apply_escape_bytes(arg, self._escape)
            self._out.write(codecs.encode("$%s\r\n" % len(val), 'ascii'))
            self._out.write(val + b"\r\n")

    def start_database(self, db_number):
        self.reset()
        self.select(db_number)

    # String handling

    def set(self, key, value, expiry, info):
        self.pre_expiry(key, expiry)
        self.emit(b'SET', key, value)
        self.post_expiry(key)

    # Hash handling

    def start_hash(self, key, length, expiry, info):
        self.pre_expiry(key, expiry)

    def hset(self, key, field, value):
        self.emit(b'HSET', key, field, value)

    def end_hash(self, key):
        self.post_expiry(key)

    # Set handling

    def start_set(self, key, cardinality, expiry, info):
        self.pre_expiry(key, expiry)

    def sadd(self, key, member):
        self.emit(b'SADD', key, member)

    def end_set(self, key):
        self.post_expiry(key)

    # List handling

    def start_list(self, key, expiry, info):
        self.pre_expiry(key, expiry)

    def rpush(self, key, value):
        self.emit(b'RPUSH', key, value)

    def end_list(self, key, info):
        self.post_expiry(key)

    # Sorted set handling

    def start_sorted_set(self, key, length, expiry, info):
        self.pre_expiry(key, expiry)

    def zadd(self, key, score, member):
        self.emit(b'ZADD', key, score, member)

    def end_sorted_set(self, key):
        self.post_expiry(key)

    # Other misc commands

    def select(self, db_number):
        self.emit(b'SELECT', db_number)

    def expireat(self, key, timestamp):
        self.emit(b'EXPIREAT', key, timestamp)
