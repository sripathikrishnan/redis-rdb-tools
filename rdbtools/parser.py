import struct
import io
import datetime
import re

from rdbtools.encodehelpers import STRING_ESCAPE_RAW, apply_escape_bytes, bval
from .compat import range, str2regexp
from .iowrapper import IOWrapper

try:
    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

try:
    import lzf
    HAS_PYTHON_LZF = True
except ImportError:
    HAS_PYTHON_LZF = False
    
REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 0x80
REDIS_RDB_64BITLEN = 0x81
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_MODULE_AUX = 247
REDIS_RDB_OPCODE_IDLE = 248
REDIS_RDB_OPCODE_FREQ = 249
REDIS_RDB_OPCODE_AUX = 250
REDIS_RDB_OPCODE_RESIZEDB = 251
REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
REDIS_RDB_OPCODE_EXPIRETIME = 253
REDIS_RDB_OPCODE_SELECTDB = 254
REDIS_RDB_OPCODE_EOF = 255

REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4
REDIS_RDB_TYPE_ZSET_2 = 5  # ZSET version 2 with doubles stored in binary.
REDIS_RDB_TYPE_MODULE = 6
REDIS_RDB_TYPE_MODULE_2 = 7
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
REDIS_RDB_TYPE_HASH_ZIPLIST = 13
REDIS_RDB_TYPE_LIST_QUICKLIST = 14
REDIS_RDB_TYPE_STREAM_LISTPACKS = 15

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3

REDIS_RDB_MODULE_OPCODE_EOF = 0   # End of module value.
REDIS_RDB_MODULE_OPCODE_SINT = 1
REDIS_RDB_MODULE_OPCODE_UINT = 2
REDIS_RDB_MODULE_OPCODE_FLOAT = 3
REDIS_RDB_MODULE_OPCODE_DOUBLE = 4
REDIS_RDB_MODULE_OPCODE_STRING = 5

DATA_TYPE_MAPPING = {
    0 : "string", 1 : "list", 2 : "set", 3 : "sortedset", 4 : "hash", 5 : "sortedset", 6 : "module", 7: "module",
    9 : "hash", 10 : "list", 11 : "set", 12 : "sortedset", 13 : "hash", 14 : "list", 15 : "stream"}

class RdbCallback(object):
    """
    A Callback to handle events as the Redis dump file is parsed.
    This callback provides a serial and fast access to the dump file.
    
    """
    def __init__(self, string_escape):
        if string_escape is None:
            self._escape = STRING_ESCAPE_RAW
        else:
            self._escape = string_escape

    def encode_key(self, key):
        """
        Escape a given key bytes with the instance chosen escape method.

        Key is not escaped if it contains only 'ASCII printable' bytes.
        """
        return apply_escape_bytes(key, self._escape, skip_printable=True)

    def encode_value(self, val):
        """Escape a given value bytes with the instance chosen escape method."""
        return apply_escape_bytes(val, self._escape)

    def start_rdb(self):
        """
        Called once we know we are dealing with a valid redis dump file
        
        """
        pass

    def aux_field(self, key, value):
        """"
        Called in the beginning of the RDB with various meta data fields such as:
        redis-ver, redis-bits, ctime, used-mem
        exists since redis 3.2 (RDB v7)

        """
        pass

    def start_database(self, db_number):
        """
        Called to indicate database the start of database `db_number` 
        
        Once a database starts, another database cannot start unless 
        the first one completes and then `end_database` method is called
        
        Typically, callbacks store the current database number in a class variable
        
        """
        pass

    def start_module(self, key, module_name, expiry, info):
        """
        Called to indicate start of a module key
        :param key: string. if key is None, this is module AUX data
        :param module_name: string
        :param expiry:
        :param info: is a dictionary containing additional information about this object.
        :return: boolean to indicate whatever to record the full buffer or not
        """
        return False

    def handle_module_data(self, key, opcode, data):
        pass

    def end_module(self, key, buffer_size, buffer=None):
        pass

    def db_size(self, db_size, expires_size):
        """
        Called per database before the keys, with the key count in the main dictioney and the total voletaile key count
        exists since redis 3.2 (RDB v7)

        """
        pass

    def set(self, key, value, expiry, info):
        """
        Callback to handle a key with a string value and an optional expiry
        
        `key` is the redis key
        `value` is a string or a number
        `expiry` is a datetime object. None and can be None
        `info` is a dictionary containing additional information about this object.
        
        """
        pass
    
    def start_hash(self, key, length, expiry, info):
        """Callback to handle the start of a hash
        
        `key` is the redis key
        `length` is the number of elements in this hash. 
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_hash`, the method `hset` will be called with this `key` exactly `length` times.
        After that, the `end_hash` method will be called.
        
        """
        pass
    
    def hset(self, key, field, value):
        """
        Callback to insert a field=value pair in an existing hash
        
        `key` is the redis key for this hash
        `field` is a string
        `value` is the value to store for this field
        
        """
        pass
    
    def end_hash(self, key):
        """
        Called when there are no more elements in the hash
        
        `key` is the redis key for the hash
        
        """
        pass
    
    def start_set(self, key, cardinality, expiry, info):
        """
        Callback to handle the start of a hash
        
        `key` is the redis key
        `cardinality` is the number of elements in this set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_set`, the  method `sadd` will be called with `key` exactly `cardinality` times
        After that, the `end_set` method will be called to indicate the end of the set.
        
        Note : This callback handles both Int Sets and Regular Sets
        
        """
        pass

    def sadd(self, key, member):
        """
        Callback to inser a new member to this set
        
        `key` is the redis key for this set
        `member` is the member to insert into this set
        
        """
        pass
    
    def end_set(self, key):
        """
        Called when there are no more elements in this set 
        
        `key` the redis key for this set
        
        """
        pass
    
    def start_list(self, key, expiry, info):
        """
        Callback to handle the start of a list
        
        `key` is the redis key for this list
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_list`, the method `rpush` will be called with `key` exactly `length` times
        After that, the `end_list` method will be called to indicate the end of the list
        
        Note : This callback handles both Zip Lists and Linked Lists.
        
        """
        pass
    
    def rpush(self, key, value):
        """
        Callback to insert a new value into this list
        
        `key` is the redis key for this list
        `value` is the value to be inserted
        
        Elements must be inserted to the end (i.e. tail) of the existing list.
        
        """
        pass
    
    def end_list(self, key, info):
        """
        Called when there are no more elements in this list
        
        `key` the redis key for this list
        `info` is a dictionary containing additional information about this object that wasn't known in start_list.

        """
        pass
    
    def start_sorted_set(self, key, length, expiry, info):
        """
        Callback to handle the start of a sorted set
        
        `key` is the redis key for this sorted
        `length` is the number of elements in this sorted set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_sorted_set`, the method `zadd` will be called with `key` exactly `length` times. 
        Also, `zadd` will be called in a sorted order, so as to preserve the ordering of this sorted set.
        After that, the `end_sorted_set` method will be called to indicate the end of this sorted set
        
        Note : This callback handles sorted sets in that are stored as ziplists or skiplists
        
        """
        pass
    
    def zadd(self, key, score, member):
        """Callback to insert a new value into this sorted set
        
        `key` is the redis key for this sorted set
        `score` is the score for this `value`
        `value` is the element being inserted
        """
        pass
    
    def end_sorted_set(self, key):
        """
        Called when there are no more elements in this sorted set
        
        `key` is the redis key for this sorted set
        
        """
        pass

    def start_stream(self, key, listpacks_count, expiry, info):
        """Callback to handle the start of a stream

        `key` is the redis key
        `listpacks_count` is the number of listpacks in this stream.
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.

        After `start_stream`, the method `stream_listpack` will be called with this `key` exactly `listpacks_count` times.
        After that, the `end_stream` method will be called.

        """
        pass

    def stream_listpack(self, key, entry_id, data):
        """
        Callback to insert a listpack into a stream

        `key` is the redis key for this stream
        `entry_id` is binary (bigendian)
        `data` the bytes of the listpack

        """
        pass

    def end_stream(self, key, items, last_entry_id, cgroups):
        """
        Called when there is no more data in the stream

        `key` is the redis key for the stream
        `items` is the total number of items in the stream
        `last_entry_id` is in "<millisecondsTime>-<sequenceNumber>" format
        `cgroups` is an array of consumer group metadata

        """
        pass

    def end_database(self, db_number):
        """
        Called when the current database ends
        
        After `end_database`, one of the methods are called - 
        1) `start_database` with a new database number
            OR
        2) `end_rdb` to indicate we have reached the end of the file
        
        """
        pass
    
    def end_rdb(self):
        """Called to indicate we have completed parsing of the dump file"""
        pass

class RdbParser(object):
    """
    A Parser for Redis RDB Files
    
    This class is similar in spirit to a SAX parser for XML files.
    The dump file is parsed sequentially. As and when objects are discovered,
    appropriate methods in the callback are called. 
        
    Typical usage :
        callback = MyRdbCallback() # Typically a subclass of RdbCallback
        parser = RdbParser(callback)
        parser.parse('/var/redis/6379/dump.rdb')
    
    filter is a dictionary with the following keys
        {"dbs" : [0, 1], "keys" : "foo.*", "types" : ["hash", "set", "sortedset", "list", "string"]}
        
        If filter is None, results will not be filtered
        If dbs, keys or types is None or Empty, no filtering will be done on that axis
    """
    def __init__(self, callback, filters = None) :
        """
            `callback` is the object that will receive parse events
        """
        self._callback = callback
        self._key = None
        self._expiry = None
        self._idle = None
        self._freq = None
        self.init_filter(filters)
        self._rdb_version = 0

    def parse(self, filename):
        """
        Parse a redis rdb dump file, and call methods in the 
        callback object during the parsing operation.
        """
        self.parse_fd(open(filename, "rb"))

    def parse_fd(self, fd):
        with fd as f:
            self.verify_magic_string(f.read(5))
            self.verify_version(f.read(4))
            self._callback.start_rdb()

            is_first_database = True
            db_number = 0
            while True :
                self._expiry = None
                self._idle = None
                self._freq = None
                data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS :
                    self._expiry = read_milliseconds_time(f)
                    data_type = read_unsigned_char(f)
                elif data_type == REDIS_RDB_OPCODE_EXPIRETIME :
                    self._expiry = to_datetime(read_unsigned_int(f) * 1000000)
                    data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_IDLE:
                    self._idle = self.read_length(f)
                    data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_FREQ:
                    self._freq = read_unsigned_char(f)
                    data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_SELECTDB :
                    if not is_first_database :
                        self._callback.end_database(db_number)
                    is_first_database = False
                    db_number = self.read_length(f)
                    self._callback.start_database(db_number)
                    continue

                if data_type == REDIS_RDB_OPCODE_AUX:
                    aux_key = self.read_string(f)
                    aux_val = self.read_string(f)
                    ret = self._callback.aux_field(aux_key, aux_val)
                    if ret:
                        break  # TODO: make all callbacks return abort flag
                    continue

                if data_type == REDIS_RDB_OPCODE_RESIZEDB:
                    db_size = self.read_length(f)
                    expire_size = self.read_length(f)
                    self._callback.db_size(db_size, expire_size)
                    continue

                if data_type == REDIS_RDB_OPCODE_MODULE_AUX:
                    self.read_module(f)
                    continue

                if data_type == REDIS_RDB_OPCODE_EOF:
                    self._callback.end_database(db_number)
                    self._callback.end_rdb()
                    if self._rdb_version >= 5:
                        f.read(8)
                    break

                if self.matches_filter(db_number):
                    self._key = self.read_string(f)
                    if self.matches_filter(db_number, self._key, data_type):
                        self.read_object(f, data_type)
                    else:
                        self.skip_object(f, data_type)
                else :
                    self.skip_key_and_object(f, data_type)
                self._key = None


    def read_length_with_encoding(self, f):
        length = 0
        is_encoded = False
        bytes = []
        bytes.append(read_unsigned_char(f))
        enc_type = (bytes[0] & 0xC0) >> 6
        if enc_type == REDIS_RDB_ENCVAL:
            is_encoded = True
            length = bytes[0] & 0x3F
        elif enc_type == REDIS_RDB_6BITLEN:
            length = bytes[0] & 0x3F
        elif enc_type == REDIS_RDB_14BITLEN:
            bytes.append(read_unsigned_char(f))
            length = ((bytes[0] & 0x3F) << 8) | bytes[1]
        elif bytes[0] == REDIS_RDB_32BITLEN:
            length = read_unsigned_int_be(f)
        elif bytes[0] == REDIS_RDB_64BITLEN:
            length = read_unsigned_long_be(f)
        else:
            raise Exception('read_length_with_encoding', "Invalid string encoding %s (encoding byte 0x%X)" % (enc_type, bytes[0]))
        return (length, is_encoded)

    def read_length(self, f) :
        return self.read_length_with_encoding(f)[0]

    def read_string(self, f) :
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        val = None
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                val = read_signed_char(f)
            elif length == REDIS_RDB_ENC_INT16 :
                val = read_signed_short(f)
            elif length == REDIS_RDB_ENC_INT32 :
                val = read_signed_int(f)
            elif length == REDIS_RDB_ENC_LZF :
                clen = self.read_length(f)
                l = self.read_length(f)
                val = self.lzf_decompress(f.read(clen), l)
            else:
                raise Exception('read_string', "Invalid string encoding %s"%(length))
        else :
            val = f.read(length)
        return val

    def read_float(self, f):
        dbl_length = read_unsigned_char(f)
        if dbl_length == 253:
            return float('nan')
        elif dbl_length == 254:
            return float('inf')
        elif dbl_length == 255:
            return float('-inf')
        data = f.read(dbl_length)
        if isinstance(data, str):
            return float(data)
        return data # bug?

    # Read an object for the stream
    # f is the redis file 
    # enc_type is the type of object
    def read_object(self, f, enc_type) :
        if enc_type == REDIS_RDB_TYPE_STRING :
            val = self.read_string(f)
            self._callback.set(self._key, val, self._expiry, info={'encoding':'string','idle':self._idle,'freq':self._freq})
        elif enc_type == REDIS_RDB_TYPE_LIST :
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a list from it
            # The lists are in order i.e. the first string is the head, 
            # and the last string is the tail of the list
            length = self.read_length(f)
            self._callback.start_list(self._key, self._expiry, info={'encoding':'linkedlist','idle':self._idle,'freq':self._freq})
            for count in range(0, length) :
                val = self.read_string(f)
                self._callback.rpush(self._key, val)
            self._callback.end_list(self._key, info={'encoding':'linkedlist' })
        elif enc_type == REDIS_RDB_TYPE_SET:
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a set from it
            # Note that the order of strings is non-deterministic
            length = self.read_length(f)
            self._callback.start_set(self._key, length, self._expiry, info={'encoding':'hashtable','idle':self._idle,'freq':self._freq})
            for count in range(0, length):
                val = self.read_string(f)
                self._callback.sadd(self._key, val)
            self._callback.end_set(self._key)
        elif enc_type == REDIS_RDB_TYPE_ZSET or enc_type == REDIS_RDB_TYPE_ZSET_2 :
            length = self.read_length(f)
            self._callback.start_sorted_set(self._key, length, self._expiry, info={'encoding':'skiplist','idle':self._idle,'freq':self._freq})
            for count in range(0, length):
                val = self.read_string(f)
                score = read_binary_double(f) if enc_type == REDIS_RDB_TYPE_ZSET_2 else self.read_float(f)
                self._callback.zadd(self._key, score, val)
            self._callback.end_sorted_set(self._key)
        elif enc_type == REDIS_RDB_TYPE_HASH:
            length = self.read_length(f)
            self._callback.start_hash(self._key, length, self._expiry, info={'encoding':'hashtable','idle':self._idle,'freq':self._freq})
            for count in range(0, length):
                field = self.read_string(f)
                value = self.read_string(f)
                self._callback.hset(self._key, field, value)
            self._callback.end_hash(self._key)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP:
            self.read_zipmap(f)
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST:
            self.read_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET:
            self.read_intset(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST:
            self.read_zset_from_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST:
            self.read_hash_from_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_LIST_QUICKLIST:
            self.read_list_from_quicklist(f)
        elif enc_type == REDIS_RDB_TYPE_MODULE:
            raise Exception('read_object', 'Unable to read Redis Modules RDB objects (key %s)' % self._key)
        elif enc_type == REDIS_RDB_TYPE_MODULE_2:
            self.read_module(f)
        elif enc_type == REDIS_RDB_TYPE_STREAM_LISTPACKS:
            self.read_stream(f)
        else:
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))

    def skip_key_and_object(self, f, data_type):
        self.skip_string(f)
        self.skip_object(f, data_type)

    def skip_string(self, f):
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        bytes_to_skip = 0
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                bytes_to_skip = 1
            elif length == REDIS_RDB_ENC_INT16 :
                bytes_to_skip = 2
            elif length == REDIS_RDB_ENC_INT32 :
                bytes_to_skip = 4
            elif length == REDIS_RDB_ENC_LZF :
                clen = self.read_length(f)
                l = self.read_length(f)
                bytes_to_skip = clen
        else :
            bytes_to_skip = length
        
        skip(f, bytes_to_skip)
        
    def skip_float(self, f):
        dbl_length = read_unsigned_char(f)
        if dbl_length < 253:
            skip(f, dbl_length)
        
    def skip_binary_double(self, f):
        skip(f, 8)

    def skip_object(self, f, enc_type):
        skip_strings = 0
        if enc_type == REDIS_RDB_TYPE_STRING :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_SET :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET or enc_type == REDIS_RDB_TYPE_ZSET_2 :
            length = self.read_length(f)
            for x in range(length):
                self.skip_string(f)
                self.skip_binary_double(f) if enc_type == REDIS_RDB_TYPE_ZSET_2 else self.skip_float(f)
        elif enc_type == REDIS_RDB_TYPE_HASH :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_QUICKLIST:
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_MODULE:
            raise Exception('skip_object', 'Unable to skip Redis Modules RDB objects (key %s)' % self._key)
        elif enc_type == REDIS_RDB_TYPE_MODULE_2:
            self.skip_module(f)
        elif enc_type == REDIS_RDB_TYPE_STREAM_LISTPACKS:
            self.skip_stream(f)
        else:
            raise Exception('skip_object', 'Invalid object type %d for key %s' % (enc_type, self._key))
        for x in range(0, skip_strings):
            self.skip_string(f)


    def read_intset(self, f) :
        raw_string = self.read_string(f)
        buff = BytesIO(raw_string)
        encoding = read_unsigned_int(buff)
        num_entries = read_unsigned_int(buff)
        self._callback.start_set(self._key, num_entries, self._expiry, info={'encoding':'intset', 'sizeof_value':len(raw_string),'idle':self._idle,'freq':self._freq})
        for x in range(0, num_entries) :
            if encoding == 8 :
                entry = read_signed_long(buff)
            elif encoding == 4 :
                entry = read_signed_int(buff)
            elif encoding == 2 :
                entry = read_signed_short(buff)
            else :
                raise Exception('read_intset', 'Invalid encoding %d for key %s' % (encoding, self._key))
            self._callback.sadd(self._key, entry)
        self._callback.end_set(self._key)

    def read_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = BytesIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        self._callback.start_list(self._key, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string),'idle':self._idle,'freq':self._freq})
        for x in range(0, num_entries) :
            val = self.read_ziplist_entry(buff)
            self._callback.rpush(self._key, val)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_list(self._key, info={'encoding':'ziplist'})

    def read_list_from_quicklist(self, f):
        count = self.read_length(f)
        total_size = 0
        self._callback.start_list(self._key, self._expiry, info={'encoding': 'quicklist', 'zips': count,'idle':self._idle,'freq':self._freq})
        for i in range(0, count):
            raw_string = self.read_string(f)
            total_size += len(raw_string)
            buff = BytesIO(raw_string)
            zlbytes = read_unsigned_int(buff)
            tail_offset = read_unsigned_int(buff)
            num_entries = read_unsigned_short(buff)
            for x in range(0, num_entries):
                self._callback.rpush(self._key, self.read_ziplist_entry(buff))
            zlist_end = read_unsigned_char(buff)
            if zlist_end != 255:
                raise Exception('read_quicklist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_list(self._key, info={'encoding': 'quicklist', 'zips': count, 'sizeof_value': total_size})

    def read_zset_from_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = BytesIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_zset_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries // 2
        self._callback.start_sorted_set(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string),'idle':self._idle,'freq':self._freq})
        for x in range(0, num_entries) :
            member = self.read_ziplist_entry(buff)
            score = self.read_ziplist_entry(buff)
            if isinstance(score, bytes) :
                score = float(score)
            self._callback.zadd(self._key, score, member)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_zset_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_sorted_set(self._key)

    def read_hash_from_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = BytesIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_hash_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries // 2
        self._callback.start_hash(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string),'idle':self._idle,'freq':self._freq})
        for x in range(0, num_entries) :
            field = self.read_ziplist_entry(buff)
            value = self.read_ziplist_entry(buff)
            self._callback.hset(self._key, field, value)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_hash_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_hash(self._key)
    
    
    def read_ziplist_entry(self, f) :
        length = 0
        value = None
        prev_length = read_unsigned_char(f)
        if prev_length == 254 :
            prev_length = read_unsigned_int(f)
        entry_header = read_unsigned_char(f)
        if (entry_header >> 6) == 0 :
            length = entry_header & 0x3F
            value = f.read(length)
        elif (entry_header >> 6) == 1 :
            length = ((entry_header & 0x3F) << 8) | read_unsigned_char(f)
            value = f.read(length)
        elif (entry_header >> 6) == 2 :
            length = read_unsigned_int_be(f)
            value = f.read(length)
        elif (entry_header >> 4) == 12 :
            value = read_signed_short(f)
        elif (entry_header >> 4) == 13 :
            value = read_signed_int(f)
        elif (entry_header >> 4) == 14 :
            value = read_signed_long(f)
        elif (entry_header == 240) :
            value = read_24bit_signed_number(f)
        elif (entry_header == 254) :
            value = read_signed_char(f)
        elif (entry_header >= 241 and entry_header <= 253) :
            value = entry_header - 241
        else :
            raise Exception('read_ziplist_entry', 'Invalid entry_header %d for key %s' % (entry_header, self._key))
        return value
        
    def read_zipmap(self, f) :
        raw_string = self.read_string(f)
        buff = io.BytesIO(bytearray(raw_string))
        num_entries = read_unsigned_char(buff)
        self._callback.start_hash(self._key, num_entries, self._expiry, info={'encoding':'zipmap', 'sizeof_value':len(raw_string),'idle':self._idle,'freq':self._freq})
        while True :
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                break
            key = buff.read(next_length)
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                raise Exception('read_zip_map', 'Unexepcted end of zip map for key %s' % self._key)        
            free = read_unsigned_char(buff)
            value = buff.read(next_length)
            try:
                value = int(value)
            except ValueError:
                pass
            
            skip(buff, free)
            self._callback.hset(self._key, key, value)
        self._callback.end_hash(self._key)

    def read_zipmap_next_length(self, f) :
        num = read_unsigned_char(f)
        if num < 254:
            return num
        elif num == 254:
            return read_unsigned_int(f)
        else:
            return None

    def skip_module(self, f):
        self.read_length_with_encoding(f) # read module id first
        opcode = self.read_length(f)
        while opcode != REDIS_RDB_MODULE_OPCODE_EOF:
            if opcode == REDIS_RDB_MODULE_OPCODE_SINT or opcode == REDIS_RDB_MODULE_OPCODE_UINT:
                self.read_length(f)
            elif opcode == REDIS_RDB_MODULE_OPCODE_FLOAT:
                read_binary_float(f)
            elif opcode == REDIS_RDB_MODULE_OPCODE_DOUBLE:
                read_binary_double(f)
            elif opcode == REDIS_RDB_MODULE_OPCODE_STRING:
                self.skip_string(f)
            else:
                raise Exception("Unknown module opcode %s" % opcode)
            # read the next item in the module data type
            opcode = self.read_length(f)

    def read_module(self, f):
        # this method is based on the actual implementation in redis (src/rdb.c:rdbLoadObject)
        iowrapper = IOWrapper(f)
        iowrapper.start_recording_size()
        iowrapper.start_recording()
        length, encoding = self.read_length_with_encoding(iowrapper)
        record_buffer = self._callback.start_module(self._key, self._decode_module_id(length), self._expiry, info={'idle':self._idle, 'freq':self._freq})

        if not record_buffer:
            iowrapper.stop_recording()

        opcode = self.read_length(iowrapper)
        while opcode != REDIS_RDB_MODULE_OPCODE_EOF:
            if opcode == REDIS_RDB_MODULE_OPCODE_SINT or opcode == REDIS_RDB_MODULE_OPCODE_UINT:
                data = self.read_length(iowrapper)
            elif opcode == REDIS_RDB_MODULE_OPCODE_FLOAT:
                data = read_binary_float(iowrapper)
            elif opcode == REDIS_RDB_MODULE_OPCODE_DOUBLE:
                data = read_binary_double(iowrapper)
            elif opcode == REDIS_RDB_MODULE_OPCODE_STRING:
                data = self.read_string(iowrapper)
            else:
                raise Exception("Unknown module opcode %s" % opcode)
            self._callback.handle_module_data(self._key, opcode, data)
            # read the next item in the module data type
            opcode = self.read_length(iowrapper)

        buffer = None
        if record_buffer:
            # prepand the buffer with REDIS_RDB_TYPE_MODULE_2 type
            buffer = struct.pack('B', REDIS_RDB_TYPE_MODULE_2) + iowrapper.get_recorded_buffer()
            iowrapper.stop_recording()
        self._callback.end_module(self._key, buffer_size=iowrapper.get_recorded_size(), buffer=buffer)

    def skip_stream(self, f):
        listpacks = self.read_length(f)
        for _lp in range(listpacks):
            self.skip_string(f)
            self.skip_string(f)
        self.read_length(f)
        self.read_length(f)
        self.read_length(f)
        cgroups = self.read_length(f)
        for _cg in range(cgroups):
            self.skip_string(f)
            self.read_length(f)
            self.read_length(f)
            pending = self.read_length(f)
            for _pel in range(pending):
                f.read(16)
                f.read(8)
                self.read_length(f)
            consumers = self.read_length(f)
            for _c in range(consumers):
                self.skip_string(f)
                f.read(8)
                pending = self.read_length(f)
                f.read(pending*16)

    def read_stream(self, f):
        listpacks = self.read_length(f)
        self._callback.start_stream(self._key, listpacks, self._expiry,
                                    info={'encoding': 'listpack', 'idle': self._idle, 'freq': self._freq})
        for _lp in range(listpacks):
            self._callback.stream_listpack(self._key, self.read_string(f), self.read_string(f))
        items = self.read_length(f)
        last_entry_id = "%s-%s" % (self.read_length(f), self.read_length(f))
        cgroups = self.read_length(f)
        cgroups_data = []
        for _cg in range(cgroups):
            cgname = self.read_string(f)
            last_cg_entry_id = "%s-%s" % (self.read_length(f), self.read_length(f))
            pending = self.read_length(f)
            group_pending_entries = []
            for _pel in range(pending):
                eid = f.read(16)
                delivery_time = read_milliseconds_time(f)
                delivery_count = self.read_length(f)
                group_pending_entries.append({'id': eid,
                                              'delivery_time': delivery_time,
                                              'delivery_count': delivery_count})
            consumers = self.read_length(f)
            consumers_data = []
            for _c in range(consumers):
                cname = self.read_string(f)
                seen_time = read_milliseconds_time(f)
                pending = self.read_length(f)
                consumer_pending_entries = []
                for _pel in range( pending):
                    eid = f.read(16)
                    consumer_pending_entries.append({'id': eid})
                consumers_data.append({'name': cname,
                                       'seen_time': seen_time,
                                       'pending': consumer_pending_entries})
            cgroups_data.append({'name': cgname,
                                 'last_entry_id': last_cg_entry_id,
                                 'pending': group_pending_entries,
                                 'consumers': consumers_data})
        self._callback.end_stream(self._key, items, last_entry_id, cgroups_data)

    charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

    def _decode_module_id(self, module_id):
        """
        decode module id to string
        based on @antirez moduleTypeNameByID function from redis/src/module.c
        :param module_id: 64bit integer
        :return: string
        """
        name = [''] * 9
        module_id >>= 10
        for i in reversed(range(9)):
            name[i] = self.charset[module_id & 63]
            module_id >>= 6
        return ''.join(name)

    def verify_magic_string(self, magic_string) :
        if magic_string != b'REDIS' :
            raise Exception('verify_magic_string', 'Invalid File Format')

    def verify_version(self, version_str) :
        version = int(version_str)
        if version < 1 or version > 9:
            raise Exception('verify_version', 'Invalid RDB version number %d' % version)
        self._rdb_version = version

    def init_filter(self, filters):
        self._filters = {}
        if not filters:
            filters={}

        if not 'dbs' in filters:
            self._filters['dbs'] = None
        elif isinstance(filters['dbs'], int):
            self._filters['dbs'] = (filters['dbs'], )
        elif isinstance(filters['dbs'], list):
            self._filters['dbs'] = [int(x) for x in filters['dbs']]
        else:
            raise Exception('init_filter', 'invalid value for dbs in filter %s' %filters['dbs'])
        
        if not ('keys' in filters and filters['keys']):
            self._filters['keys'] = re.compile(b".*")
        else:
            self._filters['keys'] = str2regexp(filters['keys'])
        
        if not ('not_keys' in filters and filters['not_keys']):
            self._filters['not_keys'] = None
        else:
            self._filters['not_keys'] = str2regexp(filters['not_keys'])

        if 'types' in filters:
            if isinstance(filters['types'], bytes):
                self._filters['types'] = (filters['types'], )
            elif isinstance(filters['types'], list):
                self._filters['types'] = [str(x) for x in filters['types']]
            else:
                raise Exception('init_filter', 'invalid value for types in filter %s' %filters['types'])
        
    def matches_filter(self, db_number, key=None, data_type=None):

        if isinstance(key, bytes):
            key_to_match = key
        elif isinstance(key, str): # bytes key in python2
            key_to_match = key
        else:
            key_to_match = str(key).encode('utf-8')

        if self._filters['dbs'] and (not db_number in self._filters['dbs']):
            return False
        if key and self._filters['not_keys'] and (self._filters['not_keys'].match(key_to_match)):
            return False
        if key and (not self._filters['keys'].match(key_to_match)):
            return False

        if data_type is not None and 'types' in self._filters and (not self.get_logical_type(data_type) in self._filters['types']):
            return False
        return True
    
    def get_logical_type(self, data_type):
        return DATA_TYPE_MAPPING[data_type]
        
    def lzf_decompress(self, compressed, expected_length):
        if HAS_PYTHON_LZF:
            return lzf.decompress(compressed, expected_length)
        else:
            in_stream = bytearray(compressed)
            in_len = len(in_stream)
            in_index = 0
            out_stream = bytearray()
            out_index = 0

            while in_index < in_len :
                ctrl = in_stream[in_index]
                if not isinstance(ctrl, int) :
                    raise Exception('lzf_decompress', 'ctrl should be a number %s for key %s' % (str(ctrl), self._key))
                in_index = in_index + 1
                if ctrl < 32 :
                    for x in range(0, ctrl + 1) :
                        out_stream.append(in_stream[in_index])
                        #sys.stdout.write(chr(in_stream[in_index]))
                        in_index = in_index + 1
                        out_index = out_index + 1
                else :
                    length = ctrl >> 5
                    if length == 7 :
                        length = length + in_stream[in_index]
                        in_index = in_index + 1

                    ref = out_index - ((ctrl & 0x1f) << 8) - in_stream[in_index] - 1
                    in_index = in_index + 1
                    for x in range(0, length + 2) :
                        out_stream.append(out_stream[ref])
                        ref = ref + 1
                        out_index = out_index + 1
            if len(out_stream) != expected_length :
                raise Exception('lzf_decompress', 'Expected lengths do not match %d != %d for key %s' % (len(out_stream), expected_length, self._key))
            return bytes(out_stream)

def skip(f, free):
    if free :
        f.read(free)

def to_datetime(usecs_since_epoch):
    seconds_since_epoch = usecs_since_epoch // 1000000
    if seconds_since_epoch > 221925052800 :
        seconds_since_epoch = 221925052800
    useconds = usecs_since_epoch % 1000000
    dt = datetime.datetime.utcfromtimestamp(seconds_since_epoch)
    delta = datetime.timedelta(microseconds = useconds)
    return dt + delta
    
def read_signed_char(f) :
    return struct.unpack('b', f.read(1))[0]
    
def read_unsigned_char(f) :
    return struct.unpack('B', f.read(1))[0]

def read_signed_short(f) :
    return struct.unpack('h', f.read(2))[0]
        
def read_unsigned_short(f) :
    return struct.unpack('H', f.read(2))[0]

def read_signed_int(f) :
    return struct.unpack('i', f.read(4))[0]
    
def read_unsigned_int(f) :
    return struct.unpack('I', f.read(4))[0]

def read_unsigned_int_be(f):
    return struct.unpack('>I', f.read(4))[0]

def read_24bit_signed_number(f):
    s = b'0' + f.read(3)
    num = struct.unpack('i', s)[0]
    return num >> 8
    
def read_signed_long(f) :
    return struct.unpack('q', f.read(8))[0]
    
def read_unsigned_long(f) :
    return struct.unpack('Q', f.read(8))[0]
    
def read_milliseconds_time(f) :
    return to_datetime(read_unsigned_long(f) * 1000)

def read_unsigned_long_be(f) :
    return struct.unpack('>Q', f.read(8))[0]

def read_binary_double(f) :
    return struct.unpack('d', f.read(8))[0]

def read_binary_float(f) :
    return struct.unpack('f', f.read(4))[0]

def string_as_hexcode(string) :
    for s in string :
        if isinstance(s, int) :
            print(hex(s))
        else :
            print(hex(ord(s)))


class DebugCallback(RdbCallback) :
    def start_rdb(self):
        print('[')
    
    def aux_field(self, key, value):
        print('aux:[%s:%s]' % (key, value))

    def start_database(self, db_number):
        print('{')

    def db_size(self, db_size, expires_size):
        print('db_size: %s, expires_size %s' % (db_size, expires_size))
    
    def set(self, key, value, expiry):
        print('"%s" : "%s"' % (str(key), str(value)))
    
    def start_hash(self, key, length, expiry):
        print('"%s" : {' % str(key))
        pass
    
    def hset(self, key, field, value):
        print('"%s" : "%s"' % (str(field), str(value)))
    
    def end_hash(self, key):
        print('}')
    
    def start_set(self, key, cardinality, expiry):
        print('"%s" : [' % str(key))

    def sadd(self, key, member):
        print('"%s"' % str(member))
    
    def end_set(self, key):
        print(']')
    
    def start_list(self, key, expiry, info):
        print('"%s" : [' % str(key))
    
    def rpush(self, key, value) :
        print('"%s"' % str(value))
    
    def end_list(self, key, info):
        print(']')
    
    def start_sorted_set(self, key, length, expiry):
        print('"%s" : {' % str(key))
    
    def zadd(self, key, score, member):
        print('"%s" : "%s"' % (str(member), str(score)))
    
    def end_sorted_set(self, key):
        print('}')
    
    def end_database(self, db_number):
        print('}')
    
    def end_rdb(self):
        print(']')
