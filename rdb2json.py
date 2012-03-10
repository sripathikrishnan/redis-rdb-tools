import struct

REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 2
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
REDIS_RDB_OPCODE_EXPIRETIME = 253
REDIS_RDB_OPCODE_SELECTDB = 254
REDIS_RDB_OPCODE_EOF = 255

REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4

# Object types for encoded objects
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3

def parse_rdb(filename) :
    with open(filename, "rb") as f:
        verify_magic_string(f.read(5))
        verify_version(f.read(4))
        
        db_number = 0
        while True :
            expires = 0
            data_type = read_unsigned_char(f)
            
            if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS :
                expires = read_unsigned_int(f)
                data_type = read_unsigned_char(f)
            elif data_type == REDIS_RDB_OPCODE_EXPIRETIME :
                expires = read_unsigned_int(f) * 1000
                data_type = read_unsigned_char(f)
            
            if data_type == REDIS_RDB_OPCODE_SELECTDB :
                db_number = read_length(f)[0]
                continue
            if data_type == REDIS_RDB_OPCODE_EOF :
                break
            
            key = read_string(f)
            print("key = %s" % key)
            print("data type = %d" % data_type)
            value = read_object(f, data_type)
            print("value = %s" % value)

def read_object(f, enc_type) :
    val = None
    if enc_type == REDIS_RDB_TYPE_STRING :
        val = read_string(f)
    elif enc_type == REDIS_RDB_TYPE_LIST or enc_type == REDIS_RDB_TYPE_SET :
        length = read_length(f)[0]
        val = []
        for count in xrange(0, length) :
            val.append(read_string(f))
    elif enc_type == REDIS_RDB_TYPE_ZSET :
        val = []
        length = read_length(f)[0]
        for count in xrange(0, length) :
            val.append(read_string(f))
            dbl_length = read_unsigned_char(f)
            f.read(dbl_length)
    elif enc_type == REDIS_RDB_TYPE_HASH :
        length = read_length(f)[0]
        val = {}
        for count in xrange(0, length) :
            key = read_string(f)
            value = read_string(f)
            val[key] = value
    elif (enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP or enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST
            or enc_type == REDIS_RDB_TYPE_SET_INTSET or enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST) :
        val = read_string(f)
    else :
        raise Exception('read_object', 'Invalid object type %d' % enc_type)
        
    return val

def read_string(f) :
    tup = read_length(f)
    length = tup[0]
    is_encoded = tup[1]
    val = None
    if is_encoded :
        if length == REDIS_RDB_ENC_INT8 :
            val = read_signed_char(f)
        elif length == REDIS_RDB_ENC_INT16 :
            bytes = bytearray(f.read(2))
            val = bytes[0] | (bytes[1] << 8)
        elif length == REDIS_RDB_ENC_INT32 :
            bytes = bytearray(f.read(4))
            val = bytes[0]|(bytes[1]<<8)|(bytes[2]<<16)|(bytes[3]<<24);
        elif length == REDIS_RDB_ENC_LZF :
            clen = read_length(f)[0]
            l = read_length(f)[0]
            f.read(clen)
            val = "Encoded string of length %d" % clen
    else :
        val = f.read(length)
    
    return val
    
def read_length(f) :
    length = 0
    is_encoded = False
    
    bytes = []
    bytes.append(read_unsigned_char(f))
    enc_type = (bytes[0] & 0xC0) >> 6
    if enc_type == REDIS_RDB_ENCVAL :
        is_encoded = True
        length = bytes[0] & 0x3F
    elif enc_type == REDIS_RDB_6BITLEN :
        length = bytes[0] & 0x3F
    elif enc_type == REDIS_RDB_14BITLEN :
        bytes.append(read_unsigned_char(f))
        length = ((bytes[0]&0x3F)<<8)|bytes[1]
    else :
        length = ntohl(f)
    
    return (length, is_encoded)

#TODO revisit this method
def ntohl(f) :
    read_unsigned_int(f)

def read_signed_char(f) :
    return struct.unpack('b', f.read(1))[0]
    
def read_unsigned_char(f) :
    return struct.unpack('B', f.read(1))[0]
    
def read_unsigned_int(f) :
    return struct.unpack('i', f.read(4))[0]

def verify_magic_string(magic_string) :
    if magic_string != 'REDIS' :
        raise Exception('verify_magic_string', 'Invalid File Format')

def verify_version(version_str) :
    version = int(version_str)
    if version < 1 or version > 3 : 
        raise Exception('verify_version', 'Invalid RDB version number %d' % version)

def main() :
    parse_rdb("dump.rdb")

if __name__ == '__main__' :
    main()

