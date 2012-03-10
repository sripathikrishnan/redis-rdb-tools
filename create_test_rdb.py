import redis
import random
import string

r = redis.StrictRedis()

def create_test_rdb() :
    clean_database()
    keys_with_expiry()
    integer_keys()
    uncompressible_string_keys()
    easily_compressible_string_key()
    zipmap_that_doesnt_compress()
    zipmap_that_compresses_easily()
    #dictionary()
    ziplist_that_compresses_easily()
    ziplist_that_doesnt_compress()
    ziplist_with_integers()
    #linkedlist()
    intset_16()
    intset_32()
    intset_64()
    regular_set()
    sorted_set_as_ziplist()
    regular_sorted_set()
    
def clean_database() :
    r.flushdb()

def keys_with_expiry() :
    pass

def integer_keys() :
    r.set(-123, "Negative 8 bit integer")
    r.set(125, "Positive 8 bit integer")
    r.set(0xABAB, "Positive 16 bit integer")
    r.set(-0x7325, "Negative 16 bit integer")
    r.set(0x0AEDD325, "Positive 32 bit integer")
    r.set(-0x0AEDD325, "Negative 32 bit integer")

def uncompressible_string_keys() :
    r.set(random_string(60, "length within 6 bits"), "Key length within 6 bits")
    r.set(random_string(16382, "length within 14 bits"), "Key length more than 6 bits but less than 14 bits")
    r.set(random_string(16386, "length within 32 bits"), "Key length more than 14 bits but less than 32")

def easily_compressible_string_key() :
    r.set("".join('a' for x in range(0, 200)), "Key that redis should compress easily")

def zipmap_that_compresses_easily() :
    r.hset("zimap_compresses_easily", "a", "aa")
    r.hset("zimap_compresses_easily", "aa", "aaaa")
    r.hset("zimap_compresses_easily", "aaaaa", "aaaaaaaaaaaaaa")
    
def zipmap_that_doesnt_compress() :
    r.hset("zimap_doesnt_compress", "MKD1G6", "2")
    r.hset("zimap_doesnt_compress", "YNNXK", "F7TI")

def dictionary() :
    num_entries = 1000
    for x in xrange(0, num_entries) :
        r.hset("force_dictionary", random_string(50, x), random_string(50, x + num_entries))

def ziplist_that_compresses_easily() :
    r.lpush("ziplist_compresses_easily", "aaaaaa")
    r.lpush("ziplist_compresses_easily", "aaaaaaaaaaaa")
    r.lpush("ziplist_compresses_easily", "aaaaaaaaaaaaaaaaaa")
    r.lpush("ziplist_compresses_easily", "aaaaaaaaaaaaaaaaaaaaaaaa")
    r.lpush("ziplist_compresses_easily", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    r.lpush("ziplist_compresses_easily", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    
def ziplist_that_doesnt_compress() :
    r.lpush("ziplist_doesnt_compress", "aj2410")
    r.lpush("ziplist_doesnt_compress", "cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344")

def ziplist_with_integers() :
    r.lpush("ziplist_with_integers", 63)
    r.lpush("ziplist_with_integers", 16380)
    r.lpush("ziplist_with_integers", 65535)
    r.lpush("ziplist_with_integers", 0x7fffffffffffffff)
    
def linkedlist() :
    num_entries = 1000
    for x in xrange(0, num_entries) :
        r.lpush("force_linkedlist", random_string(50, x))

def intset_16() :
    r.sadd("intset_16", 0xfffe)
    r.sadd("intset_16", 0xfffd)
    r.sadd("intset_16", 0xfffc)

def intset_32() :
    r.sadd("intset_32", 0xfffefffe)
    r.sadd("intset_32", 0xfffefffd)
    r.sadd("intset_32", 0xfffefffc)
    
def intset_64() :
    r.sadd("intset_64", 0x7ffefffefffefffe)
    r.sadd("intset_64", 0x7ffefffefffefffd)
    r.sadd("intset_64", 0x7ffefffefffefffc)

def regular_set() :
    r.sadd("regular_set", "alpha")
    r.sadd("regular_set", "beta")
    r.sadd("regular_set", "gamma")
    r.sadd("regular_set", "delta")
    r.sadd("regular_set", "phi")
    r.sadd("regular_set", "kappa")

def sorted_set_as_ziplist() :
    r.zadd("sorted_set_as_ziplist", 1, "8b6ba6718a786daefa69438148361901")
    r.zadd("sorted_set_as_ziplist", 2, "cb7a24bb7528f934b841b34c3a73e0c7")
    r.zadd("sorted_set_as_ziplist", 3, "523af537946b79c4f8369ed39ba78605")
    
def regular_sorted_set() :
    pass
    
def random_string(length, seed) :
    random.seed(seed)
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def main() :
    create_test_rdb()

if __name__ == '__main__' :
    main()        
