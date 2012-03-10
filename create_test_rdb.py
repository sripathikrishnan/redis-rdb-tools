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
    zip_map_that_doesnt_compress()
    zip_map_that_compresses_easily()
    dictionary()
        
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

def zip_map_that_compresses_easily() :
    r.hset("zimap_compresses_easily", "a", "aa")
    r.hset("zimap_compresses_easily", "aa", "aaaa")
    r.hset("zimap_compresses_easily", "aaaaa", "aaaaaaaaaaaaaa")
    
def zip_map_that_doesnt_compress() :
    r.hset("zimap_doesnt_compress", "MKD1G6", "2")
    r.hset("zimap_doesnt_compress", "YNNXK", "F7TI")

def dictionary() :
    num_entries = 1000
    for x in xrange(0, num_entries) :
        r.hset("force_dictionary", random_string(50, x), random_string(50, x + num_entries))
    
def random_string(length, seed) :
    random.seed(seed)
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def main() :
    create_test_rdb()

if __name__ == '__main__' :
    main()        
