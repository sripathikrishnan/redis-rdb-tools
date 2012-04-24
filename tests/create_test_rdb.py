import redis
import random
import string
import shutil
import os

r = redis.StrictRedis()
r2 = redis.StrictRedis(db=2)

def create_test_rdbs(path_to_redis_dump, dump_folder) :
    clean_database()
    tests = (
#                empty_database,
#                multiple_databases,
#                keys_with_expiry, 
#                integer_keys, 
#                uncompressible_string_keys, 
#                easily_compressible_string_key, 
#                zipmap_that_doesnt_compress, 
#                zipmap_that_compresses_easily, 
#                zipmap_with_big_values,
#                dictionary, 
#                ziplist_that_compresses_easily, 
#                ziplist_that_doesnt_compress, 
                ziplist_with_integers, 
#                linkedlist, 
#                intset_16, 
#                intset_32, 
#                intset_64, 
#                regular_set, 
#                sorted_set_as_ziplist, 
#                regular_sorted_set
            )
    for t in tests :
        create_rdb_file(t, path_to_redis_dump, dump_folder)

def create_rdb_file(test, path_to_rdb, dump_folder):
    clean_database()
    test()
    save_database()
    file_name = "%s.rdb" % test.__name__
    shutil.copy(path_to_rdb, os.path.join(dump_folder, file_name))
    
def clean_database() :
    r.flushall()

def save_database() :
    r.save()
    
def empty_database() :
    pass

def keys_with_expiry() :
    r.set("expires_ms_precision", "2022-12-25 10:11:12.573 UTC")
    r.execute_command('PEXPIREAT', "expires_ms_precision", 1671963072573)

def multiple_databases() :
    r.set("key_in_zeroth_database", "zero")
    r2.set("key_in_second_database", "second")
    
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
    r.hset("zipmap_compresses_easily", "a", "aa")
    r.hset("zipmap_compresses_easily", "aa", "aaaa")
    r.hset("zipmap_compresses_easily", "aaaaa", "aaaaaaaaaaaaaa")
    
def zipmap_that_doesnt_compress() :
    r.hset("zimap_doesnt_compress", "MKD1G6", "2")
    r.hset("zimap_doesnt_compress", "YNNXK", "F7TI")

def zipmap_with_big_values():
    r.hset("zipmap_with_big_values", "253bytes", random_string(253, 'seed1'))
    r.hset("zipmap_with_big_values", "254bytes", random_string(254, 'seed2'))
    r.hset("zipmap_with_big_values", "255bytes", random_string(255, 'seed3'))
    r.hset("zipmap_with_big_values", "300bytes", random_string(300, 'seed4'))
    
def dictionary() :
    num_entries = 1000
    for x in xrange(0, num_entries) :
        r.hset("force_dictionary", random_string(50, x), random_string(50, x + num_entries))

def ziplist_that_compresses_easily() :
    for length in (6, 12, 18, 24, 30, 36) :
        r.rpush("ziplist_compresses_easily", ("".join("a" for x in xrange(length))))
    
def ziplist_that_doesnt_compress() :
    r.rpush("ziplist_doesnt_compress", "aj2410")
    r.rpush("ziplist_doesnt_compress", "cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344")

def ziplist_with_integers() :
    
    # Integers between 0 and 12, both inclusive, are encoded differently
    for x in range(0,13):
        r.rpush("ziplist_with_integers", x)
    
    
    # Dealing with 1 byte integers
    r.rpush("ziplist_with_integers", -2)
    r.rpush("ziplist_with_integers", 13)
    r.rpush("ziplist_with_integers", 25)
    r.rpush("ziplist_with_integers", -61)
    r.rpush("ziplist_with_integers", 63)
    
    # Dealing with 2 byte integers
    r.rpush("ziplist_with_integers", 16380)
    r.rpush("ziplist_with_integers", -16000)
    
    # Dealing with 4 byte signed integers
    r.rpush("ziplist_with_integers", 65535)
    r.rpush("ziplist_with_integers", -65523)
    
    # Dealing with 8 byte signed integers
    r.rpush("ziplist_with_integers", 4194304)
    r.rpush("ziplist_with_integers", 0x7fffffffffffffff)

def linkedlist() :
    num_entries = 1000
    for x in xrange(0, num_entries) :
        r.rpush("force_linkedlist", random_string(50, x))

def intset_16() :
    r.sadd("intset_16", 0x7ffe)
    r.sadd("intset_16", 0x7ffd)
    r.sadd("intset_16", 0x7ffc)

def intset_32() :
    r.sadd("intset_32", 0x7ffefffe)
    r.sadd("intset_32", 0x7ffefffd)
    r.sadd("intset_32", 0x7ffefffc)
    
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
    r.zadd("sorted_set_as_ziplist", 2.37, "cb7a24bb7528f934b841b34c3a73e0c7")
    r.zadd("sorted_set_as_ziplist", 3.423, "523af537946b79c4f8369ed39ba78605")
    
def regular_sorted_set() :
    num_entries = 500
    for x in xrange(0, num_entries) :
        r.zadd("force_sorted_set", float(x) / 100, random_string(50, x))
    
def random_string(length, seed) :
    random.seed(seed)
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def backup_redis_dump(redis_dump, backup_folder):
    backup_file = os.path.join(backup_folder, 'dump.rdb.backup')
    shutil.copy(redis_dump, backup_file)
    
def main() :
    dump_folder = os.path.join(os.path.dirname(__file__), 'dumps')
    if not os.path.exists(dump_folder) :
        os.makedirs(dump_folder)
    
    redis_dump = '/var/redis/6379/dump.rdb'
    
    backup_redis_dump(redis_dump, dump_folder)
    create_test_rdbs(redis_dump, dump_folder)

if __name__ == '__main__' :
    main()

