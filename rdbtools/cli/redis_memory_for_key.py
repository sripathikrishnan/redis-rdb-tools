#!/usr/bin/env python
import struct
import os
import sys

try:
    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO
    
from optparse import OptionParser
from rdbtools import RdbParser, JSONCallback, MemoryCallback

from redis import StrictRedis
from redis.exceptions import ConnectionError, ResponseError

def main():
    usage = """usage: %prog [options] redis-key
Examples :
%prog user:13423
%prog -s localhost -p 6379 user:13423
"""

    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--server", dest="host", default="127.0.0.1", 
                  help="Redis Server hostname. Defaults to 127.0.0.1")
    parser.add_option("-p", "--port", dest="port", default=6379, type="int", 
                  help="Redis Server port. Defaults to 6379")
    parser.add_option("-a", "--password", dest="password", 
                  help="Password to use when connecting to the server")
    parser.add_option("-d", "--db", dest="db", default=0,
                  help="Database number, defaults to 0")
    
    (options, args) = parser.parse_args()
    
    if len(args) == 0:
        parser.error("Key not specified")
    redis_key = args[0]
    print_memory_for_key(redis_key, host=options.host, port=options.port, 
                    db=options.db, password=options.password)

def print_memory_for_key(key, host='localhost', port=6379, db=0, password=None):
    redis = connect_to_redis(host, port, db, password)
    reporter = PrintMemoryUsage()
    callback = MemoryCallback(reporter, 64)
    parser = RdbParser(callback, filters={})
    #  DUMP command only return the key data, so we hack RdbParser to inject key name as parsed bytes.
    parser._key = key.encode('utf-8')

    raw_dump = redis.execute_command('dump', key)
    if not raw_dump:
        sys.stderr.write('Key %s does not exist\n' % key)
        sys.exit(-1)
    
    stream = BytesIO(raw_dump)
    data_type = read_unsigned_char(stream)
    parser.read_object(stream, data_type)

def connect_to_redis(host, port, db, password):
    try:
        redis = StrictRedis(host=host, port=port, db=db, password=password)
        if not check_redis_version(redis):
            sys.stderr.write('This script only works with Redis Server version 2.6.x or higher\n')
            sys.exit(-1)
    except ConnectionError as e:
        sys.stderr.write('Could not connect to Redis Server : %s\n' % e)
        sys.exit(-1)
    except ResponseError as e:
        sys.stderr.write('Could not connect to Redis Server : %s\n' % e)
        sys.exit(-1)
    return redis
    
def check_redis_version(redis):
    server_info = redis.info()
    version_str = server_info['redis_version']
    version = tuple(map(int, version_str.split('.')))
    
    if version[0] > 2 or (version[0] == 2 and version[1] >= 6) :
        return True
    else:
        return False

def read_unsigned_char(f) :
    return struct.unpack('B', f.read(1))[0]

class PrintMemoryUsage(object):
    def next_record(self, record) :
        print("%s\t\t\t\t%s" % ("Key", record.key))
        print("%s\t\t\t\t%s" % ("Bytes", record.bytes))
        print("%s\t\t\t\t%s" % ("Type", record.type))
        if record.type in ('set', 'list', 'sortedset', 'hash'):
            print("%s\t\t\t%s" % ("Encoding", record.encoding))
            print("%s\t\t%s" % ("Number of Elements", record.size))
            print("%s\t%s" % ("Length of Largest Element", record.len_largest_element))
        
        #print("%d,%s,%s,%d,%s,%d,%d\n" % (record.database, record.type, encode_key(record.key), 
        #                                         record.bytes, record.encoding, record.size, record.len_largest_element))

if __name__ == '__main__':
    #print_memory_for_key('x')
    main()


