# Tools to inspect Redis's dump.rdb file #

RDB Tools is a set of tools to work with Redis dump files

rdb-tools lets you : 
 
 1.  Convert dump files into JSON
 2.  Generate a Memory Report of your data across all databases and keys
 3.  Compare two dump files using standard diff tools
 4.  Efficiently parse and process rdb files

RDB Tools is implemented in Python. 

## Installing rdbtools ##

    git checkout git@github.com:sripathikrishnan/redis-rdb-tools.git
    cd redis-rdb-tools
    sudo python setup.py install

## Converting dump files to JSON ##

Parse the dump file and print the JSON on standard output

    ./rdb --command json /var/redis/6379/dump.rdb
    
Only process keys that match the regex

    ./rdb --command json --key "user.*" /var/redis/6379/dump.rdb
    
Only process hashes starting with "a", in database 2 

    ./rdb --command json --db 2 --type hash --key "a.*" /var/redis/6379/dump.rdb


## Generate Memory Report ##

Running with the  `-c memory` generates a CSV report with the approximate memory used by that key.

    ./rdb -c memory /var/redis/6379/dump.rdb > memory.csv


The generated CSV has the following columns - Database Number, Data Type, Key, Memory Used in bytes and Encoding. 
Memory usage includes the key, the value and any other overheads.

Note that the memory usage is approximate. In general, the actual memory used will be slightly higher than what is reported.

You can filter the report on keys or database number or data type.

The memory report should help you detect memory leaks caused by your application logic. It will also help you optimize Redis memory usage. 


## Comparing RDB files ##

First, use the --command diff option, and pipe the output to standard sort utility

    ./rdb --command diff /var/redis/6379/dump1.rdb | sort > dump1.txt
    ./rdb --command diff /var/redis/6379/dump2.rdb | sort > dump2.txt
    
Then, run your favourite diff program

    kdiff3 dump1.txt dump2.txt

To limit the size of the files, you can filter on keys using the --key=regex option

## Using the Parser ##

    import sys
    from rdbtools import RdbParser, RdbCallback

    class MyCallback(RdbCallback) :
        ''' Simple example to show how callback works. 
            See RdbCallback for all available callback methods.
            See JsonCallback for a concrete example
        ''' 
        def set(self, key, value, expiry):
            print('%s = %s' % (str(key), str(value)))
        
        def hset(self, key, field, value):
            print('%s.%s = %s' % (str(key), str(field), str(value)))
        
        def sadd(self, key, member):
            print('%s has {%s}' % (str(key), str(member)))
        
        def rpush(self, key, value) :
            print('%s has [%s]' % (str(key), str(value)))
        
        def zadd(self, key, score, member):
            print('%s has {%s : %s}' % (str(key), str(member), str(score)))

    callback = MyCallback()
    parser = RdbParser(callback)
    parser.parse('/var/redis/6379/dump.rdb')

## Other Pages

 1. [Frequently Asked Questions](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/FAQs)
 2. [Redis Dump File Specification](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format)
 3. [Redis Dump File Version History](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_Version_History.textile) - this also has notes on converting a dump file to an older version.

## Credits

 1. [Didier Spézia](https://twitter.com/#!/didier_06)
 2. [Yoav Steinberg](https://github.com/yoav-steinberg)
 3. [Daniel Mezzatto](https://github.com/mezzatto)

