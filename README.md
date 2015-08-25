# Parse Redis dump.rdb files, Analyze Memory, and Export Data to JSON #

Rdbtools is a parser for Redis' dump.rdb files. The parser generates events similar to an xml sax parser, and is very efficient memory wise.

In addition, rdbtools provides utilities to :

 1.  Generate a Memory Report of your data across all databases and keys
 2.  Convert dump files to JSON
 3.  Compare two dump files using standard diff tools

Rdbtools is written in Python, though there are similar projects in other languages. See [FAQs](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/FAQs) for more information.

## Installing rdbtools ##

Pre-Requisites : 

1. python 2.x and pip.
2. redis-py is optional and only needed to run test cases.

To install from PyPI (recommended) :

    pip install rdbtools
    
To install from source : 

    git clone https://github.com/sripathikrishnan/redis-rdb-tools
    cd redis-rdb-tools
    sudo python setup.py install

## Converting dump files to JSON ##

Parse the dump file and print the JSON on standard output

    rdb --command json /var/redis/6379/dump.rdb
    
Only process keys that match the regex

    rdb --command json --key "user.*" /var/redis/6379/dump.rdb
    
Only process hashes starting with "a", in database 2 

    rdb --command json --db 2 --type hash --key "a.*" /var/redis/6379/dump.rdb


## Generate Memory Report ##

Running with the  `-c memory` generates a CSV report with the approximate memory used by that key.

    rdb -c memory /var/redis/6379/dump.rdb > memory.csv


The generated CSV has the following columns - Database Number, Data Type, Key, Memory Used in bytes and Encoding. 
Memory usage includes the key, the value and any other overheads.

Note that the memory usage is approximate. In general, the actual memory used will be slightly higher than what is reported.

You can filter the report on keys or database number or data type.

The memory report should help you detect memory leaks caused by your application logic. It will also help you optimize Redis memory usage. 

## Find Memory used by a Single Key ##

Sometimes you just want to find the memory used by a particular key, and running the entire memory report on the dump file is time consuming.

In such cases, you can use the `redis-memory-for-key` command

Example :

    redis-memory-for-key person:1
    
    redis-memory-for-key -s localhost -p 6379 -a mypassword person:1
    
Output :

    Key 			"person:1"
    Bytes				111
    Type				hash
    Encoding			ziplist
    Number of Elements		2
    Length of Largest Element	8

NOTE : 

1. This was added to redis-rdb-tools version 0.1.3
2. This command depends [redis-py](https://github.com/andymccurdy/redis-py) package

## Comparing RDB files ##

First, use the --command diff option, and pipe the output to standard sort utility

    rdb --command diff /var/redis/6379/dump1.rdb | sort > dump1.txt
    rdb --command diff /var/redis/6379/dump2.rdb | sort > dump2.txt
    
Then, run your favourite diff program

    kdiff3 dump1.txt dump2.txt

To limit the size of the files, you can filter on keys using the --key=regex option

## Emitting Redis Protocol ##

You can convert RDB file into a stream of [redis protocol](http://redis.io/topics/protocol) using the "protocol" command.

    rdb --command protocol /var/redis/6379/dump.rdb
    
    *4
    $4
    HSET
    $9
    users:123
    $9
    firstname
    $8
    Sripathi

You can pipe the output to netcat and re-import a subset of the data. 
For example, if you want to shard your data into two redis instances, you can use the --key flag to select a subset of data, 
and then pipe the output to a running redis instance to load that data.

Read [Redis Mass Insert](http://redis.io/topics/mass-insert) for more information on this.

## Using the Parser ##

    import sys
    from rdbtools import RdbParser, RdbCallback

    class MyCallback(RdbCallback) :
        ''' Simple example to show how callback works. 
            See RdbCallback for all available callback methods.
            See JsonCallback for a concrete example
        ''' 
        def set(self, key, value, expiry, info):
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

## License

rdbtools is licensed under the MIT License. See [LICENSE](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/LICENSE)

## Maintained By 

Sripathi Krishnan : @srithedabbler

## Credits

 1. [Didier Spézia](https://twitter.com/#!/didier_06)
 2. [Yoav Steinberg](https://github.com/yoav-steinberg)
 3. [Daniel Mezzatto](https://github.com/mezzatto)
 4. [Carlo Cabanilla](https://github.com/clofresh)
 5. [Josep M. Pujol](https://github.com/solso)
 6. [Charles Gordon](https://github.com/cgordon)
 7. [Justin Poliey](https://github.com/jdp)

