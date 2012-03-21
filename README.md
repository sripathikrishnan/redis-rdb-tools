# Tools to inspect Redis's dump.rdb file #

RDB Tools is a set of tools to work with Redis dump files

rdb-tools lets you : 
 
 1.  Convert dump files into JSON
 2.  Compare two dump files using standard diff tools
 3.  Efficiently parse and process rdb files

RDB Tools is implemented in Python. 

## Installing rdbtools ##

    git checkout git@github.com:sripathikrishnan/redis-rdb-tools.git
    cd redis-rdb-tools
    sudo python setup.py install

## Converting to dump files to JSON ##

Parse the dump file and print the JSON on standard output

    ./rdb --command json /var/redis/6379/dump.rdb
    
Only process keys that match the regex

    ./rdb --command json --key "user.*" /var/redis/6379/dump.rdb
    
Only process hashes starting with "a", in database 2 

    ./rdb --command json --db 2 --type hash --key "a.*" /var/redis/6379/dump.rdb


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

## What can I do with this parser? ## 
Several things 

 1.  Export redis into a relational database like MySQL
 2.  Export redis into a full text search engine like lucene/solr, so that you can do (almost) real time searches
 3.  Merge or split dump files. This is useful if you using several instances of Redis and shard your data
 4.  Build a UI/Explorer for the data in Redis

