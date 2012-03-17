rdb-tools can parse Redis *.rdb files and convert them to JSON

The API is similar to a XML SAX parser. You create a callback to receive events, 
and then pass your callback to the parser. 

The JSON converter is memory-efficient. It avoids storing objects in memory
and writes to the stream as soon as an object is read from the rdb file.

## Sample Code ##

    import sys
    from rdbtools import RdbParser, RdbCallback
    
    callback = JSONCallback(sys.stdout)
    parser = RdbParser(callback)
    parser.parse('/var/redis/6379/dump.rdb')


## Installing rdbtools ##

    git checkout git@github.com:sripathikrishnan/redis-rdb-tools.git
    cd redis-rdb-tools
    sudo python setup.py install


