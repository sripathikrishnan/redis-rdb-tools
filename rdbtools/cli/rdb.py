#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
import timeit
import time
from rdbtools import RdbParser, JSONCallback, DiffCallback, MemoryCallback, PrintAllKeys

VALID_TYPES = ("hash", "set", "string", "list", "sortedset")
def main():
    usage = """usage: %prog [options] /path/to/dump.rdb

Example : %prog --command json -k "user.*" /var/redis/6379/dump.rdb"""

    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--command", dest="command",
                  help="Command to execute. Valid commands are json or diff", metavar="FILE")
                  
    parser.add_option("-f", "--file", dest="output",
                  help="Output file", metavar="FILE")
    parser.add_option("-n", "--db", dest="dbs", action="append",
                  help="Database Number. Multiple databases can be provided. If not specified, all databases will be included.")
    parser.add_option("-k", "--key", dest="keys", default=None,
                  help="Keys to export. This can be a regular expression,"
                       "When picking this mode, we will dump a deep copy of the key")
    parser.add_option("-t", "--type", dest="types", action="append",
                  help="""Data types to include. Possible values are string, hash, set, sortedset, list. Multiple typees can be provided. 
                    If not specified, all data types will be returned""")

    parser.add_option("-p", "--pos", dest="pos",
                  help="""Position in RDB file to skip to, after generated an memory index file this can be used to speed up.
                          The data starts reading at 9 bytes""",
                  default = 9)

    parser.add_option("-m", "--max", dest="max",
                  help=""" Read maximum number of keys, to limit search.""",
                  default = 2e31)


    parser.add_option("-v", "--verbose", dest="verbose",
                  help="""If true dump a deep copy of the data structure""",
                  action = "store_true",
                  default = False)

    parser.add_option("-q", "--quick", dest="quick",
                  help="""If true dump a deep copy of the data structure""",
                  action = "store_true",
                  default = False)
    
    (options, args) = parser.parse_args()
    
    if len(args) == 0:
        parser.error("Redis RDB file not specified")
    dump_file = args[0]
    
    filters = {}
    if options.dbs:
        filters['dbs'] = []
        for x in options.dbs:
            try:
                filters['dbs'].append(int(x))
            except ValueError:
                raise Exception('Invalid database number %s' %x)
    
    if options.keys:
        filters['keys'] = options.keys
    
    if options.types:
        filters['types'] = []
        for x in options.types:
            if not x in VALID_TYPES:
                raise Exception('Invalid type provided - %s. Expected one of %s' % (x, (", ".join(VALID_TYPES))))
            else:
                filters['types'].append(x)


    filters['pos'] = options.pos
    filters['max'] = options.max
    
    # TODO : Fix this ugly if-else code
    callback = None
    if options.output:
        f = open(options.output, "wb")
        if 'diff' == options.command:
            callback = DiffCallback(f)
        elif 'json' == options.command:
            callback = JSONCallback(f)
        elif 'memory' == options.command:
            reporter = PrintAllKeys(f)
            callback = MemoryCallback(reporter, 64, options.verbose)
        else:
            raise Exception('Invalid Command %s' % options.output)
    else:
        if 'diff' == options.command:
            callback = DiffCallback(sys.stdout)
        elif 'json' == options.command:
            callback = JSONCallback(sys.stdout)
        elif 'memory' == options.command:
            reporter = PrintAllKeys(sys.stdout)
            callback = MemoryCallback(reporter, 64, options.verbose)
        else:
            raise Exception('Invalid Command %s' % options.output)


    start = time.clock()
    parser = RdbParser(callback, filters=filters, quick=options.quick)
    parser.parse(dump_file)
    end =  time.clock()

    print "time=%s seconds" % (end-start)
    
if __name__ == '__main__':

   main()

