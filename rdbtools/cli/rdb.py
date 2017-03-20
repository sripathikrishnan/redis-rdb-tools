#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from rdbtools import RdbParser, JSONCallback, DiffCallback, MemoryCallback, ProtocolCallback, PrintAllKeys, KeysOnlyCallback, KeyValsOnlyCallback
from rdbtools.encodehelpers import ESCAPE_CHOICES

VALID_TYPES = ("hash", "set", "string", "list", "sortedset")
def main():
    usage = """usage: %prog [options] /path/to/dump.rdb

Example : %prog --command json -k "user.*" /var/redis/6379/dump.rdb"""

    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--command", dest="command",
                  help="Command to execute. Valid commands are json, diff, justkeys, justkeyvals, memory and protocol", metavar="FILE")
    parser.add_option("-f", "--file", dest="output",
                  help="Output file", metavar="FILE")
    parser.add_option("-n", "--db", dest="dbs", action="append",
                  help="Database Number. Multiple databases can be provided. If not specified, all databases will be included.")
    parser.add_option("-k", "--key", dest="keys", default=None,
                  help="Keys to export. This can be a regular expression")
    parser.add_option("-o", "--not-key", dest="not_keys", default=None,
                  help="Keys Not to export. This can be a regular expression")
    parser.add_option("-t", "--type", dest="types", action="append",
                  help="""Data types to include. Possible values are string, hash, set, sortedset, list. Multiple typees can be provided. 
                    If not specified, all data types will be returned""")
    parser.add_option("-b", "--bytes", dest="bytes", default=None,
                  help="Limit memory output to keys greater to or equal to this value (in bytes)")
    parser.add_option("-l", "--largest", dest="largest", default=None,
                  help="Limit memory output to only the top N keys (by size)")
    parser.add_option("-e", "--escape", dest="escape", choices=ESCAPE_CHOICES,
                      help="Escape strings to encoding: %s (default), %s, %s, or %s." % tuple(ESCAPE_CHOICES))

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
        
    if options.not_keys:
        filters['not_keys'] = options.not_keys
    
    if options.types:
        filters['types'] = []
        for x in options.types:
            if not x in VALID_TYPES:
                raise Exception('Invalid type provided - %s. Expected one of %s' % (x, (", ".join(VALID_TYPES))))
            else:
                filters['types'].append(x)

    out_file_obj = None
    try:
        if options.output:
            out_file_obj = open(options.output, "wb")
        else:
            # Prefer not to depend on Python stdout implementation for writing binary.
            out_file_obj = os.fdopen(sys.stdout.fileno(), 'wb')

        try:
            callback = {
                'diff': lambda f: DiffCallback(f, string_escape=options.escape),
                'json': lambda f: JSONCallback(f, string_escape=options.escape),
                'justkeys': lambda f: KeysOnlyCallback(f, string_escape=options.escape),
                'justkeyvals': lambda f: KeyValsOnlyCallback(f, string_escape=options.escape),
                'memory': lambda f: MemoryCallback(PrintAllKeys(f, options.bytes, options.largest),
                                                   64, string_escape=options.escape),
                'protocol': lambda f: ProtocolCallback(f, string_escape=options.escape)
            }[options.command](out_file_obj)
        except:
            raise Exception('Invalid Command %s' % options.command)

        parser = RdbParser(callback, filters=filters)
        parser.parse(dump_file)
    finally:
        if options.output and out_file_obj is not None:
            out_file_obj.close()

if __name__ == '__main__':
    main()
