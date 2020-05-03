#!/usr/bin/env python
from __future__ import print_function
import os
import sys
from argparse import ArgumentParser
from rdbtools import RdbParser, JSONCallback, DiffCallback, MemoryCallback, ProtocolCallback, PrintAllKeys, KeysOnlyCallback, KeyValsOnlyCallback
from rdbtools.encodehelpers import ESCAPE_CHOICES
from rdbtools.parser import HAS_PYTHON_LZF as PYTHON_LZF_INSTALLED


def eprint(*args, **kwargs):
    """Print a string to the stderr stream"""
    print(*args, file=sys.stderr, **kwargs)

VALID_TYPES = ("hash", "set", "string", "list", "sortedset")
def main():
    usage = """usage: %(prog)s [options] /path/to/dump.rdb

Example : %(prog)s --command json -k "user.*" /var/redis/6379/dump.rdb"""

    parser = ArgumentParser(prog='rdb', usage=usage)
    parser.add_argument("-c", "--command", dest="command", required=True,
                  help="Command to execute. Valid commands are json, diff, justkeys, justkeyvals, memory and protocol", metavar="CMD")
    parser.add_argument("-f", "--file", dest="output",
                  help="Output file", metavar="FILE")
    parser.add_argument("-n", "--db", dest="dbs", action="append",
                  help="Database Number. Multiple databases can be provided. If not specified, all databases will be included.")
    parser.add_argument("-k", "--key", dest="keys", default=None,
                  help="Keys to export. This can be a regular expression")
    parser.add_argument("-o", "--not-key", dest="not_keys", default=None,
                  help="Keys Not to export. This can be a regular expression")
    parser.add_argument("-t", "--type", dest="types", action="append",
                  help="""Data types to include. Possible values are string, hash, set, sortedset, list. Multiple typees can be provided. 
                    If not specified, all data types will be returned""")
    parser.add_argument("-b", "--bytes", dest="bytes", default=None,
                  help="Limit memory output to keys greater to or equal to this value (in bytes)")
    parser.add_argument("-l", "--largest", dest="largest", default=None,
                  help="Limit memory output to only the top N keys (by size)")
    parser.add_argument("-e", "--escape", dest="escape", choices=ESCAPE_CHOICES,
                  help="Escape strings to encoding: %s (default), %s, %s, or %s." % tuple(ESCAPE_CHOICES))
    expire_group = parser.add_mutually_exclusive_group(required=False)
    expire_group.add_argument("-x", "--no-expire", dest="no_expire", default=False, action='store_true',
                  help="With protocol command, remove expiry from all keys")
    expire_group.add_argument("-a", "--amend-expire", dest="amend_expire", default=0, type=int, metavar='N',
                  help="With protocol command, add N seconds to key expiry time")
    parser.add_argument("dump_file", nargs=1, help="RDB Dump file to process")

    options = parser.parse_args()
    
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
                'protocol': lambda f: ProtocolCallback(f, string_escape=options.escape,
                                                       emit_expire=not options.no_expire,
                                                       amend_expire=options.amend_expire
                                                      )
            }[options.command](out_file_obj)
        except:
            raise Exception('Invalid Command %s' % options.command)

        if not PYTHON_LZF_INSTALLED:
            eprint("WARNING: python-lzf package NOT detected. " +
                "Parsing dump file will be very slow unless you install it. " +
                "To install, run the following command:")
            eprint("")
            eprint("pip install python-lzf")
            eprint("")

        parser = RdbParser(callback, filters=filters)
        parser.parse(options.dump_file[0])
    finally:
        if options.output and out_file_obj is not None:
            out_file_obj.close()

if __name__ == '__main__':
    main()
