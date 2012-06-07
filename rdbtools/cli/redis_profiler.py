#!/usr/bin/env python
import os
import sys
from string import Template
from optparse import OptionParser
from rdbtools import RdbParser, MemoryCallback, PrintAllKeys, StatsAggregator

def main(): 
    usage = """usage: %prog [options] /path/to/dump.rdb

Example 1 : %prog -k "user.*" -k "friends.*" -f memoryreport.html /var/redis/6379/dump.rdb
Example 2 : %prog /var/redis/6379/dump.rdb"""

    parser = OptionParser(usage=usage)

    parser.add_option("-f", "--file", dest="output",
                  help="Output file", metavar="FILE")
    parser.add_option("-k", "--key", dest="keys", action="append",
                  help="Keys that should be grouped together. Multiple regexes can be provided")
    
    (options, args) = parser.parse_args()
    
    if len(args) == 0:
        parser.error("Redis RDB file not specified")
    dump_file = args[0]
    
    if not options.output:
        output = "redis_memory_report.html"
    else:
        output = options.output

    stats = StatsAggregator()
    callback = MemoryCallback(stats, 64)
    parser = RdbParser(callback)
    parser.parse(dump_file)
    stats_as_json = stats.get_json()
    
    t = open(os.path.join(os.path.dirname(__file__),"report.html.template")).read()
    report_template = Template(t)
    html = report_template.substitute(REPORT_JSON = stats_as_json)
    print(html)
    
if __name__ == '__main__':
    main()

