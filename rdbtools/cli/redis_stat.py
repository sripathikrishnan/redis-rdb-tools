#!/usr/bin/env python
import os
import sys

PWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(PWD, '../../'))

import collections

from string import Template
from optparse import OptionParser
from rdbtools import RdbParser, MemoryCallback, PrintAllKeys, StatsAggregator


class Stat2D(object):
    def __init__(self, init=None):
        self.ddict = collections.defaultdict(lambda:collections.defaultdict(int))

    def __getitem__(self, key):
        return self.ddict[key]

    def __setitem__(self, key, value):
        self.ddict[key] = value

    def __delitem__(self, key):
        del self.ddict[key]

    def __contains__(self, key):
        return key in self.ddict

    def __len__(self):
        return len(self.ddict)

    def __repr__(self):
        return repr(self.ddict)
    def __iter__(self):
        return self.ddict.__iter__()
    def __reversed__(self):
        return self.ddict.__reversed__()

    def keys(self):
        return self.ddict.keys()

    def rows(self):
        if not len(self.ddict):
            return []

        headers = [d.keys() for d in self.ddict.values()]
        headers = list(set([i for sublist in headers for i in sublist])) # flat list and uniq
        headers.sort()

        rows = []
        rows.append(['-'] + headers)
        for k in self.ddict:
            row = [str(self.ddict[k][h]) for h in headers]
            rows.append([k] + row)
        rows = sorted(rows)
        return rows


    def to_text(self, rows=None):
        if not rows:
            rows = self.rows()
            if not rows:
                return '-'

        def padding(s, width):
            return ' '*(width-len(s)) + s

        for j in range(len(rows[0])):
            max_width = max([len(row[j]) for row  in rows])
            for i in range(len(rows)):
                rows[i][j] = padding(rows[i][j], max_width)

        rows = [[c for c in r] for r in rows]
        return '\n'.join( ['\t'.join(row) for row in rows])

    def __str__(self):
        return self.to_text()

    def get_html(self):
        pass


class MyStatsAggregator():
    def __init__(self, key_groupings = []):
        self.stat2d = Stat2D()
        self.key_groupings = key_groupings

    def next_record(self, record):

        def get_ns(key):
            for k in self.key_groupings:
                if key.startswith(k):
                    return k
            return 'z_unknown'
        ns = get_ns(str(record.key))

        for k in [ns, 'zz_all']:
            self.stat2d[k]['cnt_' + record.type] += 1
            self.stat2d[k]['mem_' + record.type] += record.bytes
            self.stat2d[k]['cnt'] += 1
            self.stat2d[k]['mem'] += record.bytes

            if record.ttl == -1:
                self.stat2d[k]['z_cnt_no_expire'] += 1
            else:
                self.stat2d[k]['z_sum_expire'] += record.ttl

        #self.add_aggregate('key_group_count', ns, 1)
        #self.add_aggregate('key_group_memory', ns, record.bytes)
        #if record.ttl == -1:
            #self.add_aggregate('key_group_no_expire_count', ns, 1)
        #else:
            #self.add_aggregate('key_group_sum_expire', ns, record.ttl)
  
    def to_text(self):
        return self.stat2d.to_text()

def main(): 
    usage = """usage: %prog [options] /path/to/dump.rdb

Example 1 : %prog -k "user-" -k "friends-" /var/redis/6379/dump.rdb
Example 2 : %prog /var/redis/6379/dump.rdb"""

    parser = OptionParser(usage=usage)

    parser.add_option("-k", "--key", dest="keys", action="append",
                  help="Keys that should be grouped together. Multiple prefix can be provided")
    
    (options, args) = parser.parse_args()
    
    if len(args) == 0:
        parser.error("Redis RDB file not specified")
    dump_file = args[0]
    if not options.keys:
        options.keys = []

    stat2d = MyStatsAggregator(options.keys)
    callback = MemoryCallback(stat2d, 64)
    parser = RdbParser(callback)
    try:
        parser.parse(dump_file)
    except Exception, e:
        print 'error: ', e

    print stat2d.to_text()
    
if __name__ == '__main__':
    main()

