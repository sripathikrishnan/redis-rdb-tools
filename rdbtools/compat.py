# python2->3 compat

import sys, re

try:
    xrange
    range = xrange
except NameError:
    range = range

try:
    long
    def isnumber(n):
        return isinstance(n, int) or isinstance(n, long) or isinstance(n, float)
except NameError:
    def isnumber(n):
        return isinstance(n, int) or isinstance(n, float)

if sys.version_info < (3,):
    def str2regexp(pattern):
        return re.compile(pattern)
else:
    def str2regexp(pattern):
        return re.compile(pattern.encode('utf-8'))

