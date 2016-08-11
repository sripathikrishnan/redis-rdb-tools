# python2->3 compat

try:
    xrange
    range = xrange
except NameError:
    range = range

try:
    long
    def isinteger(n):
        return isinstance(n, int) or isinstance(n, long)
except NameError:
    def isinteger(n):
        return isinstance(n, int)

