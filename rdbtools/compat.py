# python2->3 compat

try:
    xrange
    range = xrange
except NameError:
    range = range

