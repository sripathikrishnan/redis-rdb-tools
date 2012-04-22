import unittest
from tests.parser_tests import RedisParserTestCase
from tests.memprofiler_tests import MemoryCallbackTestCase

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(RedisParserTestCase))
    suite.addTest(unittest.makeSuite(MemoryCallbackTestCase))
    return suite
