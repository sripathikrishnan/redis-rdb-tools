import unittest
from tests.parser_tests import RedisParserTestCase

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(RedisParserTestCase))
    return suite
