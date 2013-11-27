import unittest
from tests.parser_tests import RedisParserTestCase
from tests.memprofiler_tests import MemoryCallbackTestCase
from tests.stats_aggregator_tests import TestStatsAggregator

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(RedisParserTestCase))
    suite.addTest(unittest.makeSuite(MemoryCallbackTestCase))
    suite.addTest(unittest.makeSuite(TestStatsAggregator))
    return suite
