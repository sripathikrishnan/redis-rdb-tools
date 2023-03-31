import unittest
from tests.parser_tests import RedisParserTestCase
from tests.memprofiler_tests import MemoryCallbackTestCase
from tests.callbacks_tests import ProtocolTestCase, JsonTestCase, DiffTestCase, KeysTestCase, KeyValsTestCase
from tests.protocol_tests import ProtocolExpireTestCase


def all_tests():
    suite = unittest.TestSuite()
    test_case_list = [RedisParserTestCase,
                      MemoryCallbackTestCase,
                      ProtocolTestCase,
                      JsonTestCase,
                      DiffTestCase,
                      KeysTestCase,
                      KeyValsTestCase,
                      ProtocolExpireTestCase]
    for case in test_case_list:
        suite.addTest(unittest.makeSuite(case))
    return suite
