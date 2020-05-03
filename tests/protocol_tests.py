import unittest
import os
import math
from rdbtools import ProtocolCallback, RdbParser
from io import BytesIO

class ProtocolExpireTestCase(unittest.TestCase):
    def setUp(self):
        self.dumpfile = os.path.join(
                os.path.dirname(__file__),
                'dumps',
                'keys_with_expiry.rdb')

    def tearDown(self):
        pass


    def test_keys_with_expiry(self):
        expected = (
                b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n'
                b'*3\r\n$3\r\nSET\r\n$20\r\nexpires_ms_precision\r\n'
                b'$27\r\n2022-12-25 10:11:12.573 UTC\r\n'
                b'*3\r\n$8\r\nEXPIREAT\r\n$20\r\nexpires_ms_precision\r\n'
                b'$10\r\n1671963072\r\n'
                )
        buf = BytesIO()
        parser = RdbParser(ProtocolCallback(buf))
        parser.parse(self.dumpfile)
        self.assertEquals(buf.getvalue(), expected)
        

    def test_amend_expiry(self):
        expected = (
                b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n'
                b'*3\r\n$3\r\nSET\r\n$20\r\nexpires_ms_precision\r\n'
                b'$27\r\n2022-12-25 10:11:12.573 UTC\r\n'
                b'*3\r\n$8\r\nEXPIREAT\r\n$20\r\nexpires_ms_precision\r\n'
                b'$10\r\n1671965072\r\n'
                )
        buf = BytesIO()
        parser = RdbParser(ProtocolCallback(buf, amend_expire=2000))
        parser.parse(self.dumpfile)
        self.assertEquals(buf.getvalue(), expected)


    def test_skip_expiry(self):
        expected = (
                b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n'
                b'*3\r\n$3\r\nSET\r\n$20\r\nexpires_ms_precision\r\n'
                b'$27\r\n2022-12-25 10:11:12.573 UTC\r\n'
                )
        buf = BytesIO()
        parser = RdbParser(ProtocolCallback(buf, emit_expire=False))
        parser.parse(self.dumpfile)
        self.assertEquals(buf.getvalue(), expected)


