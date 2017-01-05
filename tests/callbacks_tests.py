import unittest
import random
from io import BytesIO
from rdbtools.callbacks import ProtocolCallback


class ProtocolTestCase(unittest.TestCase):
    def setUp(self):
        self._out = BytesIO()
        self._callback = ProtocolCallback(self._out)

    def test_emit(self):
        utf8_string = '\xd9\xa1\xdf\x82\xe0\xa5\xa9\xe1\xa7\x94\xf0\x91\x8b\xb5'
        bcde_non_print = '\x00\x01bcde\r\n\x88\x99'
        random_bytes = ''.join(chr(random.randrange(0, 255)) for _ in range(64))
        integer = 46
        self._callback.emit(utf8_string, bcde_non_print, random_bytes, integer)
        expected = '\r\n'.join(['*4', '$14', utf8_string, '$10', bcde_non_print, '$64', random_bytes, '$2', '46\r\n'])
        result = self._out.getvalue()
        self.assertEquals(result, expected)
