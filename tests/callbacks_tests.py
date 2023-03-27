import glob
import os
import unittest
import random
import sys
from io import BytesIO
import traceback

from rdbtools import RdbParser
from rdbtools import encodehelpers
from rdbtools.callbacks import ProtocolCallback, JSONCallback, DiffCallback, KeysOnlyCallback, KeyValsOnlyCallback

if sys.version_info < (3,):
    def rand_bytes(count):
        return ''.join(chr(random.randrange(256)) for _ in range(count))
else:
    def rand_bytes(count):
        return bytes(random.randrange(256) for _ in range(count))

TEST_DUMPS_DIR = 'dumps-7'


class CallbackTester(unittest.TestCase):
    """
    General callback tester to use with specific callback tests.
    Child class should implement callback_setup() to fill _callback_class, and _fixture attributes.
    """
    def setUp(self):
        self._out = BytesIO()
        self.callback_setup()

    def callback_setup(self):
        self._callback_class = None
        self._fixture = {'escape_db_file': 'non_ascii_values.rdb'}

    def escape_test_helper(self, escape_name):
        if self._callback_class is None:
            return  # Handle unittest discovery attempt to test with this "abstract" class.

        escape = getattr(encodehelpers, escape_name)
        callback = self._callback_class(out=self._out, string_escape=escape)
        parser = RdbParser(callback)
        parser.parse(os.path.join(os.path.dirname(__file__), TEST_DUMPS_DIR, self._fixture['escape_db_file']))
        result = self._out.getvalue()
        # print('\n%s escape method %s' % (self._callback_class.__name__, escape_name))
        # print("\t\tself._fixture['escape_out_%s'] = %s" % (escape, repr(result)))
        # try:
        #     print(result.decode('utf8'))
        # except UnicodeDecodeError:
        #     print(result.decode('latin-1'))
        self.assertEqual(result,
                         self._fixture['escape_out_' + escape],
                         msg='\n%s escape method %s' % (self._callback_class.__name__, escape_name)
                         )

    def test_raw_escape(self):
        """Test using STRING_ESCAPE_RAW with varied key encodings against expected output."""
        self.escape_test_helper('STRING_ESCAPE_RAW')

    def test_print_escape(self):
        """Test using STRING_ESCAPE_PRINT with varied key encodings against expected output."""
        self.escape_test_helper('STRING_ESCAPE_PRINT')

    def test_utf8_escape(self):
        """Test using STRING_ESCAPE_UTF8 with varied key encodings against expected output."""
        self.escape_test_helper('STRING_ESCAPE_UTF8')

    def test_base64_escape(self):
        """Test using STRING_ESCAPE_BASE64 with varied key encodings against expected output."""
        self.escape_test_helper('STRING_ESCAPE_BASE64')

    def test_all_dumps(self):
        """Run callback with all test dumps intercepting incidental crashes."""
        if self._callback_class is None:
            return  # Handle unittest discovery attempt to test with this "abstract" class.

        for dump_name in glob.glob(os.path.join(os.path.dirname(__file__), TEST_DUMPS_DIR, '*.rdb')):
            callback = self._callback_class(out=self._out)
            parser = RdbParser(callback)
            try:
                parser.parse(dump_name)
            except Exception as err:
                raise self.failureException("%s on %s\n%s" % (
                    self._callback_class.__name__, os.path.basename(dump_name), traceback.format_exc()))
            self._out.seek(0)
            self._out.truncate()


class ProtocolTestCase(CallbackTester):
    def callback_setup(self):
        super(ProtocolTestCase, self).callback_setup()
        self._callback_class = ProtocolCallback
        self._fixture['escape_out_raw'] = b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$9\r\nint_value\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$5\r\nascii\r\n$10\r\n\x00! ~0\n\t\rAb\r\n*3\r\n$3\r\nSET\r\n$3\r\nbin\r\n$14\r\n\x00$ ~0\x7f\xff\n\xaa\t\x80\rAb\r\n*3\r\n$3\r\nSET\r\n$9\r\nprintable\r\n$7\r\n!+ Ab^~\r\n*3\r\n$3\r\nSET\r\n$3\r\n378\r\n$12\r\nint_key_name\r\n*3\r\n$3\r\nSET\r\n$4\r\nutf8\r\n$27\r\n\xd7\x91\xd7\x93\xd7\x99\xd7\xa7\xd7\x94\xf0\x90\x80\x8f123\xd7\xa2\xd7\x91\xd7\xa8\xd7\x99\xd7\xaa\r\n'
        self._fixture['escape_out_print'] = b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$9\r\nint_value\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$5\r\nascii\r\n$22\r\n\\x00! ~0\\x0A\\x09\\x0DAb\r\n*3\r\n$3\r\nSET\r\n$3\r\nbin\r\n$38\r\n\\x00$ ~0\\x7F\\xFF\\x0A\\xAA\\x09\\x80\\x0DAb\r\n*3\r\n$3\r\nSET\r\n$9\r\nprintable\r\n$7\r\n!+ Ab^~\r\n*3\r\n$3\r\nSET\r\n$3\r\n378\r\n$12\r\nint_key_name\r\n*3\r\n$3\r\nSET\r\n$4\r\nutf8\r\n$99\r\n\\xD7\\x91\\xD7\\x93\\xD7\\x99\\xD7\\xA7\\xD7\\x94\\xF0\\x90\\x80\\x8F123\\xD7\\xA2\\xD7\\x91\\xD7\\xA8\\xD7\\x99\\xD7\\xAA\r\n'
        self._fixture['escape_out_utf8'] = b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$9\r\nint_value\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$5\r\nascii\r\n$10\r\n\x00! ~0\n\t\rAb\r\n*3\r\n$3\r\nSET\r\n$3\r\nbin\r\n$23\r\n\x00$ ~0\x7f\\xFF\n\\xAA\t\\x80\rAb\r\n*3\r\n$3\r\nSET\r\n$9\r\nprintable\r\n$7\r\n!+ Ab^~\r\n*3\r\n$3\r\nSET\r\n$3\r\n378\r\n$12\r\nint_key_name\r\n*3\r\n$3\r\nSET\r\n$4\r\nutf8\r\n$27\r\n\xd7\x91\xd7\x93\xd7\x99\xd7\xa7\xd7\x94\xf0\x90\x80\x8f123\xd7\xa2\xd7\x91\xd7\xa8\xd7\x99\xd7\xaa\r\n'
        self._fixture['escape_out_base64'] = b'*2\r\n$8\r\nU0VMRUNU\r\n$4\r\nMA==\r\n*3\r\n$4\r\nU0VU\r\n$12\r\naW50X3ZhbHVl\r\n$4\r\nMTIz\r\n*3\r\n$4\r\nU0VU\r\n$8\r\nYXNjaWk=\r\n$16\r\nACEgfjAKCQ1BYg==\r\n*3\r\n$4\r\nU0VU\r\n$4\r\nYmlu\r\n$20\r\nACQgfjB//wqqCYANQWI=\r\n*3\r\n$4\r\nU0VU\r\n$12\r\ncHJpbnRhYmxl\r\n$12\r\nISsgQWJefg==\r\n*3\r\n$4\r\nU0VU\r\n$4\r\nMzc4\r\n$16\r\naW50X2tleV9uYW1l\r\n*3\r\n$4\r\nU0VU\r\n$8\r\ndXRmOA==\r\n$36\r\n15HXk9eZ16fXlPCQgI8xMjPXoteR16jXmdeq\r\n'


class JsonTestCase(CallbackTester):
    def callback_setup(self):
        super(JsonTestCase, self).callback_setup()
        self._callback_class = JSONCallback
        self._fixture['escape_out_raw'] = b'[{\r\n"int_value":"123",\r\n"ascii":"\\u0000! ~0\\n\\t\\rAb",\r\n"bin":"\\u0000$ ~0\\u007f\\u00ff\\n\\u00aa\\t\\u0080\\rAb",\r\n"printable":"!+ Ab^~",\r\n"378":"int_key_name",\r\n"utf8":"\\u00d7\\u0091\\u00d7\\u0093\\u00d7\\u0099\\u00d7\\u00a7\\u00d7\\u0094\\u00f0\\u0090\\u0080\\u008f123\\u00d7\\u00a2\\u00d7\\u0091\\u00d7\\u00a8\\u00d7\\u0099\\u00d7\\u00aa"}]'
        self._fixture['escape_out_print'] = b'[{\r\n"int_value":"123",\r\n"ascii":"\\\\x00! ~0\\\\x0A\\\\x09\\\\x0DAb",\r\n"bin":"\\\\x00$ ~0\\\\x7F\\\\xFF\\\\x0A\\\\xAA\\\\x09\\\\x80\\\\x0DAb",\r\n"printable":"!+ Ab^~",\r\n"378":"int_key_name",\r\n"utf8":"\\\\xD7\\\\x91\\\\xD7\\\\x93\\\\xD7\\\\x99\\\\xD7\\\\xA7\\\\xD7\\\\x94\\\\xF0\\\\x90\\\\x80\\\\x8F123\\\\xD7\\\\xA2\\\\xD7\\\\x91\\\\xD7\\\\xA8\\\\xD7\\\\x99\\\\xD7\\\\xAA"}]'
        self._fixture['escape_out_utf8'] = b'[{\r\n"int_value":"123",\r\n"ascii":"\\u0000! ~0\\n\\t\\rAb",\r\n"bin":"\\u0000$ ~0\\u007f\\\\xFF\\n\\\\xAA\\t\\\\x80\\rAb",\r\n"printable":"!+ Ab^~",\r\n"378":"int_key_name",\r\n"utf8":"\\u05d1\\u05d3\\u05d9\\u05e7\\u05d4\\ud800\\udc0f123\\u05e2\\u05d1\\u05e8\\u05d9\\u05ea"}]'
        self._fixture['escape_out_base64'] = b'[{\r\n"int_value":"MTIz",\r\n"ascii":"ACEgfjAKCQ1BYg==",\r\n"bin":"ACQgfjB//wqqCYANQWI=",\r\n"printable":"ISsgQWJefg==",\r\n"378":"aW50X2tleV9uYW1l",\r\n"utf8":"15HXk9eZ16fXlPCQgI8xMjPXoteR16jXmdeq"}]'


class DiffTestCase(CallbackTester):
    def callback_setup(self):
        super(DiffTestCase, self).callback_setup()
        self._callback_class = DiffCallback
        self._fixture['escape_out_raw'] = b'db=0 int_value -> 123\r\ndb=0 ascii -> \x00! ~0\n\t\rAb\r\ndb=0 bin -> \x00$ ~0\x7f\xff\n\xaa\t\x80\rAb\r\ndb=0 printable -> !+ Ab^~\r\ndb=0 378 -> int_key_name\r\ndb=0 utf8 -> \xd7\x91\xd7\x93\xd7\x99\xd7\xa7\xd7\x94\xf0\x90\x80\x8f123\xd7\xa2\xd7\x91\xd7\xa8\xd7\x99\xd7\xaa\r\n'
        self._fixture['escape_out_print'] = b'db=0 int_value -> 123\r\ndb=0 ascii -> \\x00! ~0\\x0A\\x09\\x0DAb\r\ndb=0 bin -> \\x00$ ~0\\x7F\\xFF\\x0A\\xAA\\x09\\x80\\x0DAb\r\ndb=0 printable -> !+ Ab^~\r\ndb=0 378 -> int_key_name\r\ndb=0 utf8 -> \\xD7\\x91\\xD7\\x93\\xD7\\x99\\xD7\\xA7\\xD7\\x94\\xF0\\x90\\x80\\x8F123\\xD7\\xA2\\xD7\\x91\\xD7\\xA8\\xD7\\x99\\xD7\\xAA\r\n'
        self._fixture['escape_out_utf8'] = b'db=0 int_value -> 123\r\ndb=0 ascii -> \x00! ~0\n\t\rAb\r\ndb=0 bin -> \x00$ ~0\x7f\\xFF\n\\xAA\t\\x80\rAb\r\ndb=0 printable -> !+ Ab^~\r\ndb=0 378 -> int_key_name\r\ndb=0 utf8 -> \xd7\x91\xd7\x93\xd7\x99\xd7\xa7\xd7\x94\xf0\x90\x80\x8f123\xd7\xa2\xd7\x91\xd7\xa8\xd7\x99\xd7\xaa\r\n'
        self._fixture['escape_out_base64'] = b'db=0 int_value -> MTIz\r\ndb=0 ascii -> ACEgfjAKCQ1BYg==\r\ndb=0 bin -> ACQgfjB//wqqCYANQWI=\r\ndb=0 printable -> ISsgQWJefg==\r\ndb=0 378 -> aW50X2tleV9uYW1l\r\ndb=0 utf8 -> 15HXk9eZ16fXlPCQgI8xMjPXoteR16jXmdeq\r\n'


class KeysTestCase(CallbackTester):
    def callback_setup(self):
        super(KeysTestCase, self).callback_setup()
        self._callback_class = KeysOnlyCallback
        self._fixture['escape_out_raw'] = b'int_value\nascii\nbin\nprintable\n378\nutf8\n'
        self._fixture['escape_out_print'] = b'int_value\nascii\nbin\nprintable\n378\nutf8\n'
        self._fixture['escape_out_utf8'] = b'int_value\nascii\nbin\nprintable\n378\nutf8\n'
        self._fixture['escape_out_base64'] = b'int_value\nascii\nbin\nprintable\n378\nutf8\n'


class KeyValsTestCase(CallbackTester):
    def callback_setup(self):
        super(KeyValsTestCase, self).callback_setup()
        self._callback_class = KeyValsOnlyCallback
        self._fixture['escape_out_raw'] = b'\r\nint_value 123,\r\nascii \x00! ~0\n\t\rAb,\r\nbin \x00$ ~0\x7f\xff\n\xaa\t\x80\rAb,\r\nprintable !+ Ab^~,\r\n378 int_key_name,\r\nutf8 \xd7\x91\xd7\x93\xd7\x99\xd7\xa7\xd7\x94\xf0\x90\x80\x8f123\xd7\xa2\xd7\x91\xd7\xa8\xd7\x99\xd7\xaa'
        self._fixture['escape_out_print'] = b'\r\nint_value 123,\r\nascii \\x00! ~0\\x0A\\x09\\x0DAb,\r\nbin \\x00$ ~0\\x7F\\xFF\\x0A\\xAA\\x09\\x80\\x0DAb,\r\nprintable !+ Ab^~,\r\n378 int_key_name,\r\nutf8 \\xD7\\x91\\xD7\\x93\\xD7\\x99\\xD7\\xA7\\xD7\\x94\\xF0\\x90\\x80\\x8F123\\xD7\\xA2\\xD7\\x91\\xD7\\xA8\\xD7\\x99\\xD7\\xAA'
        self._fixture['escape_out_utf8'] = b'\r\nint_value 123,\r\nascii \x00! ~0\n\t\rAb,\r\nbin \x00$ ~0\x7f\\xFF\n\\xAA\t\\x80\rAb,\r\nprintable !+ Ab^~,\r\n378 int_key_name,\r\nutf8 \xd7\x91\xd7\x93\xd7\x99\xd7\xa7\xd7\x94\xf0\x90\x80\x8f123\xd7\xa2\xd7\x91\xd7\xa8\xd7\x99\xd7\xaa'
        self._fixture['escape_out_base64'] = b'\r\nint_value MTIz,\r\nascii ACEgfjAKCQ1BYg==,\r\nbin ACQgfjB//wqqCYANQWI=,\r\nprintable ISsgQWJefg==,\r\n378 aW50X2tleV9uYW1l,\r\nutf8 15HXk9eZ16fXlPCQgI8xMjPXoteR16jXmdeq'

if __name__ == '__main__':
    unittest.main()
