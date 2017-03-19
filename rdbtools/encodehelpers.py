from __future__ import print_function
import base64
import codecs
import sys

from .compat import isnumber

STRING_ESCAPE_RAW = 'raw'
STRING_ESCAPE_PRINT = 'print'
STRING_ESCAPE_UTF8 = 'utf8'
STRING_ESCAPE_BASE64 = 'base64'
ESCAPE_CHOICES = [STRING_ESCAPE_RAW, STRING_ESCAPE_PRINT, STRING_ESCAPE_UTF8, STRING_ESCAPE_BASE64]

if sys.version_info < (3,):
    bval = ord

    def num2unistr(i): return codecs.decode(str(i), 'ascii')
    num2bytes = str
else:
    def bval(x): return x

    num2unistr = str

    def num2bytes(i): return codecs.encode(str(i), 'ascii')

ASCII_ESCAPE_LOOKUP = [u'\\x00', u'\\x01', u'\\x02', u'\\x03', u'\\x04', u'\\x05', u'\\x06', u'\\x07', u'\\x08',
                       u'\\x09', u'\\x0A', u'\\x0B', u'\\x0C', u'\\x0D', u'\\x0E', u'\\x0F', u'\\x10', u'\\x11',
                       u'\\x12', u'\\x13', u'\\x14', u'\\x15', u'\\x16', u'\\x17', u'\\x18', u'\\x19', u'\\x1A',
                       u'\\x1B', u'\\x1C', u'\\x1D', u'\\x1E', u'\\x1F', u' ', u'!', u'"', u'#', u'$', u'%', u'&', u"'",
                       u'(', u')', u'*', u'+', u',', u'-', u'.', u'/', u'0', u'1', u'2', u'3', u'4', u'5', u'6', u'7',
                       u'8', u'9', u':', u';', u'<', u'=', u'>', u'?', u'@', u'A', u'B', u'C', u'D', u'E', u'F', u'G',
                       u'H', u'I', u'J', u'K', u'L', u'M', u'N', u'O', u'P', u'Q', u'R', u'S', u'T', u'U', u'V', u'W',
                       u'X', u'Y', u'Z', u'[', u'\\', u']', u'^', u'_', u'`', u'a', u'b', u'c', u'd', u'e', u'f', u'g',
                       u'h', u'i', u'j', u'k', u'l', u'm', u'n', u'o', u'p', u'q', u'r', u's', u't', u'u', u'v', u'w',
                       u'x', u'y', u'z', u'{', u'|', u'}', u'~', u'\\x7F', u'\\x80', u'\\x81', u'\\x82', u'\\x83',
                       u'\\x84', u'\\x85', u'\\x86', u'\\x87', u'\\x88', u'\\x89', u'\\x8A', u'\\x8B', u'\\x8C',
                       u'\\x8D', u'\\x8E', u'\\x8F', u'\\x90', u'\\x91', u'\\x92', u'\\x93', u'\\x94', u'\\x95',
                       u'\\x96', u'\\x97', u'\\x98', u'\\x99', u'\\x9A', u'\\x9B', u'\\x9C', u'\\x9D', u'\\x9E',
                       u'\\x9F', u'\\xA0', u'\\xA1', u'\\xA2', u'\\xA3', u'\\xA4', u'\\xA5', u'\\xA6', u'\\xA7',
                       u'\\xA8', u'\\xA9', u'\\xAA', u'\\xAB', u'\\xAC', u'\\xAD', u'\\xAE', u'\\xAF', u'\\xB0',
                       u'\\xB1', u'\\xB2', u'\\xB3', u'\\xB4', u'\\xB5', u'\\xB6', u'\\xB7', u'\\xB8', u'\\xB9',
                       u'\\xBA', u'\\xBB', u'\\xBC', u'\\xBD', u'\\xBE', u'\\xBF', u'\\xC0', u'\\xC1', u'\\xC2',
                       u'\\xC3', u'\\xC4', u'\\xC5', u'\\xC6', u'\\xC7', u'\\xC8', u'\\xC9', u'\\xCA', u'\\xCB',
                       u'\\xCC', u'\\xCD', u'\\xCE', u'\\xCF', u'\\xD0', u'\\xD1', u'\\xD2', u'\\xD3', u'\\xD4',
                       u'\\xD5', u'\\xD6', u'\\xD7', u'\\xD8', u'\\xD9', u'\\xDA', u'\\xDB', u'\\xDC', u'\\xDD',
                       u'\\xDE', u'\\xDF', u'\\xE0', u'\\xE1', u'\\xE2', u'\\xE3', u'\\xE4', u'\\xE5', u'\\xE6',
                       u'\\xE7', u'\\xE8', u'\\xE9', u'\\xEA', u'\\xEB', u'\\xEC', u'\\xED', u'\\xEE', u'\\xEF',
                       u'\\xF0', u'\\xF1', u'\\xF2', u'\\xF3', u'\\xF4', u'\\xF5', u'\\xF6', u'\\xF7', u'\\xF8',
                       u'\\xF9', u'\\xFA', u'\\xFB', u'\\xFC', u'\\xFD', u'\\xFE', u'\\xFF']

ASCII_ESCAPE_LOOKUP_BYTES = [b'\\x00', b'\\x01', b'\\x02', b'\\x03', b'\\x04', b'\\x05', b'\\x06', b'\\x07', b'\\x08',
                             b'\\x09', b'\\x0A', b'\\x0B', b'\\x0C', b'\\x0D', b'\\x0E', b'\\x0F', b'\\x10', b'\\x11',
                             b'\\x12', b'\\x13', b'\\x14', b'\\x15', b'\\x16', b'\\x17', b'\\x18', b'\\x19', b'\\x1A',
                             b'\\x1B', b'\\x1C', b'\\x1D', b'\\x1E', b'\\x1F', b' ', b'!', b'"', b'#', b'$', b'%', b'&',
                             b"'", b'(', b')', b'*', b'+', b',', b'-', b'.', b'/', b'0', b'1', b'2', b'3', b'4', b'5',
                             b'6', b'7', b'8', b'9', b':', b';', b'<', b'=', b'>', b'?', b'@', b'A', b'B', b'C', b'D',
                             b'E', b'F', b'G', b'H', b'I', b'J', b'K', b'L', b'M', b'N', b'O', b'P', b'Q', b'R', b'S',
                             b'T', b'U', b'V', b'W', b'X', b'Y', b'Z', b'[', b'\\', b']', b'^', b'_', b'`', b'a', b'b',
                             b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n', b'o', b'p', b'q',
                             b'r', b's', b't', b'u', b'v', b'w', b'x', b'y', b'z', b'{', b'|', b'}', b'~', b'\\x7F',
                             b'\\x80', b'\\x81', b'\\x82', b'\\x83', b'\\x84', b'\\x85', b'\\x86', b'\\x87', b'\\x88',
                             b'\\x89', b'\\x8A', b'\\x8B', b'\\x8C', b'\\x8D', b'\\x8E', b'\\x8F', b'\\x90', b'\\x91',
                             b'\\x92', b'\\x93', b'\\x94', b'\\x95', b'\\x96', b'\\x97', b'\\x98', b'\\x99', b'\\x9A',
                             b'\\x9B', b'\\x9C', b'\\x9D', b'\\x9E', b'\\x9F', b'\\xA0', b'\\xA1', b'\\xA2', b'\\xA3',
                             b'\\xA4', b'\\xA5', b'\\xA6', b'\\xA7', b'\\xA8', b'\\xA9', b'\\xAA', b'\\xAB', b'\\xAC',
                             b'\\xAD', b'\\xAE', b'\\xAF', b'\\xB0', b'\\xB1', b'\\xB2', b'\\xB3', b'\\xB4', b'\\xB5',
                             b'\\xB6', b'\\xB7', b'\\xB8', b'\\xB9', b'\\xBA', b'\\xBB', b'\\xBC', b'\\xBD', b'\\xBE',
                             b'\\xBF', b'\\xC0', b'\\xC1', b'\\xC2', b'\\xC3', b'\\xC4', b'\\xC5', b'\\xC6', b'\\xC7',
                             b'\\xC8', b'\\xC9', b'\\xCA', b'\\xCB', b'\\xCC', b'\\xCD', b'\\xCE', b'\\xCF', b'\\xD0',
                             b'\\xD1', b'\\xD2', b'\\xD3', b'\\xD4', b'\\xD5', b'\\xD6', b'\\xD7', b'\\xD8', b'\\xD9',
                             b'\\xDA', b'\\xDB', b'\\xDC', b'\\xDD', b'\\xDE', b'\\xDF', b'\\xE0', b'\\xE1', b'\\xE2',
                             b'\\xE3', b'\\xE4', b'\\xE5', b'\\xE6', b'\\xE7', b'\\xE8', b'\\xE9', b'\\xEA', b'\\xEB',
                             b'\\xEC', b'\\xED', b'\\xEE', b'\\xEF', b'\\xF0', b'\\xF1', b'\\xF2', b'\\xF3', b'\\xF4',
                             b'\\xF5', b'\\xF6', b'\\xF7', b'\\xF8', b'\\xF9', b'\\xFA', b'\\xFB', b'\\xFC', b'\\xFD',
                             b'\\xFE', b'\\xFF']


def escape_ascii(bytes_data):
    return u''.join(ASCII_ESCAPE_LOOKUP[bval(ch)] for ch in bytes_data)


def escape_ascii_bytes(bytes_data):
    return b''.join(ASCII_ESCAPE_LOOKUP_BYTES[bval(ch)] for ch in bytes_data)


def escape_utf8_error(err):
    return escape_ascii(err.object[err.start:err.end]), err.end

codecs.register_error('rdbslashescape', escape_utf8_error)


def escape_utf8(byte_data):
    return byte_data.decode('utf-8', 'rdbslashescape')


def bytes_to_unicode(byte_data, escape, skip_printable=False):
    """
    Decode given bytes using specified escaping method.
    :param byte_data: The byte-like object with bytes to decode.
    :param escape: The escape method to use.
    :param skip_printable: If True, don't escape byte_data with all 'printable ASCII' bytes. Defaults to False.
    :return: New unicode string, escaped with the specified method if needed.
    """
    if isnumber(byte_data):
        if skip_printable:
            return num2unistr(byte_data)
        else:
            byte_data = num2bytes(byte_data)
    else:
        assert (isinstance(byte_data, type(b'')))
        if skip_printable and all(0x20 <= bval(ch) <= 0x7E for ch in byte_data):
            escape = STRING_ESCAPE_RAW

    if escape == STRING_ESCAPE_RAW:
        return byte_data.decode('latin-1')
    elif escape == STRING_ESCAPE_PRINT:
        return escape_ascii(byte_data)
    elif escape == STRING_ESCAPE_UTF8:
        return escape_utf8(byte_data)
    elif escape == STRING_ESCAPE_BASE64:
        return codecs.decode(base64.b64encode(byte_data), 'latin-1')
    else:
        raise UnicodeEncodeError("Unknown escape option")


def apply_escape_bytes(byte_data, escape, skip_printable=False):
    """
    Apply the specified escape method on the given bytes.
    :param byte_data: The byte-like object with bytes to escape.
    :param escape: The escape method to use.
    :param skip_printable: If True, don't escape byte_data with all 'printable ASCII' bytes. Defaults to False.
    :return: new bytes object with the escaped bytes or byte_data itself on some no-op cases.
    """

    if isnumber(byte_data):
        if skip_printable:
            return num2bytes(byte_data)
        else:
            byte_data = num2bytes(byte_data)
    else:
        assert (isinstance(byte_data, type(b'')))
        if skip_printable and all(0x20 <= bval(ch) <= 0x7E for ch in byte_data):
            escape = STRING_ESCAPE_RAW

    if escape == STRING_ESCAPE_RAW:
        return byte_data
    elif escape == STRING_ESCAPE_PRINT:
        return escape_ascii_bytes(byte_data)
    elif escape == STRING_ESCAPE_UTF8:
        return codecs.encode(escape_utf8(byte_data), 'utf-8')
    elif escape == STRING_ESCAPE_BASE64:
        return base64.b64encode(byte_data)
    else:
        raise UnicodeEncodeError("Unknown escape option")
