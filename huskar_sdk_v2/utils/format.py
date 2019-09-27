from __future__ import absolute_import


def char_encoding(value):
    """ Encode unicode into 'UTF-8' string.

    :param value:
    :return:
    """
    if not isinstance(value, bytes):
        return value.encode('utf-8')
    # consider it to be 'utf-8' character
    return value


def char_decoding(value):
    """ Decode from 'UTF-8' string to unicode.

    :param value:
    :return:
    """
    if isinstance(value, bytes):
        return value.decode('utf-8')
    # return directly if unicode or exc happens.
    return value
