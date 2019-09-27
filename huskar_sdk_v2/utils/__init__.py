from __future__ import absolute_import

import os
import string
import functools


ESCAPE_SLASH = '%SLASH%'


def combine(*parts):
    """combine ``parts`` into valid ZooKeeper path.

    :arg parts: list of str
    """
    if not any(parts):
        return '/'
    parts = filter(lambda part: part, parts)
    return os.path.join(*parts)


def to_legal_filename(s):
    valid_chars = "-_.() " + string.ascii_letters + string.digits
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ', '_')
    return filename


def no_multiprocess_check(func):
    """Check huskar is not running in a multiprocess env
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.client.huskar_pid != os.getpid():
            raise RuntimeError("Huskar can't run in multiprocess env")
        else:
            return func(self, *args, **kwargs)

    return wrapper


def lazy_property(fn):
    """lazy-evaluated version of builtin property decorator
    """
    _name = "_" + fn.__name__

    @property
    def _property(self):
        if not hasattr(self, _name):
            setattr(self, _name, fn(self))
        return getattr(self, _name)
    return _property


def encode_key(key):
    """ encode key to escape letter '/' for zookeeper.

    :param str key:
    :return str: return key with '/' encoded.
    """
    if not key:
        return key
    return key.replace('/', ESCAPE_SLASH)


def decode_key(key):
    """ decode key to '/'.

    :param str key:
    :return str: return key with original '/'.
    """
    if not key:
        return key
    return key.replace(ESCAPE_SLASH, '/')


def join_url(*urls):
    urls = list(urls)
    if not urls[0].startswith('http'):
        urls[0] = 'http://' + urls[0]
    return '/'.join([u.strip('/') for u in urls[:-1]] + [urls[-1].lstrip('/')])


def get_function_name(function):
    """Gets the code name of function."""
    name = getattr(function, 'func_name', None)
    if name is None:
        name = getattr(function, '__name__', None)
    return name


class Counter(object):
    def __init__(self, initial_value):
        self._initial_value = initial_value
        self._value = initial_value

    def __repr__(self):
        return '<Counter init=%r now=%r>' % (self._initial_value, self._value)

    def incr(self):
        self._value += 1

    def get(self):
        return self._value

    def reset(self):
        self._value = self._initial_value


try:
    from setproctitle import setproctitle as _setproctitle

    def setproctitle(title):
        _setproctitle("huskar: %s" % title)
except ImportError:
    def setproctitle(title):
        return
