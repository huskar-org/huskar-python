# -*- coding: utf-8 -*-

import os
import string
import functools
from logging import getLogger
from threading import Thread
from multiprocessing import Process

from pytest import fixture, mark

from huskar_sdk_v2.utils.cached_dict import CachedDict


# from logging import basicConfig, DEBUG
# basicConfig()
# getLogger("huskar_sdk_v2.utils.cached_dict").setLevel(DEBUG)
logger = getLogger(__name__)

THREAD_NUM = 1000
PROCESS_NUM = 100


@fixture
def new_d(cache_dir):
    return functools.partial(CachedDict, str(cache_dir.join('test.json')))


@fixture
def d(request, new_d):
    return new_d()


@fixture
def cd(d):
    d.clear()
    return d


@fixture
def data():
    data = {}
    for k in string.ascii_letters:
        data[k] = k.upper()
    return data


@fixture
def dd(cd, data):
    cd.update(data)
    return cd


def test_type(d, new_d):
    for value in ([],
                  # NOT supported:
                  # (1, 2),     tuple
                  # open,       function
                  # CachedDict, custom class
                  # "中文 str (non-ascii str)",
                  [1, 2],
                  {"a": 1},
                  {"b": [1, 2, 3]},
                  "ascii str",
                  u"中文 unicode",
                  True,
                  False):
        d['a'] = value
        assert d.get('a') == value
        logger.debug(type(new_d().get('a')), type(value))
        assert new_d().get('a') == value


def test_update(cd):
    d1 = {'a': 'a',
          'b': 'b'}
    d2 = {'a': 2,
          'c': 3}

    cd.update(d1)
    assert cd == d1
    cd.update(d2)

    d1.update(d2)
    assert cd == d1


def test_pop(dd):
    dd.pop('a')
    assert 'a' not in dd
    b = dict(dd)
    dd.pop('?', None)
    assert dd == b


def test_items(data, dd):
    assert set(dd.keys()) == set(data.keys())
    assert set(dd.values()) == set(data.values())
    assert list(dd.items()) == list(zip(dd.keys(), dd.values()))


def test_in(data, dd):
    assert 'a' in dd
    assert '?' not in dd


def test_bool(dd):
    assert dd
    dd.clear()
    assert not dd
    dd[''] = ''
    assert dd


def test_len(data, dd):
    assert len(data) == len(dd)
    dd.clear()
    assert len(dd) == 0


def test_persistence(dd, new_d, data):
    dd.pop('a')
    data.pop('a')

    nd = new_d()
    assert dict(nd) == data

    dd['?'] = '?'
    data['?'] = '?'
    assert dict(new_d()) == data

    dd.clear()
    assert not new_d()

    x = dict(dd)
    del dd
    assert x == dict(new_d())


@mark.xfail
def test_version(dd, new_d):
    data = dict(dd)
    dd.close()
    assert new_d() == data
    assert not new_d(ensure_version=999)


def test_multithreading(pool_map, cd, new_d):
    cd['x'] = 0

    def write(n):
        cd[n] = 1

    pool_map(Thread, write, range(THREAD_NUM))

    nd = new_d()
    for n in range(THREAD_NUM):
        assert n not in nd
        assert n in cd


@mark.xfail
def test_multiprocessing(pool_map, new_d):
    d = new_d()
    d.clear()

    def write(n):
        d = new_d()
        d[n] = 1

    pool_map(Process, write, range(PROCESS_NUM))

    d.reload()
    for n in range(PROCESS_NUM):
        assert n in d


def test_single_writer(pool_map, cd, new_d):
    cd['x'] = 0

    def write(n):
        cd[str(n)] = 1

    pool_map(Thread, write, range(THREAD_NUM))

    d = new_d()
    assert len(d) == 1
    assert d['x'] == 0


def _fwrite(filename, data, pos=0):
    fd = os.open(filename, os.O_WRONLY)
    if pos:
        os.lseek(fd, pos, 0)
    os.write(fd, data)
    os.close(fd)


def test_corrupt(cd, new_d):
    cd['x'] = 1

    # corrupt the database file
    _fwrite(cd.filename, b"garabage", 48)

    cd.release_write()

    d = new_d()
    assert not d
    d['s'] = 1
    assert new_d()['s'] == 1


def test_permission(cd):
    cd["x"] = 1
    assert oct(os.stat(cd.filename).st_mode & 0o777) == oct(0o666)
    assert oct(os.stat(cd.writer_lock.filename).st_mode & 0o777) == oct(0o666)
