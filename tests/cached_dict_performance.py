#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import timeit
from multiprocessing import Pool

N = 5
NUMBER = 100000

IMPORT = "from huskar_sdk_v2.utils.cached_dict import CachedDict as CD;"
INIT_NORMAL = "d = {'a': 'x' * 100};"
INIT_CACHE = ';'.join([
    "d = CD(filename='/tmp/test/test.db')",
    "d['a'] = 'x' * 100;"])


def time_normal_r(_=None):
    setup = IMPORT + INIT_NORMAL
    return timeit.timeit("d.get('a')", setup, number=NUMBER)


def time_cache_r(_=None):
    setup = IMPORT + INIT_CACHE
    return timeit.timeit("d.get('a')", setup, number=NUMBER)


def time_normal_w(_=None):
    setup = IMPORT + INIT_NORMAL
    return timeit.timeit("d['a']='x'", setup, number=NUMBER)


def time_cache_w(_=None):
    setup = IMPORT + INIT_CACHE
    return timeit.timeit("d['a']='x'", setup, number=NUMBER)


if __name__ == '__main__':
    tests = (time_normal_r, time_cache_r, time_normal_w, time_cache_w)

    print("Number: {}\nPool size: {}\n".format(NUMBER, N))

    for f in tests:
        print("{:<15}: {}".format(f.__name__, f()))

    pool = Pool(N)
    for f in tests:
        print("{:=^80}".format(f.__name__))
        normal_result = pool.map(f, range(N))
        print("MAX: ", max(normal_result))
        print("MIN: ", min(normal_result))
        print("AVG: ", sum(normal_result) / N)
