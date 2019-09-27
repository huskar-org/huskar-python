import sys
import time
from threading import Thread
from multiprocessing import Process

from pytest import fixture, mark

from huskar_sdk_v2.utils.filelock import FileLock

# from huskar_sdk_v2.utils.filelock import logger
# from logging import basicConfig, DEBUG
# basicConfig()
# logger.setLevel(DEBUG)

PROCESS_NUM = 100


@fixture
def lock(request, cache_dir):
    lock = FileLock(str(cache_dir.join('test.lock')))
    request.addfinalizer(lock.release)
    return lock


@fixture
def new_lock(request, cache_dir):
    def _new():
        lock = FileLock(str(cache_dir.join('test.lock')))
        request.addfinalizer(lock.release)
        return lock
    return _new


def test_acquire(lock):
    assert lock.acquire()
    assert lock.acquire()


@mark.skipif(sys.version_info > (3, 0),
             reason="FIXME: requires Python2")
def test_release(go, cache_dir):
    def _new():
        return FileLock(str(cache_dir.join('test.lock')))

    lock = _new()
    assert lock.acquire()

    assert not go(lambda: _new().acquire())
    lock = None
    assert go(lambda: _new().acquire())


def test_exclusive(pool_map, lock, new_lock):
    def acquire(_):
        return new_lock().acquire() or lock.acquire()

    # test thread
    with lock:
        for acquired in pool_map(Thread, acquire, range(10)):
            assert acquired is False
    assert pool_map(Thread, acquire, range(1))[0]

    # test process
    with lock:
        for acquired in pool_map(Process, acquire, range(10)):
            assert acquired is False
    assert pool_map(Process, acquire, range(1))[0]


def test_concurrency(pool_map, new_lock):
    def acquire(_):
        acquired = new_lock().acquire()
        if acquired:
            time.sleep(10)  # hold the lock
        return acquired

    acquired_count = 0
    for acquired in pool_map(Process, acquire, range(PROCESS_NUM)):
        if acquired:
            acquired_count += 1
        else:
            assert acquired is False
    assert acquired_count == 1
