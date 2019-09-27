# -*- coding: utf-8 -*-
import os
import signal
import time
import errno
import random
import fcntl
import requests

import pytest
import gevent
from mock import Mock
from gevent.event import Event
from huskar_sdk_v2.exceptions import (
    HuskarDiscoveryException, HuskarDiscoveryServerError,
    HuskarDiscoveryUserError)
from huskar_sdk_v2.http.patterns import Configurable
from huskar_sdk_v2.http.ioloops import IOLoop
from huskar_sdk_v2.http.ioloops.http import HuskarApiIOLoop
from huskar_sdk_v2.http.ioloops.file import FileCacheIOLoop


@pytest.fixture
def install_sigchld():
    def sig_child(signo, frame):
        while 1:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid <= 0:
                    break
            except OSError as e:
                if e.errno == errno.ECHILD:
                    break
    signal.signal(signal.SIGCHLD, sig_child)


@pytest.fixture
def patch_FileCacheIOLoop(monkeypatch):
    def check_stat(self):
        return
    monkeypatch.setattr(FileCacheIOLoop, 'start_check_file_stat', check_stat)


@pytest.fixture
def deleter(request):
    IOLoop.clear_configure()
    request.addfinalizer(IOLoop.clear_instance)


def set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def test_configurable(deleter):
    class P(Configurable):
        ENV = False

        def initialize(self):
            pass

        @classmethod
        def configurable_base(cls):
            return P

        @classmethod
        def configurable_default(cls):
            if P.ENV:
                return PP
            return PPP

    class PP(P):
        pass

    class PPP(P):
        pass

    assert isinstance(P(), PPP)
    P.clear_configure()
    P.ENV = True
    assert isinstance(P(), PP)

    P.configure(PP)
    assert isinstance(P(), PP)
    P.configure(PPP)
    assert isinstance(P(), PPP)


def test_IOLoop_with_one_writer_and_multiple_reader_processes(
        deleter, cache_dir):
    IOLoop.set_lockpath(os.path.join(cache_dir, 'huskar.writer'))
    wr, ww = os.pipe()
    rr, rw = os.pipe()

    num = 20
    for _ in range(num):
        pid = os.fork()
        if pid == 0:
            os.close(wr)
            os.close(rr)

            ioloop = IOLoop('test', 'test', cache_dir)
            if isinstance(ioloop, HuskarApiIOLoop):
                os.write(ww, b'1')
            elif isinstance(ioloop, FileCacheIOLoop):
                os.write(rw, b'1')

            os.close(ww)
            os.close(rw)

            time.sleep(3)  # do not release lock
            os._exit(os.EX_OK)

    os.close(ww)
    os.close(rw)

    time.sleep(1)
    writers, readers = os.read(wr, 20), os.read(rr, 20)

    os.close(wr)
    os.close(rr)

    try:
        assert len(writers) == 1
        assert len(readers) == (num-1)
    finally:
        [os.wait() for _ in range(num)]


def test_IOLoop_with_reader_become_writer_when_writer_failed(
        cache_dir, requests_mock, patch_FileCacheIOLoop, install_sigchld,
        deleter):
    IOLoop.set_lockpath(os.path.join(cache_dir, 'huskar.writer'))
    wr, ww = os.pipe()
    rr, rw = os.pipe()

    num = 20
    for _ in range(num):
        pid = os.fork()
        if pid == 0:
            os.close(wr)
            os.close(rr)

            requests_mock.set_result_file('test_data_changed.txt')
            IOLoop('test', 'test', cache_dir=cache_dir).install()
            if isinstance(IOLoop.current(), HuskarApiIOLoop):
                os.write(ww, ('{}'.format(os.getpid())).encode('utf-8'))
            elif isinstance(IOLoop.current(), FileCacheIOLoop):
                IOLoop.current().retry_acquire_gap = 0.3
                os.write(rw, b'1')
            IOLoop.current().run()

            gevent.sleep(2)

            if isinstance(IOLoop.current(), HuskarApiIOLoop):
                os.write(ww, ('{}'.format(os.getpid())).encode('utf-8'))
            elif isinstance(IOLoop.current(), FileCacheIOLoop):
                os.write(rw, b'1')

            os.close(ww)
            os.close(rw)
            time.sleep(2)
            os._exit(os.EX_OK)

    time.sleep(1)
    os.close(ww)
    os.close(rw)

    try:
        writerpid, readers = os.read(wr, 10), os.read(rr, 20)
        assert len(readers) == (num-1)

        os.kill(int(writerpid), signal.SIGKILL)

        time.sleep(2)
        writerpid, readers = os.read(wr, 10), os.read(rr, 20)
        os.kill(int(writerpid), 0)
        assert len(readers) == (num-2)
    finally:
        os.close(wr)
        os.close(rr)
        time.sleep(2)  # wait process die


def test_IOLoop_with_only_one_writer_exists_no_matter_what_happens(
        cache_dir, requests_mock, patch_FileCacheIOLoop, install_sigchld,
        deleter):
    IOLoop.set_lockpath(os.path.join(cache_dir, 'huskar.writer'))
    processes = {}

    def new_process():
        r, w = os.pipe()
        r, w = os.fdopen(r, 'r'), os.fdopen(w, 'w')
        set_non_blocking(r)
        set_non_blocking(w)
        pid = os.fork()
        if pid < 0:
            return
        elif pid == 0:  # child
            r.close()

            requests_mock.set_result_file('test_data_changed.txt')
            IOLoop('test', 'test', cache_dir=cache_dir).install()
            if isinstance(IOLoop.current(), FileCacheIOLoop):
                IOLoop.current().retry_acquire_gap = 0.5

            stoped = Event()

            # simulate framework behavior
            def _exit(signum, frame):
                stoped.set()
                IOLoop.current().stop()
                IOLoop.clear_instance()
                os._exit(os.EX_OK)
            signal.signal(signal.SIGTERM, _exit)

            IOLoop.current().run()

            while not stoped.is_set():
                if isinstance(IOLoop.current(), HuskarApiIOLoop):
                    w.write('{}\n'.format(os.getpid()))
                else:
                    w.write('0\n')
                w.flush()
                gevent.sleep(0.5)

            w.close()
            os._exit(os.EX_OK)
        else:
            w.close()
            processes[pid] = r

    def kill_process(pid):
        os.kill(pid, signal.SIGTERM)
        processes[pid].close()
        del processes[pid]

    def ensure_one():
        writers = set()
        for _ in range(10):
            for pid, r in processes.items():
                try:
                    data = r.readline()
                except IOError as e:
                    if e.errno == errno.EWOULDBLOCK:
                        continue
                if not data:
                    continue
                if int(data.strip()) == pid:
                    try:
                        os.kill(pid, 0)
                        writers.add(pid)
                    except OSError:
                        if pid in writers:
                            writers.remove(pid)
            time.sleep(0.5)

        assert len(writers) == 1
        return list(writers)[0]

    num = 20
    for _ in range(num):
        new_process()

    writer = ensure_one()

    for _ in range(12):
        if random.random() <= 0.5:
            kill_process(writer)
            num -= 1
        else:
            nkill = random.randint(2, 6)
            for _ in range(nkill):
                kill_process(random.choice(list(processes.keys())))  # Py3
            num -= nkill

        writer = ensure_one()

        if num < 10:
            nlaunch = random.randint(11 - num, 10)
            for _ in range(nlaunch):
                new_process()
            num += nlaunch
        elif num < 20 and random.random() >= 0.5:
            new_process()
            num += 1

        same_writer = ensure_one()
        assert writer == same_writer

    for pid in list(processes.keys()):  # Py3
        kill_process(pid)
        if len(processes) > 0:
            ensure_one()


def test_IOLoop_replacement(cache_dir):
    def func():
        pass

    ioloop = FileCacheIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    ioloop.install()

    ioloop.watched_configs.set_default_fail_strategy_to_raise()

    ioloop.watched_configs.add_watch('foo', 'bar')
    ioloop.watched_configs.add_listener_for_app_id_at_cluster(
        'app_foo', 'cluster_foo', func)
    ioloop.watched_services.add_watch('bar', 'baz')
    ioloop.watched_services.add_listener_for_app_id_at_cluster(
        'app_bar', 'cluster_bar', func)
    ioloop.watched_switches.add_watch('baz', 'foo')
    ioloop.watched_switches.add_listener_for_app_id_at_cluster(
        'app_baz', 'cluster_baz', func)

    assert isinstance(IOLoop.current(), FileCacheIOLoop)

    HuskarApiIOLoop(ioloop.url, ioloop.token, ioloop.cache_dir).install()
    ioloop = IOLoop.current()

    assert isinstance(ioloop, HuskarApiIOLoop)
    configs = ioloop.watched_configs
    services = ioloop.watched_services
    switches = ioloop.watched_switches
    assert 'foo' in configs.app_id_cluster_map
    assert 'bar' in services.app_id_cluster_map
    assert 'baz' in switches.app_id_cluster_map
    assert ('app_foo', 'cluster_foo') in configs.event_listeners
    assert ('app_bar', 'cluster_bar') in services.event_listeners
    assert ('app_baz', 'cluster_baz') in switches.event_listeners
    assert configs.default_fail_strategy == configs.FAIL_STRATEGY_RAISE
    IOLoop.clear_instance()


def test_http_ioloop_polling_error_hook_server_error(
        client, cache_dir, monkeypatch, requests_mock):
    handler = Mock()

    client.add_listener('polling_error', handler)
    client.run()
    assert client.connected.wait(1)
    requests_mock.stop_exception = requests.Timeout()
    assert client.is_disconnected.wait(3)
    assert handler.call_count == 1
    exc = handler.call_args[0][0]
    assert isinstance(exc, HuskarDiscoveryException)
    assert exc.orig_exc == requests_mock.stop_exception


def test_http_ioloop_polling_error_hook_500_error(
        client, cache_dir, monkeypatch, requests_mock):
    handler = Mock()

    requests_mock.set_error_mode(500, 'error')
    client.add_listener('polling_error', handler)
    client.run()
    assert client.is_disconnected.wait(3)
    assert handler.call_count == 1
    exc = handler.call_args[0][0]
    assert isinstance(exc, HuskarDiscoveryServerError)
    assert isinstance(exc.orig_exc, requests.HTTPError)
    assert exc.orig_exc.response.status_code == 500


def test_http_ioloop_polling_error_hook_user_error(
        client, cache_dir, monkeypatch, requests_mock):
    handler = Mock()

    requests_mock.set_error_mode(401)
    client.add_listener('polling_error', handler)
    client.run()
    assert client.is_disconnected.wait(3)
    assert handler.call_count == 1
    exc = handler.call_args[0][0]
    assert isinstance(exc, HuskarDiscoveryUserError)
    assert isinstance(exc.orig_exc, requests.HTTPError)
    assert exc.orig_exc.response.status_code == 401


def test_hooks_should_pass_to_new_ioloop(cache_dir, client, monkeypatch):
    handler = Mock()
    client.add_listener('polling_error', handler)
    assert client.stop(3)

    ioloop = FileCacheIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    ioloop.install()

    assert handler in ioloop.event_listeners['polling_error']
