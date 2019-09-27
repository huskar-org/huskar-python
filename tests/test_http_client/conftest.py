# -*- coding: utf-8 -*-

import os
import time
import json
import logging

import gevent
import gevent.event
import gevent.monkey
import pytest
import requests
from mock import MagicMock

from huskar_sdk_v2.http.ioloops import IOLoop
from huskar_sdk_v2.http.ioloops.http import HuskarApiIOLoop
from huskar_sdk_v2.http.ioloops.file import FileCacheIOLoop
from huskar_sdk_v2.http.ioloops.entity import Component
from huskar_sdk_v2.http.components.config import Config
from huskar_sdk_v2.http.components.service import Service
from huskar_sdk_v2.http.components.switch import Switch


logging.basicConfig()
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(autouse=True)
def gevent_monkey_patch():
    gevent.monkey.patch_all()


@pytest.fixture
def requests_mock(monkeypatch):
    Session = MagicMock()

    class MockResponse(object):
        wait_time = 0
        stop_exception = None
        changed_data_processed = gevent.event.Event()
        pending_requests = []
        first_line = open(os.path.join(
            CURRENT_DIR, "test_data.txt")).read().strip()
        ok = True
        status_code = 200

        def __init__(self, *args, **kwds):
            self.changed_data_processed.clear()
            while self.pending_requests:
                self.pending_requests.pop()

        @classmethod
        def set_result_file(cls, filename):
            with open(os.path.join(CURRENT_DIR, filename)) as f:
                cls.pending_requests.extend(f.read().splitlines())
            cls.changed_data_processed.clear()

        def should_raise_exception(self):
            if self.stop_exception is not None:
                raise self.stop_exception

        @classmethod
        def set_error_mode(cls, status_code=401, message=''):
            cls.ok = False
            cls.status_code = status_code
            cls.text = message or (
                u'{"data": null, "message": "The token is missing",'
                u' "status": "Unauthorized"}')
            cls.content = cls.text
            cls.json = lambda cls: json.loads(cls.text)

        @classmethod
        def raise_for_status(cls):
            if cls.ok:
                return
            raise requests.HTTPError(response=cls)

        @classmethod
        def add_response(cls, line):
            cls.pending_requests.append(line)
            cls.changed_data_processed.clear()

        @classmethod
        def set_first_line(cls, line):
            cls.first_line = line

        @classmethod
        def wait_processed(cls, timeout=3.0):
            return cls.changed_data_processed.wait(timeout)

        def iter_lines(self, *args, **kwds):
            gevent.sleep(self.wait_time)
            self.should_raise_exception()
            yield self.first_line
            while True:
                self.should_raise_exception()
                while self.pending_requests:
                    req = self.pending_requests.pop()
                    yield req
                else:
                    if not self.changed_data_processed.is_set():
                        self.changed_data_processed.set()
                yield '{"body": {}, "message": "ping"}'
                time.sleep(1)

    session = Session()
    session.headers = {
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate',
        'Accept': '*/*',
        'User-Agent': 'mocked-requests/0.0.0'
    }
    session.post = MockResponse
    monkeypatch.setattr(requests, 'Session', Session)
    return MockResponse


@pytest.fixture
def cache_dir(worker_id, tmpdir):
    return str(tmpdir.mkdir('huskar_{}'.format(worker_id)))


@pytest.fixture
def clear_ioloop_instance(request):
    request.addfinalizer(IOLoop.clear_instance)
    request.addfinalizer(IOLoop.clear_configure)


@pytest.fixture
def empty_client(request, requests_mock, cache_dir, clear_ioloop_instance):
    client = HuskarApiIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    client.install()

    request.addfinalizer(lambda: client.stop(3))
    request.addfinalizer(Component.value_processors.clear)
    return client


@pytest.fixture
def client(empty_client):
    empty_client.watched_configs.add_watch("arch.test", 'overall')
    empty_client.watched_switches.add_watch("arch.test", 'another-cluster')
    empty_client.watched_services.add_watch("arch.test", 'alpha-stable')
    return empty_client


@pytest.fixture
def no_cache_initial_client(request, clear_ioloop_instance):
    client = HuskarApiIOLoop('test_url', 'test_token', cache_dir=None)
    client.install()
    request.addfinalizer(lambda: client.stop(3))
    return client


@pytest.fixture
def no_cache_client(no_cache_initial_client):
    no_cache_initial_client.run()
    return no_cache_initial_client


@pytest.fixture
def started_client(client):
    client.run()
    return client


# you can treat the file_cache_client is a HuskarClient run in another process,
# and it only read and update data from file cache.
@pytest.fixture
def file_cache_client(request, monkeypatch, cache_dir):
    monkeypatch.setattr(FileCacheIOLoop, 'try_to_be_writer', lambda self: None)
    ioloop = FileCacheIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    ioloop.check_file_stat_gap = 0.2
    request.addfinalizer(ioloop.stop)
    return ioloop


@pytest.fixture
def started_file_cache_client(file_cache_client):
    file_cache_client.run()
    return file_cache_client


@pytest.fixture
def fake_config_with_file_cache_client(file_cache_client):
    class FakeConfig(Config):
        @property
        def client(self):
            return file_cache_client.watched_configs
    return FakeConfig


@pytest.fixture
def fake_service_with_file_cache_client(file_cache_client):
    class FakeService(Service):
        @property
        def client(self):
            return file_cache_client.watched_services
    return FakeService


@pytest.fixture
def fake_switch_with_file_cache_client(file_cache_client):
    class FakeSwitch(Switch):
        @property
        def client(self):
            return file_cache_client.watched_switches
    return FakeSwitch


@pytest.fixture
def test_data():
    with open(os.path.join(CURRENT_DIR, 'test_data.txt'), 'r') as f:
        content = json.load(f)

    class singleton(object):
        config_content = content['body']['config']
        service_content = content['body']['service']
        switch_content = content['body']['switch']

    return singleton()


@pytest.fixture
def wait_huskar_api_ioloop_connected():
    def _(timeout):
        ioloop = IOLoop.current()
        assert isinstance(ioloop, HuskarApiIOLoop)
        assert ioloop.connected.wait(timeout)
    return _


@pytest.fixture
def sleep_ops(monkeypatch):
    origin_sleep = gevent.sleep

    class SleepOps(object):
        def __init__(self, time=None):
            self.time = time

        def sleep(self, time):
            if self.time is not None:
                origin_sleep(self.time)
            else:
                origin_sleep(time)

        def set_constant_sleep_time(self, time):
            self.time = time

    sleep_ops = SleepOps()
    monkeypatch.setattr("gevent.sleep", sleep_ops.sleep)
    return sleep_ops
