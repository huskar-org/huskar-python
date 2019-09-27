# -*- coding: utf-8 -*-

import gevent
from pytest import fixture

from huskar_sdk_v2.bootstrap.client import BaseClient
from huskar_sdk_v2.utils import combine


@fixture
def test_key():
    return 'test_key'


@fixture
def test_full_path(base_path, test_key):
    return combine(base_path, test_key)


@fixture
def huskar_client(request, servers, base_path):
    c = BaseClient(servers, '', '', base_path)
    request.addfinalizer(c.stop)
    c.start()
    return c


def test_start_stop(servers, test_key, base_path):
    c = BaseClient(servers, '', '', base_path)
    assert not c.connected
    c.start()
    gevent.sleep(1)
    assert c.connected
    c.stop()
    assert not c.connected
    c.lazy = False
    # will not raise
    c.create(test_key, '1')

    c = BaseClient(servers, '', '', base_path, local_mode=True)
    c.create(test_key, '1')


def test_lazy(servers, test_key, base_path):
    c = BaseClient(servers, '', '', base_path, lazy=True)
    assert not c.connected
    c.exists(test_key)
    assert c.connected
    c.stop()
    assert not c.connected
    c.exists(test_key)
    assert c.connected

    c.lazy = False
    c.stop()
    assert not c.connected
    assert not c.exists(test_key)
    c.start()
    # will not raise
    c.exists(test_key)


def test_create_delete(huskar_client, test_key, base_path):
    huskar_client.ensure_path(base_path)

    huskar_client.create(test_key, '1')
    assert huskar_client.exists(test_key)
    huskar_client.delete(test_key)
    assert not huskar_client.exists(test_key)


def test_set_get(huskar_client, test_key, base_path):
    huskar_client.ensure_path(base_path)

    data = 'somedata'
    huskar_client.create(test_key, '1', ephemeral=True)
    huskar_client.set_data(test_key, data)
    value, state = huskar_client.get(test_key)
    assert value == data


def test_exception(huskar_client):
    assert not huskar_client.watch_key('nonesense')


def test_watch_key(huskar_client, test_key, test_full_path):
    huskar_client.client.create(test_full_path, b'1', makepath=True)
    huskar_client.watch_key(test_key)

    def handler(*args):
        handler.called = True
    handler.called = False

    huskar_client.watched_blinker.signal(test_key).connect(handler)
    huskar_client.client.set(test_full_path, b'changed')
    gevent.sleep(1)
    assert handler.called

    handler.called = False
    huskar_client.client.delete(test_full_path)
    gevent.sleep(1)
    assert handler.called

    handler.called = False
    huskar_client.client.create(test_full_path, b'1', makepath=True)
    # the signal should be disconnected after this
    huskar_client.stop()
    huskar_client.start()
    huskar_client.client.set(test_full_path, b'changed again')
    gevent.sleep(1)
    huskar_client.client.delete(test_full_path)
    assert not handler.called


def test_watch_path(huskar_client, test_key, test_full_path):

    def handler(children):
        handler.children = children

    huskar_client.client.create(test_full_path, b'', makepath=True)
    huskar_client.watch_path(test_key, handler)

    handler.children = None
    huskar_client.client.create(test_full_path + '/foo', b'', makepath=True)
    gevent.sleep(0.5)
    assert handler.children == ['foo']

    handler.children = None
    huskar_client.client.delete(test_full_path + '/foo')
    gevent.sleep(0.5)
    assert handler.children == []

    handler.children = None
    huskar_client.client.delete(test_full_path)
    gevent.sleep(0.5)
    assert handler.children is None
    data_watch, children_watch = huskar_client.watched_path[test_key]
    assert not data_watch._stopped
    assert children_watch._stopped

    handler.children = None
    huskar_client.client.create(test_full_path + '/bar', b'', makepath=True)
    gevent.sleep(0.5)
    assert handler.children == ['bar']
    assert not data_watch._stopped
    assert not children_watch._stopped
