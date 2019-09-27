# -*- coding: utf-8 -*-

import logging
import gevent
from pytest import fixture
from huskar_sdk_v2.utils import combine

logging.basicConfig()

key = 'test_switch_func_key'
ON = b'100'
HALF = b'50'
OFF = b'0'


@fixture
def node_dir(huskar):
    return combine(huskar.base_path, huskar.switch.base_path)


@fixture
def overall_node_dir(huskar):
    return combine(huskar.base_path, huskar.switch.overall_base_path)


@fixture
def switch(huskar, node_dir, overall_node_dir, request):
    request.addfinalizer(huskar.stop)
    huskar.client.call_client('ensure_path', node_dir)
    huskar.client.call_client('ensure_path', overall_node_dir)
    return huskar.switch


def test_lazy(switch, node_dir):
    full_path = combine(node_dir, key)
    switch.client.call_client('create', full_path, OFF, ephemeral=True)
    assert not switch.started
    while switch.is_switched_on(key):
        gevent.sleep(0.5)
    assert not switch.is_switched_on(key)
    assert switch.started


def test_lazy_off(switch, node_dir):
    full_path = combine(node_dir, key)
    switch.lazy = False
    switch.client.call_client('create', full_path, OFF, ephemeral=True)
    assert not switch.started
    assert switch.is_switched_on(key)
    assert not switch.started


def test_default(switch):
    switch.set_default_state(True)
    assert switch.is_switched_on(key + 'blabla')
    switch.set_default_state(False)
    assert not switch.is_switched_on(key + 'blabla')


def test_off(switch, node_dir):
    full_path = combine(node_dir, key)
    switch.client.call_client('create', full_path, OFF, ephemeral=True)

    @switch.bind(key)
    def switch_func():
        switch_func.called = True
    switch_func.called = False

    gevent.sleep(1)
    switch_func()
    assert not switch_func.called
    assert not switch.is_switched_on(key)


def test_float(switch, node_dir):
    switch.start()
    gevent.sleep(1)

    full_path = combine(node_dir, key)
    switch.client.call_client(
        'create', full_path, b'0.0000001', ephemeral=True)
    gevent.sleep(0.5)
    assert not switch.is_switched_on(key)

    switch.client.call_client('set', full_path, b'gibberis')
    gevent.sleep(0.5)
    assert not switch.is_switched_on(key)


def test_bind(switch, node_dir):
    full_path = combine(node_dir, key)
    switch.client.call_client('create', full_path, ON, ephemeral=True)

    gevent.sleep(1)

    @switch.bind(key)
    def switch_func():
        switch_func.called = True
    switch_func.called = False

    switch_func()
    assert switch.is_switched_on(key)
    assert switch_func.called
    switch_func.called = False

    switch.client.call_client('set', full_path, OFF)

    gevent.sleep(1)

    switch_func()
    assert not switch.is_switched_on(key)
    assert not switch_func.called


def test_default_value(switch, node_dir):
    full_path = combine(node_dir, 'func')
    switch.client.call_client('create', full_path, ON, ephemeral=True)

    @switch.bind(default=u'default')
    def func():
        return u'value'

    assert func() == u'value'
    switch.client.call_client('set', full_path, OFF)
    gevent.sleep(1)
    assert func() == u'default'

    del func

    @switch.bind('func', default=list)
    def func2():
        return [1]

    switch.client.call_client('set', full_path, ON)
    gevent.sleep(1)
    assert func2() == [1]
    switch.client.call_client('set', full_path, OFF)
    gevent.sleep(1)
    assert func2() == []


def test_overall_switch(switch, node_dir, overall_node_dir):
    full_path = combine(node_dir, key)
    overall_full_path = combine(overall_node_dir, key)
    switch.client.call_client('create', overall_full_path, OFF, makepath=True,
                              ephemeral=True)

    switch.start()
    gevent.sleep(1)

    @switch.bind(key)
    def switch_func():
        switch_func.called = True
    switch_func.called = False

    switch_func()
    assert not switch.is_switched_on(key)
    assert not switch_func.called
    switch_func.called = False

    switch.client.call_client('create', full_path, ON, ephemeral=True)

    gevent.sleep(1)

    switch_func()
    assert switch.is_switched_on(key)
    assert switch_func.called


def test_slash_key(huskar, switch, base_path):
    huskar.client.call_client(
        'create',
        combine(base_path, 'switch/test_service/test_cluster/%SLASH%'),
        OFF,
        ephemeral=True)
    switch.start()
    gevent.sleep(0.2)
    assert not switch.is_switched_on('/')


def test_switch_cache(Huskar, cache_huskar, node_dir):
    full_path = combine(node_dir, key)
    switch = cache_huskar.switch
    switch.start()
    cache_huskar.client.call_client('create', full_path, OFF, makepath=True,
                                    ephemeral=True)
    gevent.sleep(1)
    assert not switch.is_switched_on(key)
    cache_huskar.client.stop()
    gevent.sleep(1)
    assert not switch.is_switched_on(key)  # hot cache

    h = Huskar(service='test_service',
               servers='host_that_not_exists',
               cluster='test_cluster',
               lazy=True, cache_dir=cache_huskar.cache_dir)
    assert not h.switch.is_switched_on(key)  # cold cache


def test_switch_cache_consistency(Huskar, servers, cache_huskar,
                                  huskar, node_dir):
    full_path = combine(node_dir, key)

    cache_huskar.start()
    cache_huskar.switch.client.call_client('create', full_path, OFF,
                                           makepath=True)
    gevent.sleep(1)
    assert OFF == cache_huskar.client.call_client('get', full_path)[0]
    assert not cache_huskar.switch.is_switched_on(key)
    cache_huskar.stop()  # stop huskar

    # change the value of key in zk
    huskar.client.call_client('set', full_path, ON)
    huskar.stop()

    # reconnect using cache
    h = Huskar(servers=servers, service='test_service',
               cluster='test_cluster', cache_dir=cache_huskar.cache_dir)
    h.start()
    gevent.sleep(1)
    # h = cache_huskar  FIXME: uncomment this then test fails
    assert ON == h.client.call_client('get', full_path)[0]
    try:
        assert h.switch.is_switched_on(key)
    finally:
        h.client.call_client('delete', full_path)
        h.stop()


def test_iteritems(huskar, switch, overall_node_dir, node_dir):
    s = switch

    def get_full_path(path):
        return combine(node_dir, path)

    def get_full_overall_path(path):
        return combine(overall_node_dir, path)

    huskar.client.call_client(
        "create", get_full_path('test_config_2'), ON, makepath=True,
        ephemeral=True
        )

    huskar.client.call_client(
        "create", get_full_path('test_config'), OFF, makepath=True,
        ephemeral=True
        )

    huskar.client.call_client(
        "create", get_full_overall_path('test_config'), ON, makepath=True,
        ephemeral=True
        )

    s.start()
    gevent.sleep(1)

    assert list(s.iteritems()) == list({
        'test_config': 0.0,
        'test_config_2': 100.0,
        }.items())

    assert dict(s.switches) == {
        'test_config': {'path': 'switch/test_service/test_cluster',
                        'value': 0.0},
        'test_config_2': {'path': 'switch/test_service/test_cluster',
                          'value': 100.0},
        }
