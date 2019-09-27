#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import mock
import gevent
from pytest import fixture
from huskar_sdk_v2.utils import combine

logging.basicConfig()

KEY = 'test'
TEST_CONFIG = 'test_config'
SLEEP_TIME = 0.2


@fixture
def full_path(huskar):
    path = combine(huskar.config.base_path,
                   TEST_CONFIG)
    return huskar.client.get_full_path(path)


# def test_get(config):
#     assert config.get(KEY) is None
#     assert config.configs == {}
#     config.configs[KEY] = "a"
#     assert config.get(KEY) == 'a'
#
#
# def test_chinese(config):
#     config.configs[KEY] = u"中文"
#     assert config.get(KEY) == u'中文'
#
#
# def test_watch(config):
#     config.configs[KEY] = "v"
#
#     mock_callback = Mock()
#
#     config.watch(KEY, mock_callback)
#     config._do_callback(KEY, "new_v")
#     mock_callback.assert_called_once_with("new_v")
#
#     config.unwatch_key(KEY)
#     config._do_callback(KEY, "bla")
#     mock_callback.assert_called_once_with("new_v")
#
#     config.watch(KEY, lambda x: x/0)
#     raises(TypeError, config._do_callback, KEY, "trigger")


@fixture
def overall_full_path(huskar):
    path = combine(huskar.config.overall_base_path,
                   TEST_CONFIG)
    return huskar.client.get_full_path(path)


@fixture
def config(huskar):
    huskar.client.ensure_path(huskar.config.overall_base_path)
    huskar.client.ensure_path(huskar.config.base_path)
    return huskar.config


def test_lazy(huskar, config, full_path):
    huskar.client.call_client('create', full_path, b'gold', ephemeral=True)
    assert not config.started
    assert config.get(TEST_CONFIG) == u'gold'
    assert config.started


def test_lazy_off(huskar, config, full_path):
    huskar.client.call_client('create', full_path, b'gold', ephemeral=True)
    assert not config.started
    config.lazy = False
    assert not config.watch(TEST_CONFIG, lambda x: None)
    assert not config.started


def test_config_chinese(huskar, config, full_path):
    huskar.client.create(full_path, u'中文', ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == u'中文'


def test_config(huskar, config, full_path):
    huskar.client.call_client('create', full_path, b'a', ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == 'a'


def test_slash_key(huskar, config, base_path):
    huskar.client.call_client(
        'create',
        base_path + '/config/test_service/test_cluster/%SLASH%',
        b'a',
        ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get('/') == 'a'


def test_config_changes_chinese(huskar, config, full_path):
    gevent.sleep(SLEEP_TIME)
    huskar.client.create(full_path, u'中文字符', ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == u'中文字符'

    huskar.client.set_data(full_path, u'另一个中文字符fuck')
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == u'另一个中文字符fuck'


def test_config_changes(huskar, config, full_path):
    gevent.sleep(SLEEP_TIME)
    huskar.client.call_client('create', full_path, b'a', ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == 'a'

    huskar.client.call_client('set', full_path, b'b')
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == 'b'


def test_overall_config_changes(huskar, config, full_path, overall_full_path):
    huskar.client.call_client('create', overall_full_path, b'overall_value',
                              ephemeral=True)
    huskar.client.call_client('create', full_path, b'value', ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == 'value'
    assert config.get(TEST_CONFIG, _force_overall=True) == 'overall_value'

    mock_handler = mock.Mock()
    config.on_change(TEST_CONFIG)(mock_handler)

    huskar.client.call_client('delete', full_path)
    gevent.sleep(SLEEP_TIME)
    mock_handler.assert_called_once_with(u'overall_value')
    assert config.get(TEST_CONFIG) == 'overall_value'


def test_watch(huskar, config, overall_full_path):
    huskar.client.call_client('create', overall_full_path, b'value',
                              ephemeral=True)

    def callback(x):
        callback.value = x
    callback.value = None

    gevent.sleep(SLEEP_TIME)
    config.watch(TEST_CONFIG, callback)
    huskar.client.call_client("set", overall_full_path, b"new_value")
    gevent.sleep(SLEEP_TIME)
    assert callback.value == "new_value"
    config.unwatch_key(TEST_CONFIG)
    huskar.client.call_client('set', overall_full_path, b"blabla")
    assert callback.value == "new_value"
    config.watch(TEST_CONFIG, lambda x: 1 / 0)
    huskar.client.call_client('set', overall_full_path, b"trigger")


def test_on_change(huskar, config, overall_full_path):
    huskar.client.call_client('create', overall_full_path, b'value',
                              ephemeral=True)

    @config.on_change(TEST_CONFIG)
    def callback(x):
        callback.called += 1
        callback.value = x
    callback.value = None
    callback.called = 0

    assert callback.value is None
    assert callback.called == 0
    huskar.client.call_client("set", overall_full_path, b"new_value")
    gevent.sleep(SLEEP_TIME)
    assert callback.value == "new_value"
    assert callback.called == 1


def test_config_cache(Huskar, cache_huskar, full_path):
    config = cache_huskar.config
    cache_huskar.client.call_client('create', full_path, b'a', makepath=True,
                                    ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert config.get(TEST_CONFIG) == 'a'
    gevent.sleep(SLEEP_TIME)
    cache_huskar.client.stop()
    gevent.sleep(SLEEP_TIME*10)
    assert not config.client.connected
    assert config.get(TEST_CONFIG) == 'a'  # hot cache
    assert not config.client.connected
    cache_huskar.stop()

    h = Huskar(service='test_service',
               servers='host_that_not_exists',
               cluster='test_cluster',
               lazy=True, cache_dir=cache_huskar.cache_dir)
    assert not h.client.connected
    assert h.config.get(TEST_CONFIG) == 'a'  # cold cache


def test_stop(cache_huskar):
    cache_huskar.config.stop()
    cache_huskar.config.get(TEST_CONFIG)


def test_serialization(cache_huskar, full_path):
    c = cache_huskar.config
    cache_huskar.client.call_client("create", full_path, b'a', makepath=True,
                                    ephemeral=True)
    gevent.sleep(SLEEP_TIME)
    assert 'a' == c.get(TEST_CONFIG)
    cache_huskar.client.call_client("set", full_path, b"true")
    gevent.sleep(SLEEP_TIME)
    assert c.get(TEST_CONFIG) is True


def test_iteritems(huskar, full_path, overall_full_path):
    c = huskar.config

    def get_full_path(path):
        path = combine(huskar.config.base_path, path)
        return huskar.client.get_full_path(path)

    def get_full_overall_path(path):
        path = combine(huskar.config.overall_base_path, path)
        return huskar.client.get_full_path(path)

    huskar.client.call_client(
        "create", get_full_path('test_config_2'), b'a', makepath=True,
        ephemeral=True
        )
    gevent.sleep(SLEEP_TIME)

    huskar.client.call_client(
        "create", get_full_path('test_config'), b'a', makepath=True,
        ephemeral=True
        )
    gevent.sleep(SLEEP_TIME)

    huskar.client.call_client(
        "create", get_full_overall_path('test_config'), b'b', makepath=True,
        ephemeral=True
        )
    gevent.sleep(SLEEP_TIME)

    assert 'a' == c.get(TEST_CONFIG)
    assert list(c.iteritems()) == list({
        'test_config': 'a',
        'test_config_2': 'a'
        }.items())
