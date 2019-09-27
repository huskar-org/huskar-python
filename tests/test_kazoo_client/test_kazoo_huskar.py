# -*- coding: utf-8 -*-

import logging

import gevent
import mock
import pytest


test_key = 'test_key'

logger = logging.getLogger(__name__)


def test_offline(offline_huskar):
    s = offline_huskar.switch
    c = offline_huskar.config

    assert not c.watch(test_key, lambda x: None)
    # will not raise
    s.is_switched_on(test_key)
    s.start()

    @s.bind()
    def f():
        pass

    # Config operation should fail and switch should not, if ZK is offline.
    with pytest.raises(RuntimeError):
        c.get(test_key)


def test_local_mode(Huskar, servers, cache_dir):
    h = Huskar(servers=servers,
               cluster="test",
               service="test",
               cache_dir=str(cache_dir),
               local_mode=True)

    assert not h.config.watch(test_key, lambda x: None)
    gevent.sleep(1)
    assert not h.config.get(test_key)
    assert h.switch.is_switched_on(test_key)


def test_fallback(Huskar, servers, cache_dir):
    import os  # noqa
    with mock.patch("huskar_sdk_v2.utils.cached_dict.CachedDict.init",
                    side_effect=Exception()):
        h = Huskar(service="test", servers=servers, cluster="test",
                   lazy=True, cache_dir=str(cache_dir))
        h.start()
        gevent.sleep(1)
    assert isinstance(h.config.configs, dict)
    assert isinstance(h.switch.switches, dict)


def test_lazy(Huskar, servers, cache_dir):
    def make_huskar(lazy):
        return Huskar(
            servers=servers,
            cluster='test',
            service='test',
            cache_dir=str(cache_dir),
            lazy=lazy
        )

    h = make_huskar(True)
    assert not h.client.connected
    h.client.exists(test_key)
    assert h.client.connected
    h.client.stop()
    assert not h.client.connected
    h.client.exists(test_key)
    assert h.client.connected

    h = make_huskar(False)
    assert not h.client.connected
    h.client.exists(test_key)
    h.client.start()
    assert h.client.connected
    h.client.exists(test_key)


def test_stop(Huskar, servers, cache_dir):
    h = Huskar(servers=servers, cluster='test', service='test',
               cache_dir=str(cache_dir))
    h.start()
    assert h.client.connected
    h.stop()
    assert not h.client.connected
