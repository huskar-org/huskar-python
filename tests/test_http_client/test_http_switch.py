# -*- coding: utf-8 -*-

import pytest
from mock import Mock
import gevent
from huskar_sdk_v2.http.components.switch import Switch


@pytest.fixture
def switch_component(client, wait_huskar_api_ioloop_connected):
    switch = Switch('arch.test', 'overall')
    client.run()
    wait_huskar_api_ioloop_connected(1)
    return switch


@pytest.fixture
def fake_switch_component(file_cache_client,
                          fake_switch_with_file_cache_client):
    switch = fake_switch_with_file_cache_client(
        'arch.test', 'overall')
    file_cache_client.run()
    return switch


def test_deleted_switch_should_give_default(
        switch_component,
        fake_switch_component,
        requests_mock
        ):
    requests_mock.add_response(
        '{"body": {"switch": {"arch.test": {"overall": '
        '{"test-deleted-switch": {"value": "0"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    assert not switch_component.is_switched_on('test-deleted-switch')
    requests_mock.add_response(
        '{"body": {"switch": {"arch.test": {"overall": '
        '{"test-deleted-switch": {"value": null}}}}}, "message": "delete"}'
    )
    assert requests_mock.wait_processed()
    assert switch_component.is_switched_on('test-deleted-switch')


def test_switches(switch_component, fake_switch_component):
    def _test(switch_component):
        assert switch_component.is_switched_on('switch-name')
        assert not switch_component.is_switched_on('switch-name-another',
                                                   default=False)
        assert switch_component.is_switched_on('switch-name-another')
        switch_component.set_default_state(False)
        assert not switch_component.is_switched_on('switch-name-another')
        assert not switch_component.is_switched_on('illegal-switch')

    _test(switch_component)
    gevent.sleep(0.3)
    _test(fake_switch_component)


def test_switch_on_bind_decorator_with_default_being_a_factory(
        switch_component, requests_mock, fake_switch_component):
    alternate_func = Mock()
    fake_alternate_func = Mock()
    binder = switch_component.bind('test-switch', default=alternate_func)
    fake_binder = fake_switch_component.bind('test-switch',
                                             default=fake_alternate_func)

    origin_fn = Mock()
    origin_fn.__name__ = 'origin_fn'
    decorated = binder(origin_fn)
    fake_decorated = fake_binder(origin_fn)

    decorated(1, 2, 3)
    origin_fn.assert_called_with(1, 2, 3)
    assert origin_fn.call_count == 1
    assert not alternate_func.called

    fake_decorated(4, 5, 6)
    origin_fn.assert_called_with(4, 5, 6)
    assert origin_fn.call_count == 2
    assert not fake_alternate_func.called

    requests_mock.add_response(
        '{"body": {"switch": {"arch.test": {"overall": '
        '{"test-switch": {"value": "0"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    decorated(1, 2, 3)
    assert origin_fn.call_count == 2
    assert alternate_func.called

    gevent.sleep(0.3)
    fake_decorated(4, 5, 6)
    assert origin_fn.call_count == 2
    assert fake_alternate_func.called


def test_switch_on_bind_decorator_with_default_being_an_object(
        switch_component, requests_mock, fake_switch_component):
    alternate_return = object()
    binder = switch_component.bind('test-switch', default=alternate_return)
    fake_binder = fake_switch_component.bind('test-switch',
                                             default=alternate_return)

    origin_fn = Mock()
    origin_fn.__name__ = 'origin_fn'
    decorated = binder(origin_fn)
    fake_decorated = fake_binder(origin_fn)

    assert decorated(1, 2, 3) != alternate_return
    origin_fn.assert_called_with(1, 2, 3)
    assert origin_fn.call_count == 1

    assert fake_decorated(4, 5, 6) != alternate_return
    origin_fn.assert_called_with(4, 5, 6)
    assert origin_fn.call_count == 2

    requests_mock.add_response(
        '{"body": {"switch": {"arch.test": {"overall": '
        '{"test-switch": {"value": "0"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    assert decorated(1, 2, 3) == alternate_return
    assert origin_fn.call_count == 2

    gevent.sleep(0.3)
    assert fake_decorated(4, 5, 6) == alternate_return
    assert origin_fn.call_count == 2


def test_iteritems_in_config(
        requests_mock, client, wait_huskar_api_ioloop_connected):
    switch = Switch('arch.test', 'a_cluster')
    client.run()
    wait_huskar_api_ioloop_connected(1)

    requests_mock.add_response(
        r'{"body": {"switch": {"arch.test": {"a_cluster": {"test_switch":'
        r' {"value": "100"}}}}}, "message": "update"}'
    )
    requests_mock.add_response(
        r'{"body": {"switch": {"arch.test": {"overall": {"test_switch":'
        r' {"value": "0"}}}}}, "message": "update"}'
    )
    requests_mock.add_response(
        r'{"body": {"switch": {"arch.test": {"overall": {"test_switch_2":'
        r' {"value": "0"}}}}}, "message": "update"}'
    )
    assert requests_mock.wait_processed()

    items = list(switch.iteritems())

    assert sorted(items) == sorted(list({
        'test_switch': 100.0,
        'test_switch_2': 0.0,
        'switch-name': 100.0,
        'illegal-switch': u'aaaaa',
        }.items()))


def test_switch_value_zero_should_fail_test(requests_mock, monkeypatch,
                                            wait_huskar_api_ioloop_connected,
                                            switch_component):
    requests_mock.add_response(
        '{"body": {"switch": {"arch.test": {"overall": '
        '{"test-deleted-switch": {"value": "0"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    monkeypatch.setattr(switch_component.rand, 'randint', lambda l, h: 1)
    assert not switch_component.is_switched_on('test-deleted-switch')

    monkeypatch.setattr(switch_component.rand, 'randint', lambda l, h: 10000)
    assert not switch_component.is_switched_on('test-deleted-switch')


def test_switch_value_100_should_pass(requests_mock, monkeypatch,
                                      wait_huskar_api_ioloop_connected,
                                      switch_component):
    requests_mock.add_response(
        '{"body": {"switch": {"arch.test": {"overall": '
        '{"test-deleted-switch": {"value": "100"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    monkeypatch.setattr(switch_component.rand, 'randint', lambda l, h: 10000)
    assert switch_component.is_switched_on('test-deleted-switch')

    monkeypatch.setattr(switch_component.rand, 'randint', lambda l, h: 1)
    assert switch_component.is_switched_on('test-deleted-switch')
