# -*- coding: utf-8 -*-

from mock import Mock

import pytest
import gevent
from huskar_sdk_v2.http.components.service import Service

initial_service_data = {u'192.168.1.1_17400': {
    u'ip': u'192.168.1.1',
    u'meta': {
        u'control_daemon_port': 5544,
        u'protocol': u'thrift',
        u'pushSequence': 4974,
        u'soaVersion': u'0.14.5.3',
        u'weight': 1},
    u'name': u'arch.test',
    u'port': {u'main': 17400},
    u'state': u'up'},
}

added_service_data = {"192.168.1.1_23471": {
    "ip": "192.168.1.1",
    "state": "up",
    "meta": {
        "control_daemon_port": 5544,
        "soaVersion": "0.14.5.3",
        "protocol": "thrift", "weight": 1,
        "pushSequence": 4975},
    "name": "arch.test",
    "port": {"main": 23471}}
}


@pytest.fixture
def service_component(request, requests_mock, started_client):
    assert started_client.connected.wait(1)
    return Service('arch.test', 'alpha-stable')


@pytest.fixture
def fake_service_component(started_file_cache_client,
                           fake_service_with_file_cache_client):
    started_file_cache_client.watched_configs.add_watch(
        "arch.test", 'overall')
    started_file_cache_client.watched_switches.add_watch(
        "arch.test", 'another-cluster')
    started_file_cache_client.watched_services.add_watch(
        "arch.test", 'alpha-stable')
    return fake_service_with_file_cache_client('arch.test', 'alpha-stable')


def test_service_should_yield_the_same_format_as_old_huskar(
        service_component, started_client,
        fake_service_component):

    assert started_client.connected.wait(1)
    assert service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data

    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data


def test_service_changed_should_change_service_nodes(
        requests_mock, service_component, started_client,
        fake_service_component):
    assert started_client.connected.wait(1)
    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    new_service_data = dict(initial_service_data)
    new_service_data.update(added_service_data)
    assert service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == new_service_data

    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == new_service_data


def test_service_deleted_should_change_service_nodes(
        requests_mock, service_component, started_client,
        fake_service_component):
    listener = Mock()
    assert started_client.connected.wait(1)
    service_component.register_hook_function(
        'arch.test', 'alpha-stable', listener)
    requests_mock.set_result_file('test_data_deleted.txt')
    assert requests_mock.wait_processed()
    assert listener.call_count == 2
    listener.assert_any_call({})
    assert service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == {}

    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == {}


def test_service_node_changed_should_notify_listeners(
        requests_mock, service_component, started_client,
        fake_service_component):
    assert started_client.connected.wait(1)
    listener = Mock()
    fake_listener = Mock()
    service_component.register_hook_function(
        'arch.test', 'alpha-stable', listener)
    fake_service_component.register_hook_function(
        'arch.test', 'alpha-stable', fake_listener)

    listener.assert_called_once_with(initial_service_data)
    gevent.sleep(0.5)
    fake_listener.assert_called_with(initial_service_data)

    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()

    new_service_data = dict(initial_service_data)
    new_service_data.update(added_service_data)

    listener.assert_any_call(new_service_data)
    gevent.sleep(0.5)
    fake_listener.assert_any_call(new_service_data)


def test_file_client_add_watch_after_data_already_processed(
        requests_mock, service_component, started_client,
        fake_service_component):
    fake_service_component.client.app_id_cluster_map.pop('arch.test', None)
    assert started_client.connected.wait(1)
    listener = Mock()
    fake_listener = Mock()
    service_component.register_hook_function(
        'arch.test', 'alpha-stable', listener)
    listener.assert_called_once_with(initial_service_data)

    gevent.sleep(0.5)
    assert ('alpha-stable' not in
            fake_service_component.client.app_id_cluster_map['arch.test'])
    fake_service_component.register_hook_function(
        'arch.test', 'alpha-stable', fake_listener)
    fake_listener.assert_called_with(initial_service_data)
    assert ('alpha-stable' in
            fake_service_component.client.app_id_cluster_map['arch.test'])


def test_service_batch_add_watch(requests_mock, service_component,
                                 started_client, started_file_cache_client,
                                 fake_service_component):
    service_component.preprocess_service_mappings({})
    fake_service_component.preprocess_service_mappings({})
    assert service_component.preprocess_service_mappings({
        'arch.test1': {'that-cluster'},
        'arch.test2': {'this-cluster'},
        }) is True

    assert fake_service_component.preprocess_service_mappings({
        'arch.test1': {'that-cluster'},
        'arch.test2': {'this-cluster'},
        }) is True

    assert dict(started_client.watched_services.app_id_cluster_map) == {
        'arch.test': {'alpha-stable'},
        'arch.test1': {'that-cluster'},
        'arch.test2': {'this-cluster'},
        }
    fake_services = started_file_cache_client.watched_services
    assert dict(fake_services.app_id_cluster_map) == {
        'arch.test': {'alpha-stable'},
        'arch.test1': {'that-cluster'},
        'arch.test2': {'this-cluster'},
    }


def test_legacy_interface(requests_mock, service_component):
    service_component.set_min_server_num(1)


def test_add_service_in_the_middle_of_runtime(
        requests_mock, service_component,
        started_client, fake_service_component):
    assert started_client.connected.wait(1)
    assert service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data
    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data

    requests_mock.add_response(
        r'{"body": {"service": {"arch.test": {"beta-stable": '
        r'{"192.168.1.1_9999": {"value": "{\"ip\": \"192.168.1.1\"'
        r', \"state\": \"up\", \"meta\": {\"control_daemon_port\": 5544,'
        r' \"soaVersion\": \"0.14.5.3\", \"protocol\": \"thrift\",'
        r' \"weight\": 1, \"pushSequence\": 4975}, \"name\":'
        r' \"arch.test\", \"port\": {\"main\": 9999}}"}}}}},'
        r' "message": "update"}')
    assert requests_mock.wait_processed()
    assert service_component.get_service_node_list(
        'arch.test', 'beta-stable') == {}

    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'beta-stable') == {}

    assert service_component.add_service('arch.test', 'beta-stable',
                                         timeout=10)
    assert fake_service_component.add_service('arch.test', 'beta-stable',
                                              timeout=10)

    requests_mock.add_response(
        r'{"body": {"service": {"arch.test": {"beta-stable":'
        r' {"192.168.1.1_9999": {"value": "{\"ip\":'
        r' \"192.168.1.1\", \"state\": \"up\", \"meta\":'
        r' {\"control_daemon_port\": 5544, \"soaVersion\": \"0.14.5.3\",'
        r' \"protocol\": \"thrift\", \"weight\": 1, \"pushSequence\":'
        r' 4975}, \"name\": \"arch.test\", \"port\": {\"main\": 9999'
        r'}}"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    assert service_component.get_service_node_list(
        'arch.test', 'beta-stable')

    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'beta-stable')


def test_service_should_not_update_if_watch_is_removed(
        requests_mock, service_component,
        started_client, fake_service_component):
    assert started_client.connected.wait(1)
    assert service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data

    gevent.sleep(0.5)
    assert fake_service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data

    assert service_component.unwatch_service(
        'arch.test', 'alpha-stable', timeout=2.0)
    assert fake_service_component.unwatch_service(
        'arch.test', 'alpha-stable', timeout=2.0)

    requests_mock.add_response(
        r'{"body": {"service": {"arch.test": {"alpha-stable": '
        r'{"192.168.1.1_9999": {"value": "{\"ip\": \"192.168.1.1\",'
        r' \"state\": \"up\", \"meta\": {\"control_daemon_port\": 5544,'
        r' \"soaVersion\": \"0.14.5.3\", \"protocol\": \"thrift\", \"weight\":'
        r' 1, \"pushSequence\": 4975}, \"name\": \"arch.test\", \"port\": '
        r'{\"main\": 9999}}"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    assert service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data
    assert fake_service_component.get_service_node_list(
        'arch.test', 'alpha-stable') == initial_service_data
