# -*- coding: utf-8 -*-

from mock import Mock

import socket
import pytest
from huskar_sdk_v2.http.components.config import Config


@pytest.fixture
def config_component(requests_mock, no_cache_client):
    return Config('arch.test', 'overall')


def test_config_should_success_for_app_id_clueter(
        config_component, requests_mock):
    assert config_component.get('test_config') == 'test_value'


def test_config_watch_decorator(
        config_component, requests_mock, no_cache_client,
        wait_huskar_api_ioloop_connected):
    wait_huskar_api_ioloop_connected(3.0)
    handler = Mock()
    handler_2 = Mock()
    config_component.watch("test_config", handler)
    config_component.on_change("test_config")(handler_2)

    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    assert config_component.get('test_config') == 'new_value'
    handler.assert_called_once_with(u'new_value')
    handler_2.assert_called_once_with(u'new_value')
    assert handler.call_count == 1
    assert handler_2.call_count == 1


def test_config_exists_and_not_exists(
        config_component, requests_mock, no_cache_client,
        wait_huskar_api_ioloop_connected):
    wait_huskar_api_ioloop_connected(3.0)
    assert config_component.exists('test_config')
    assert not config_component.exists('something_that_shouldnt_exists')


def test_should_get_global_config_if_local_not_exists(
        requests_mock, no_cache_client, wait_huskar_api_ioloop_connected):
    config = Config('arch.test', 'some-cluster-not-exists')
    wait_huskar_api_ioloop_connected(3.0)
    assert config.get('test_config') == 'test_value'


def test_should_get_new_value_if_cluster_config_is_added(
        requests_mock, no_cache_client, wait_huskar_api_ioloop_connected):
    config = Config('arch.test', 'some-cluster-not-exists')
    wait_huskar_api_ioloop_connected(3.0)
    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"some-cluster-not-exists": '
        '{"test_config": {"value": "new_value"}}}}}, "message": "update"}'
    )
    assert requests_mock.wait_processed()
    assert config.get('test_config') == 'new_value'
    assert config.get('test_config', _force_overall=True) == 'test_value'


def test_should_get_overall_value_if_cluster_config_is_deleted(
        requests_mock, no_cache_client, wait_huskar_api_ioloop_connected):
    config = Config('arch.test', 'some-cluster-not-exists')
    wait_huskar_api_ioloop_connected(3.0)
    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"some-cluster-not-exists":'
        ' {"test_config": {"value": "new_value"}}}}}, "message": "update"}'
    )
    assert requests_mock.wait_processed()
    assert config.get('test_config') == 'new_value'

    mock_handler = Mock()
    config.on_change("test_config")(mock_handler)

    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"some-cluster-not-exists": '
        '{"test_config": {"value": null}}}}}, "message": "delete"}'
    )
    assert requests_mock.wait_processed()
    assert config.get('test_config') == 'test_value'
    mock_handler.assert_called_once_with(u'test_value')


def test_listener_called_with_config_in_overall_cluster_if_not_exists(
        requests_mock, no_cache_client, wait_huskar_api_ioloop_connected):
    config = Config('arch.test', 'some-cluster-not-exists')
    wait_huskar_api_ioloop_connected(3.0)
    handler = Mock()
    config.watch("test_config", handler)

    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    assert config.get('test_config') == 'new_value'
    assert config.exists('test_config')
    handler.assert_called_once_with(u'new_value')
    assert handler.call_count == 1

    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"some-cluster-not-exists": '
        '{"test_config": {"value": "new_value_2"}}}}}, "message": "update"}'
    )
    assert requests_mock.wait_processed()
    assert config.get('test_config') == 'new_value_2'
    assert config.exists('test_config')
    handler.assert_any_call(u'new_value_2')
    assert handler.call_count == 2

    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"overall": {"test_config": '
        '{"value": "new_value_2"}}}}}, "message": "update"}'
    )
    assert handler.call_count == 2


def test_should_get_dict_if_config_is_dict(
        requests_mock, client, config_component,
        wait_huskar_api_ioloop_connected):
    client.run()
    wait_huskar_api_ioloop_connected(10)
    requests_mock.add_response(
        r'{"body": {"config": {"arch.test": {"overall": {"test_config":'
        r' {"value": "{\"config\": 2}"}}}}}, "message": "update"}'
    )
    requests_mock.wait_processed()
    assert config_component.get('test_config')['config'] == 2


def test_should_get_correct_config_from_cache(requests_mock, started_client,
                                              config_component, cache_dir):
    import requests
    from huskar_sdk_v2.http.ioloops.http import HuskarApiIOLoop
    assert started_client.connected.wait(1)
    requests_mock.add_response(
        r'{"body": {"config": {"arch.test": {"overall": {"test_json":'
        r' {"value": "\"abcdefg\""}}}}}, "message": "update"}'
    )
    assert requests_mock.wait_processed()
    assert started_client.watched_configs.get(
        'arch.test', 'overall',
        'test_json', raises=True) == {'value': 'abcdefg'}

    assert started_client.stop(3)

    requests_mock.stop_exception = requests.Timeout
    client = HuskarApiIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    client.install()
    client.run()

    assert client.watched_configs.get(
        'arch.test', 'overall',
        'test_json', raises=True) == {'value': 'abcdefg'}


def test_iteritems_in_config(requests_mock, started_client):
    config = Config('arch.test', 'a_cluster')
    assert started_client.connected.wait(1)
    requests_mock.add_response(
        r'{"body": {"config": {"arch.test": {"a_cluster": {"test_json":'
        r' {"value": "\"abcdefg\""}}}}}, "message": "update"}'
    )
    requests_mock.add_response(
        r'{"body": {"config": {"arch.test": {"a_cluster": {"test_config":'
        r' {"value": "\"12345\""}}}}}, "message": "update"}'
    )
    assert requests_mock.wait_processed()

    items = list(config.iteritems())
    assert sorted(items) == sorted(list({
        'test_json': "abcdefg",
        'test_config': "12345",
        }.items()))


def test_critical_component_should_raise_if_client_not_connected(requests_mock,
                                                                 client):
    config = Config('arch.test', 'a_cluster')
    config.set_critical()
    requests_mock.wait_time = 11
    client.run()

    with pytest.raises(RuntimeError):
        config.get("test")


def test_non_critical_component_should_ignore_if_client_not_connected(
        requests_mock, client):
    config = Config('arch.test', 'a_cluster')
    requests_mock.wait_time = 10
    client.run()

    assert config.get("test") is None


def test_critical_component_should_still_work_if_cache_is_found(requests_mock,
                                                                client,
                                                                sleep_ops):
    config = Config('arch.test', 'a_cluster')
    config_component = config.client
    config.set_critical()
    client.run()
    config.get("test")
    assert not config_component.fail_mode

    sleep_ops.set_constant_sleep_time(0.1)
    requests_mock.stop_exception = socket.timeout
    assert client.is_disconnected.wait(3)
    assert not client.is_connected()

    config.get("test")
    assert not config_component.fail_mode


def test_critical_component_should_recover_if_connection_reestablished(
        requests_mock, client, sleep_ops):
    config = Config('arch.test', 'a_cluster')
    config.set_critical()
    client.run()
    config_component = config.client
    config.get("test")
    assert not config_component.fail_mode

    sleep_ops.set_constant_sleep_time(0.1)
    requests_mock.stop_exception = socket.timeout
    assert client.is_disconnected.wait(3)
    assert not client.is_connected()

    config.get("test")
    assert not config_component.fail_mode

    requests_mock.stop_exception = None
    assert client.connected.wait(3)

    config.get("test")
    assert not config_component.fail_mode
