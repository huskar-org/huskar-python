# -*- coding: utf-8 -*-

import gevent
import shutil
import requests
import pytest
import time
import mock

from huskar_sdk_v2.http.ioloops.http import HuskarApiIOLoop


def assert_config_value_correct(client):
    assert client.watched_configs.get(
        'arch.test', 'overall',
        'test_config', raises=True) == {'value': 'test_value'}


def assert_config_new_value_correct(client):
    assert client.watched_configs.get(
        'arch.test', 'overall',
        'test_config', raises=True) == {'value': 'new_value'}


def test_non_watched_app_should_raise_exception(requests_mock, empty_client):
    empty_client.run()
    assert empty_client.connected.wait(10)

    with pytest.raises(RuntimeError):
        assert_config_value_correct(empty_client)


def test_get(requests_mock, client):
    client.run()
    assert_config_value_correct(client)
    assert client.watched_services.get(
        'arch.test', 'alpha-stable',
        '192.168.1.1_17400') == {
            "value": (
                '{"ip": "192.168.1.1", "state": "up", "meta":'
                ' {"control_daemon_port": 5544, "soaVersion": "0.14.5.3", '
                '"protocol": "thrift", "weight": 1, "pushSequence":'
                ' 4974}, "name": "arch.test", "port": {"main": 17400}}'
            )}
    assert client.watched_switches.get(
        'arch.test', 'another-cluster',
        'switch-name') == {'value': "100"}
    assert client.watched_configs.fail_mode is False


def test_get_should_wait_for_server_return(requests_mock, client):
    requests_mock.wait_time = 1
    client.run()
    assert_config_value_correct(client)


def test_stop_signal_should_stop_event_loop(requests_mock, client):
    assert client.stopped.is_set() is True
    client.run()
    client.connected.wait(1)
    assert not client.stopped.is_set()
    assert client.stop(3)
    assert not client.connected.is_set()


def test_client_should_reconnect_when_timeout(requests_mock, client):
    client.max_alive_time = 1.0
    client.run()
    gevent.sleep(1.5)
    assert requests.Session.call_count == 3


def test_config_should_get_data_from_cache_in_time_during_startup(
        requests_mock, clear_ioloop_instance, cache_dir):
    old_client = HuskarApiIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    old_client.install()

    old_client.watched_configs.add_watch("arch.test", 'overall')
    old_client.run()
    assert old_client.connected.wait(1)
    assert_config_value_correct(old_client)
    requests_mock.stop_exception = requests.Timeout
    assert old_client.stop(3)

    client = HuskarApiIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    client.install()

    client.watched_configs.add_watch("arch.test", 'overall')
    client.run()
    begin = time.time()
    for i in range(100):
        assert_config_value_correct(client)
    assert time.time() - begin < 11
    client.stop()
    shutil.rmtree(client.cache_dir)


def test_config_should_fail_if_cant_get_from_any_source(
        requests_mock, no_cache_initial_client):
    requests_mock.stop_exception = requests.Timeout
    no_cache_initial_client.run()
    with pytest.raises(RuntimeError):
        assert_config_value_correct(no_cache_initial_client)


def test_exists_should_return_false_if_connection_is_broken(
        requests_mock, no_cache_client):
    requests_mock.stop_exception = requests.Timeout
    no_cache_client.run()
    assert not no_cache_client.watched_configs.exists(
        'arch.test', 'overall', 'test_config')


def test_delete_config_value_if_cache_is_not_consistent_with_server(
        requests_mock, client, clear_ioloop_instance, cache_dir):
    client.run()
    assert client.connected.wait(1)
    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"overall": {"new_config":'
        ' {"value": "new_value_2"}}}}}, "message": "update"}')
    assert requests_mock.wait_processed()
    assert client.watched_configs.get(
        'arch.test', 'overall', 'new_config') == {'value': 'new_value_2'}
    assert client.stop(3)

    client = HuskarApiIOLoop('test_url', 'test_token', cache_dir=cache_dir)
    client.install()
    client.watched_configs.add_watch("arch.test", 'overall')
    client.run()
    assert client.connected.wait(1)
    with pytest.raises(RuntimeError):
        assert client.watched_configs.get(
            'arch.test', 'overall',
            'new_config', raises=True) == {'value': 'new_value_2'}
    assert client.stop(3)


def test_config_should_get_newest_value_if_connection_has_recovered(
        requests_mock, client):
    client.run()
    assert_config_value_correct(client)

    requests_mock.set_result_file('test_data_changed.txt')
    gevent.sleep(1.0)
    assert_config_new_value_correct(client)


def test_config_should_success_if_connection_recovers(
        requests_mock, no_cache_initial_client):
    no_cache_initial_client.watched_configs.add_watch(
        "arch.test", 'overall')
    no_cache_initial_client.reconnect_gap = 1.0
    requests_mock.stop_exception = requests.Timeout
    no_cache_initial_client.run()
    with pytest.raises(RuntimeError):
        assert_config_value_correct(no_cache_initial_client)
    requests_mock.stop_exception = None
    assert no_cache_initial_client.connected.wait(10)
    assert_config_value_correct(no_cache_initial_client)


def test_config_should_get_newest_value_if_connection_recorvers(
        requests_mock, client):
    client.reconnect_gap = 1.0
    client.run()
    assert_config_value_correct(client)

    requests_mock.stop_exception = requests.Timeout

    for i in range(100):
        assert_config_value_correct(client)

    assert client.is_disconnected.wait(2)
    assert not client.connected.is_set()
    requests_mock.stop_exception = None
    assert client.connected.wait(10)
    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    assert not client.is_disconnected.is_set()
    assert_config_new_value_correct(client)


def test_config_should_be_deleted_if_deleted_event_received(
        requests_mock, client):
    client.run()
    assert_config_value_correct(client)
    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    client.watched_configs.get(
        'arch.test', 'overall', 'test_config') is None


def test_we_should_recover_from_illegal_reponse(requests_mock, client):
    client.run()
    assert_config_value_correct(client)
    requests_mock.add_response('asdf;kjas;dkfj;asldkjf;askdjf;lakdjsf')
    assert requests_mock.wait_processed()
    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    requests_mock.add_response('{"that": "this"}')
    assert requests_mock.wait_processed()
    assert_config_new_value_correct(client)


def test_shouldnt_start_if_first_response_is_illegal_and_no_cache_available(
        requests_mock, client):
    requests_mock.set_first_line('adfasdasdf')
    client.run()
    assert client.connected.wait(1)
    assert not client.watched_configs.values

    with pytest.raises(RuntimeError):
        client.watched_configs.get('arch.test', 'overall',
                                   'test_config', raises=True)

    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    assert_config_new_value_correct(client)


def test_should_work_with_illegal_cache_dir(requests_mock):
    client = HuskarApiIOLoop('test_url', 'test_token',
                             cache_dir='/that/ileegal')
    client.watched_configs.add_watch("arch.test", 'overall')
    client.run()
    assert_config_value_correct(client)
    requests_mock.set_result_file('test_data_changed.txt')
    assert requests_mock.wait_processed()
    assert_config_new_value_correct(client)
    client.stop()


def test_removed_service_should_not_be_triggered(requests_mock, client):
    client.run()
    assert client.connected.wait(1)

    client.watched_configs.get(
        'arch.test', 'overall', 'test_config', raises=True)
    client.watched_configs.remove_watch('arch.test', 'overall')

    assert client.next_watch_completed_event.wait(3)
    requests_mock.add_response(
        '{"body": {"config": {"arch.test": {"overall": {"new_config": '
        '{"value": "new_value_2"}}}}}, "message": "update"}')
    client.watched_configs.get('arch.test', 'overall', 'test_config',
                               raises=True)


def test_api_error(requests_mock, client, mocker):
    logger = mocker.patch(
        'huskar_sdk_v2.http.ioloops.http.logger', autospec=True)
    requests_mock.set_error_mode()
    client.run()
    assert client.is_disconnected.wait(1)
    assert not client.connected.is_set()
    assert not client.watched_configs.values

    logger.error.assert_called_once_with(
        'failed to watch: %d %r', 401, requests_mock.text)


def test_start_long_poll_exec_once():
    def poll(*args, **kwds):
        pass

    client = HuskarApiIOLoop('url', 'token')
    client.start_long_poll = mock.Mock(spec=poll)
    assert client.greenlet is None
    client.run()
    client.run()
    time.sleep(1)
    client.start_long_poll.assert_called_once()
