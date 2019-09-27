# -*- coding: utf-8 -*-

import os

import pytest
from mock import Mock

from huskar_sdk_v2.http.components import BaseComponent
from huskar_sdk_v2.http import HttpHuskar
from huskar_sdk_v2.http.ioloops import IOLoop
from huskar_sdk_v2.consts import (
    OVERALL,
    ENV_CACHE_DIR_NAMESPACE,
    ENV_SUPERVISOR_GROUP_NAME,
    ENV_DOCKER_CONTAINER_ID,
)


def test_should_write_virtual_methods_for_base_component(client):
    with pytest.raises(NotImplementedError):
        component = BaseComponent('arch.test', 'overall')
        component.add_current_app_to_watchlist()

    class FakeComponent(BaseComponent):
        @property
        def client(self):
            return client.watched_configs
    component = FakeComponent('arch.test', 'overall')

    with pytest.raises(NotImplementedError):
        component.get('test')

    with pytest.raises(NotImplementedError):
        component.exists('test')

    with pytest.raises(NotImplementedError):
        component.handle_changes('test')


def test_http_huskar_init_with_no_cluster(
        requests_mock, file_cache_client, fake_config_with_file_cache_client,
        wait_huskar_api_ioloop_connected, cache_dir
        ):

    huskar = HttpHuskar('arch.test', url='test_url', token='test_token',
                        cache_dir=cache_dir)
    assert huskar.cluster == OVERALL
    huskar.stop()

    huskar = HttpHuskar('arch.test', cluster=None, url='test_url',
                        token='test_token', cache_dir=cache_dir)
    assert huskar.cluster == OVERALL
    huskar.stop()


def test_init_http_huskar(requests_mock,
                          file_cache_client,
                          fake_config_with_file_cache_client,
                          wait_huskar_api_ioloop_connected, cache_dir):
    huskar = HttpHuskar('arch.test', 'alpha-stable',
                        url='test_url', token='test_token',
                        cache_dir=cache_dir)
    assert huskar.config
    assert huskar.switch
    assert huskar.service_consumer

    huskar.start()
    wait_huskar_api_ioloop_connected(1)

    cache_config = fake_config_with_file_cache_client(
        'arch.test', 'alpha-stable')
    file_cache_client.cache_dir = IOLoop.current().cache_dir
    file_cache_client.components_paths = {
        name: os.path.join(file_cache_client.cache_dir, name + '_cache.json')
        for name in file_cache_client.components.keys()
    }

    file_cache_client.run()

    assert huskar.config.get('test_config') == 'test_value'

    assert cache_config.get('test_config') == 'test_value'
    huskar.stop()


def test_user_agent(client):
    user_agent = client.session.headers.get('User-Agent', '')
    assert user_agent.startswith('huskar-sdk')
    assert user_agent.endswith('mocked-requests/0.0.0')


def test_soa_mode_default(empty_client):
    empty_client.init_session()
    assert 'X-SOA-Mode' not in empty_client.session.headers


@pytest.mark.parametrize('soa_mode', ['orig', 'prefix', 'route'])
def test_soa_mode_cluster(empty_client, soa_mode):
    cluster = 'mock-1'
    empty_client.set_soa_mode_cluster(soa_mode, cluster)
    empty_client.init_session()

    assert empty_client.session.headers['X-SOA-Mode'] == soa_mode
    assert empty_client.session.headers['X-Cluster-Name'] == cluster


def test_namespace_and_mode(monkeypatch):
    def _assert_namespace_mode(namepsace, mode):
        namepsace, mode = HttpHuskar._get_namespace_and_mode()
        assert namepsace == namepsace
        assert mode == mode

    monkeypatch.setenv(ENV_DOCKER_CONTAINER_ID, '3e1f23a6')
    monkeypatch.setenv(ENV_SUPERVISOR_GROUP_NAME, 'huskar.test')

    # container_id shadow supervisor_group_name
    _assert_namespace_mode('3e1f23a6', HttpHuskar.MODE_MULTIPROCESS)
    # huskar_namepsace shadow container_id
    monkeypatch.setenv(ENV_CACHE_DIR_NAMESPACE, 'huskar_namepsace')
    _assert_namespace_mode('huskar_namepsace', HttpHuskar.MODE_MULTIPROCESS)

    # supervisor_group_name shadow default
    monkeypatch.delenv(ENV_CACHE_DIR_NAMESPACE)
    monkeypatch.delenv(ENV_DOCKER_CONTAINER_ID)
    _assert_namespace_mode('huskar.test', HttpHuskar.MODE_MULTIPROCESS)

    # supervisor_group_name as app_id
    monkeypatch.setenv(ENV_SUPERVISOR_GROUP_NAME, 'huskar.test.daemons.abc')
    _assert_namespace_mode('huskar.test.daemons.abc',
                           HttpHuskar.MODE_MULTIPROCESS)

    # no surprise when convert supervisor_group_name into app_id
    monkeypatch.setenv(ENV_SUPERVISOR_GROUP_NAME, 'huskar')
    _assert_namespace_mode('huskar', HttpHuskar.MODE_MULTIPROCESS)

    # huskar_namepsace shadow supervisor_group_name
    monkeypatch.setenv(ENV_CACHE_DIR_NAMESPACE, 'huskar_namepsace')
    _assert_namespace_mode('huskar_namepsace', HttpHuskar.MODE_MULTIPROCESS)

    # default namespace: "default"
    monkeypatch.delenv(ENV_CACHE_DIR_NAMESPACE)
    monkeypatch.delenv(ENV_SUPERVISOR_GROUP_NAME)
    _assert_namespace_mode('default', HttpHuskar.MODE_SINGLEPROCESS)

    # huskar_namepsace shadow default
    monkeypatch.setenv(ENV_CACHE_DIR_NAMESPACE, 'huskar_namepsace')
    _assert_namespace_mode('huskar_namepsace', HttpHuskar.MODE_SINGLEPROCESS)


def test_register_huskar_event_handler(
        requests_mock, wait_huskar_api_ioloop_connected, cache_dir):
    huskar = HttpHuskar('arch.test', 'alpha-stable',
                        url='test_url', token='test_token',
                        cache_dir=cache_dir)

    handler = Mock()
    huskar.register_ioloop_hook('polling_error', handler)

    assert handler in IOLoop.current().event_listeners['polling_error']
