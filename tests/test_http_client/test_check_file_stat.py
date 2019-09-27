# -*- coding: utf-8 -*-

import os
import json

import pytest
import gevent


def write_content(fpath, content):
    with open(fpath, 'w') as f:
        json.dump(content, f)


def read_content(fpath):
    with open(fpath, 'r') as f:
        return json.load(f)


@pytest.fixture
def config_path(cache_dir):
    return os.path.join(cache_dir, 'configs_cache.json')


@pytest.fixture
def service_path(cache_dir):
    return os.path.join(cache_dir, 'services_cache.json')


@pytest.fixture
def switch_path(cache_dir):
    return os.path.join(cache_dir, 'switches_cache.json')


@pytest.fixture
def fake_file(request, config_path, service_path, switch_path, test_data):
    write_content(config_path, test_data.config_content)
    write_content(service_path, test_data.service_content)
    write_content(switch_path, test_data.switch_content)


def test_check_file_stat(
        fake_file, started_file_cache_client,
        config_path, service_path, switch_path):
    assert started_file_cache_client.wait(1)

    assert started_file_cache_client.started
    for name, stat in started_file_cache_client.files_stat.items():
        assert stat
    old_files_stat = started_file_cache_client.files_stat.copy()

    started_file_cache_client.started.clear()
    os.chmod(config_path, 0o777)
    read_content(switch_path)
    read_content(service_path)
    assert not started_file_cache_client.started.is_set()

    gevent.sleep(0.5)
    for name, stat in started_file_cache_client.files_stat.items():
        assert old_files_stat[name] == stat

    write_content(config_path, {})
    write_content(switch_path, {})
    write_content(service_path, {})
    assert started_file_cache_client.started.is_set()

    gevent.sleep(0.5)
    for name, stat in started_file_cache_client.files_stat.items():
        assert old_files_stat[name] != stat

    assert started_file_cache_client.started.is_set()


def test_kill(fake_file, started_file_cache_client):
    started_file_cache_client.retry_acquire_gap = 0.5
    started_file_cache_client.check_file_stat_gap = 0.5
    assert started_file_cache_client.is_running()
    assert started_file_cache_client.stop(timeout=2)
    assert not started_file_cache_client.is_running()
