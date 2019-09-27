# -*- coding: utf-8 -*-

import pytest
from huskar_sdk_v2.http.service_registry import ServiceRegistry


class MockSession(object):
    class Resp(object):
        status_code = 200
        status = 'SUCCESS'
        message = ''

        def json(self):
            if self.status_code == 500:
                raise ValueError()
            return {'status': self.status, 'message': self.message,
                    'data': None}

        @property
        def ok(self):
            return self.status_code == 200

    def __init__(self):
        self.headers = {}

    def post(self, *args, **kwargs):
        return self.Resp()

    def delete(self, *args, **kwargs):
        return self.Resp()

    def close(self, *args, **kwargs):
        pass


@pytest.fixture
def registry():
    service_registry = ServiceRegistry('test', 'test', 'http://api', 'huskar')
    service_registry.session = MockSession()
    return service_registry


@pytest.fixture
def logger(mocker):
    return mocker.patch(
        'huskar_sdk_v2.http.service_registry.logger', autospec=True)


@pytest.fixture
def data():
    return {
        'ip': '8.8.8.8',
        'port': {'main': 8888},
        'meta': {'soaVersion': 'latest', 'protocol': 'thrift'},
        'state': 'up'
    }


def test_register_instance(registry, logger, data):
    assert registry.register_instance(**data) == '8.8.8.8_8888'
    assert not logger.error.called


def test_register_instance_denied(registry, logger, data):
    MockSession.Resp.status_code = 400
    MockSession.Resp.status = 'BadRequest'
    MockSession.Resp.message = 'Avada Kedavra'
    assert registry.register_instance(**data) is None
    logger.error.assert_called_once_with(
        'failed to register service, %d %s: %s', 400, 'BadRequest',
        'Avada Kedavra')


def test_register_instance_crashed(registry, logger, data):
    MockSession.Resp.status_code = 500
    assert registry.register_instance(**data) is None
    logger.error.assert_called_once_with(
        'unexpected error when register service:', exc_info=True)


@pytest.mark.parametrize('soa_mode', ['orig', 'prefix', 'route'])
def test_register_instance_with_soa_mode(soa_mode, logger, data):
    cluster = 'test'
    registry = ServiceRegistry(
        'test', cluster, 'http://api', 'huskar', soa_mode=soa_mode)
    assert registry.session.headers['X-SOA-Mode'] == soa_mode
    assert registry.session.headers['X-Cluster-Name'] == cluster


def test_register_instance_without_soa_mode(logger, data):
    registry = ServiceRegistry('test', 'test', 'http://api', 'huskar')
    assert 'X-SOA-Mode' not in registry.session.headers
    assert 'X-Cluster-Name' not in registry.session.headers
