import json

from huskar_sdk_v2.common import ServiceInstance
from huskar_sdk_v2.consts import ENV_DOCKER_CONTAINER_ID


def test_instance_fingerprint(monkeypatch):
    instance = ServiceInstance('whatever', '0.0.0.0', {'main': 55555})
    assert instance.fingerprint == '0.0.0.0_55555'
    monkeypatch.setenv(ENV_DOCKER_CONTAINER_ID, '455cd1345')
    assert instance.fingerprint == '455cd1345'


def test_build_instance(service_registry):
    instance = service_registry.build_instance('8.8.8.8', {'main': 88},
                                               meta={'test': 'test'})
    assert instance.ip == '8.8.8.8'
    assert instance.port == {'main': 88}
    assert instance.meta == {'test': 'test'}
    assert instance.state == 'up'
    instance.mark_down()
    assert instance.state == 'down'


def test_register(service_registry):
    instance = service_registry.build_instance('8.8.8.8', {'main': 88})
    service_registry.register(instance)
    assert service_registry.exists(instance)


def test_unregister(service_registry):
    instance = service_registry.build_instance('8.8.8.8', {'main': 88})
    instance_id = service_registry.register(instance)
    assert service_registry.exists(instance)
    service_registry.unregister(instance_id)
    assert not service_registry.exists(instance)


def test_update_instance(service_registry, huskar):
    instance = service_registry.build_instance('8.8.8.8', {'main': 88},
                                               meta={})
    instance_id = service_registry.register(instance)
    new_instance = service_registry.build_instance('8.8.8.8', {'main': 88},
                                                   meta={'test': 'test'})
    new_instance_id = service_registry.update_instance(instance_id,
                                                       new_instance)
    assert service_registry.exists(new_instance)

    value_json, _ = huskar.client.get(
        service_registry._get_instance_path(new_instance_id))
    value = json.loads(value_json)
    assert value.get('meta', None) == {'test': 'test'}

    new_instance1 = service_registry.build_instance('8.8.8.9', {'main': 88},
                                                    meta={'test': 'test'})
    service_registry.update_instance(new_instance_id, new_instance1)

    assert service_registry.exists(new_instance1)
    assert not service_registry.exists(new_instance)
