from pytest import fixture
import gevent


@fixture
def service_consumer(request, huskar):
    return huskar.service_consumer


def test_get_service_instance(service_registry, service_consumer):
    for ip in ['1.1.1.1', '2.2.2.2', '3.3.3.3', '4.4.4.4']:
        instance = service_registry.build_instance(ip, {'main': 88})
        service_registry.register(instance)

    instances = service_consumer.get_service_instance('test_service',
                                                      'test_cluster')
    assert len(instances) == 4
    assert set(instances) == {
        '1.1.1.1_88', '2.2.2.2_88', '3.3.3.3_88', '4.4.4.4_88'}


def test_get_service_instance_with_dirty_data(
        huskar, service_registry, service_consumer):
    for ip in ['1.1.1.1', '2.2.2.2', '2.3.3.3', '3.3.3.3', '4.4.4.4']:
        instance = service_registry.build_instance(ip, {'main': 88})
        if ip == '2.3.3.3':
            instance.to_string = lambda: 'dirty-data'
        service_registry.register(instance)

    instances = service_consumer.get_service_instance('test_service',
                                                      'test_cluster')
    assert '2.3.3.3_88' not in set(instances)
    assert len(instances) == 4


def test_register_hook(service_registry, service_consumer):
    for ip in ['1.1.1.1', '2.2.2.2', '3.3.3.3', '4.4.4.4']:
        instance = service_registry.build_instance(ip, {'main': 88})
        service_registry.register(instance)

    service_consumer.get_service_instance('test_service',
                                          'test_cluster')

    def hook_fun(server_list):
        hook_fun.hook_fun_called = True

    hook_fun.hook_fun_called = False
    service_consumer.register_hook_function(service='test_service',
                                            cluster='test_cluster',
                                            hook_function=hook_fun,
                                            trigger=False,)

    assert not hook_fun.hook_fun_called

    instance = service_registry.build_instance('8.8.8.8', {'main': 88})
    service_registry.register(instance)

    gevent.sleep(1)

    assert hook_fun.hook_fun_called
