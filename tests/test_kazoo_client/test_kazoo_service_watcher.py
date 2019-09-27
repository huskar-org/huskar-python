import os
import mock
import pytest
from huskar_sdk_v2.service_watcher import (
    ServiceWatcher, launch_master_controller)


@pytest.fixture
def huskar_options(huskar):
    return {'service': huskar.service,
            'servers': huskar.servers,
            'username': huskar.username,
            'password': huskar.password,
            'cluster': huskar.cluster}


@pytest.fixture
def huskar_http_options(huskar):
    return {
        "service": huskar.service,
        "cluster": huskar.cluster,
        "url": 'http://huskar.api',
        "token": 'youguess',
        "soa_mode": 'route',
    }


@mock.patch('socket.gethostbyname', return_value='')
@mock.patch('huskar_sdk_v2.service_watcher.launch_master_controller')
@mock.patch('os.fork', return_value=0)
@mock.patch('os.kill', return_value=0)
def test_master_controller_called_with(
        kill, fork, mock_process, _, huskar,
        huskar_options, huskar_http_options):

    configs = {
        "hosts": huskar.servers,
        "service_name": huskar.service,
    }
    configs.update(huskar_options)
    configs.update(huskar_http_options)

    watcher = ServiceWatcher(configs)

    watcher.register_instances()
    mock_process.assert_called_with(
        huskar_options,
        instances=None,
        master_pid=os.getpid(),
        ip='',
        unregister_on_exit=False,
        service_checker=watcher.check_service,
        boot_wait_time=5,
        use_http=False
    )

    configs.update({'use_http': True})
    watcher = ServiceWatcher(configs)
    watcher.register_instances()
    mock_process.assert_called_with(
        huskar_http_options,
        instances=None,
        master_pid=os.getpid(),
        ip="",
        unregister_on_exit=False,
        use_http=True,
        service_checker=watcher.check_service,
        boot_wait_time=5,
    )


@mock.patch(
    'huskar_sdk_v2.bootstrap.components.service_registry.ServiceRegistry.'
    'register_instance')
def test_controller_calls_registry(mock_register, huskar, huskar_options):
    port = {'main': 1200}
    ip = '123'

    instances = [{'port': port, 'state': 'up', 'meta': {}}]
    with mock.patch('os.kill', side_effect=OSError):
        with mock.patch('sys.exit'):
            with mock.patch('os.getppid'):
                launch_master_controller(huskar_options,
                                         ip=ip, master_pid=os.getpid(),
                                         instances=instances)
    mock_register.assert_called_with(ip, port, state='up', meta={})


@mock.patch('huskar_sdk_v2.service_watcher.ServiceWatcher.check_service',
            side_effect=[True, TypeError(), True, False])
@mock.patch(
    'huskar_sdk_v2.bootstrap.components.service_registry.ServiceRegistry.'
    'register_instance')
@mock.patch('socket.gethostbyname', return_value='169.254.0.1')
@mock.patch('os.fork', return_value=0)
@mock.patch('os.kill', return_value=0)
def test_service_checker(
        kill, fork, gethostbyname, register_instance, check_service,
        huskar, huskar_options):

    class BreakError(Exception):
        pass

    watcher = ServiceWatcher({'service_name': huskar.service,
                              'hosts': huskar.servers,
                              'username': huskar.username,
                              'password': huskar.password,
                              'cluster': huskar.cluster}, boot_wait_time=0)

    with pytest.raises(BreakError), \
            mock.patch('signal.signal', side_effect=BreakError):
        watcher.register_instances([
            {'port': 5000, 'meta': {'version': '1.0'}, 'state': 'up'},
            {'port': 5001, 'meta': {'version': '1.0'}, 'state': 'up'},
            {'port': 5002, 'meta': {'version': '1.0'}, 'state': 'down'},
            {'port': 5003, 'meta': {'version': '1.0'}, 'state': 'up'},
        ])

    check_service.assert_has_calls([
        mock.call('169.254.0.1',
                  {'port': 5000, 'meta': {'version': '1.0'}, 'state': 'up'}),
        mock.call('169.254.0.1',
                  {'port': 5001, 'meta': {'version': '1.0'}, 'state': 'up'}),
        mock.call('169.254.0.1',
                  {'port': 5002, 'meta': {'version': '1.0'}, 'state': 'down'}),
        mock.call('169.254.0.1',
                  {'port': 5003, 'meta': {'version': '1.0'}, 'state': 'up'}),
    ])

    register_instance.assert_has_calls([
        mock.call('169.254.0.1', 5000, meta={'version': '1.0'}, state='up'),
        mock.call('169.254.0.1', 5002, meta={'version': '1.0'}, state='down'),
    ])
