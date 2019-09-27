import glob
import logging
from os import environ as env
from threading import Thread
from multiprocessing import Pipe

import pytest
from kazoo.client import KazooClient
from kazoo.testing.harness import get_global_cluster


@pytest.fixture
def base_path(worker_id):
    return '/huskar_{}'.format(worker_id)


@pytest.fixture
def cache_dir(worker_id, tmpdir):
    return tmpdir.mkdir('huskar_{}'.format(worker_id))


@pytest.fixture
def servers(request, base_path):
    env.setdefault('ZOOKEEPER_VERSION', '3.4.9')
    if not (env.get("ZOOKEEPER_PATH") or env.get("ZOOKEEPER_CLASSPATH")):
        mac_default = glob.glob('/usr/local/Cellar/zookeeper/*/libexec')
        if mac_default:
            env['ZOOKEEPER_PATH'] = mac_default[0]
    try:
        cluster = get_global_cluster()
        cluster.start()
        servers = ','.join([s.address for s in cluster])
    except AssertionError:
        servers = "127.0.0.1:2181"
        logging.warn("Using ZooKeeper: {}".format(servers))

    def cleanup():
        kazoo = KazooClient(servers)
        try:
            kazoo.start()
            kazoo.delete(base_path, recursive=True)
        except Exception:
            logging.getLogger(__name__).error('Failed to clean up znodes.')
        finally:
            kazoo.stop()
            kazoo.close()
    request.addfinalizer(cleanup)
    return servers


@pytest.fixture
def Huskar(request, monkeypatch, base_path):
    import huskar_sdk_v2.bootstrap
    monkeypatch.setattr(huskar_sdk_v2.bootstrap, 'BASE_PATH', base_path)

    def huskar_maker(*args, **kwargs):
        h = huskar_sdk_v2.bootstrap.BootstrapHuskar(*args, **kwargs)
        request.addfinalizer(h.stop)
        return h
    return huskar_maker


@pytest.fixture
def huskar(Huskar, servers):
    return Huskar(service='test_service',
                  servers=servers,
                  cluster='test_cluster',
                  username='',
                  password='',
                  cache_dir=None,
                  record_version=False,
                  lazy=True)


@pytest.fixture
def offline_huskar(Huskar, request, cache_dir):
    return Huskar("test_service",
                  servers="host_that_not_exists",
                  cluster="test_cluster",
                  record_version=False,
                  cache_dir=str(cache_dir),
                  lazy=True)


@pytest.fixture
def cache_huskar(Huskar, request, servers, cache_dir):
    huskar = Huskar(service='test_service',
                    servers=servers, cluster='test_cluster',
                    lazy=True,
                    cache_dir=str(cache_dir),
                    record_version=False)

    request.addfinalizer(huskar.stop)
    return huskar


@pytest.fixture
def service_registry(request, huskar):
    registry = huskar.service_registry

    def clean_up():
        huskar.client.delete(registry.base_path, recursive=True)
    request.addfinalizer(clean_up)
    return registry


@pytest.fixture
def pool_map():
    def pool_map(C, func, iterable):
        ps = [(i, Pipe()) for i in iterable]

        def run(func, p):
            def _func(*args):
                p.send(func(*args))
                p.close()
            return _func

        pool = [C(target=run(func, l), args=(i,)) for i, (l, r) in ps]
        [c.start() for c in pool]
        [c.join() for c in pool]
        return [r.recv() for _, (l, r) in ps]
    return pool_map


@pytest.fixture
def go(pool_map):
    def go(_func, *args):
        return pool_map(Thread, lambda _: _func(*args), (1,))[0]
    return go
