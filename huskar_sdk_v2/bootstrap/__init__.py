from __future__ import absolute_import

import os
import logging


from huskar_sdk_v2.utils import to_legal_filename, lazy_property
from huskar_sdk_v2.utils.cached_dict import CachedDict
from huskar_sdk_v2.consts import OVERALL, BASE_PATH, SERVICE_CACHE_FILENAME
from .client import BaseClient
from .components.switch import Switch
from .components.config import Config
from .components.service_registry import ServiceRegistry
from .components.service_consumer import ServiceConsumer


__all__ = ['BootstrapHuskar']


class BootstrapHuskar(object):
    """A client for service registration and configuration management.
    **The order of arguments may change, initialize Huskar only with keyword
    arguments.**

    :arg str service: service name of **your** service(e.g. `arch.test`)
    :arg str servers: Comma-separated list of Huskar hosts to connect to.
                      (e.g. `127.0.0.1:2181,127.0.0.1:2182`)
    :arg str username: username to connect to Huskar.
    :arg str password: password to connect to Huskar.
    :arg str cluster: the cluster name of **your** server(e.g. `alpha_stable`),
                      if you don't have the need of multiple cluster(or you
                      just don't understand), leave this default(do not pass).
                      You can named your cluster whatever you like.
                      **BUT DO NOT** use ``overall``, this is a reserved
                      cluster whose configuration will become the default for
                      other clusters, you can override them in the specific
                      cluster. This works just like a base class for other
                      clusters.
    :arg str cache_dir: a path to store cache files, will be used when
                        connection issue occurs to huskar server.
    :arg bool lazy: indicates if :meth:`.start` should be invoked automatically
                    when some methods are called(e.g. `config.get()`), usually
                    you just leave this alone.
    :arg bool local_mode: only used in testing mode, in this mode no connection
                          to huskar is made.
    :arg bool record_version: whether send huskar version for statistic. It's
                              async and won't influence your app.
    """
    def __init__(self, service, servers=None, username=None, password=None,
                 cluster=OVERALL, cache_dir="/tmp/huskar",
                 lazy=True, handler=None, local_mode=False,
                 record_version=True):
        self.base_path = BASE_PATH
        self.service = service
        self.servers = servers
        self.username = username
        self.password = password
        self.cluster = cluster
        self.cache_dir = cache_dir
        self.handler = None
        self.lazy = lazy
        self.local_mode = local_mode
        self.logger = logging.getLogger(self.__class__.__module__)
        self.client = BaseClient(
            servers, username, password, base_path=BASE_PATH,
            local_mode=local_mode, lazy=lazy)

    @lazy_property
    def config(self):
        """A lazy-initialized instance of :class:`~.components.config.Config`
        """
        return Config(client=self.client,
                      service=self.service,
                      cluster=self.cluster,
                      cache_cls=self._cache_cls,
                      local_mode=self.local_mode,
                      lazy=self.lazy)

    @lazy_property
    def switch(self):
        """A lazy-initialized instance of :class:`~.components.switch.Switch`
        """
        return Switch(client=self.client,
                      service=self.service,
                      cluster=self.cluster,
                      cache_cls=self._cache_cls,
                      local_mode=self.local_mode,
                      lazy=self.lazy)

    @lazy_property
    def service_registry(self):
        """A lazy-initialized instance of
        :class:`~.components.service_registry.ServiceRegistry`
        """
        return ServiceRegistry(client=self.client,
                               service=self.service,
                               cluster=self.cluster)

    @lazy_property
    def service_consumer(self):
        """A lazy-initialized instance of
        :class:`~.components.service_consumer.ServiceConsumer`
        """
        return ServiceConsumer(client=self.client,
                               service=self.service,
                               cluster=self.cluster,
                               cache_cls=self._cache_cls,
                               local_mode=self.local_mode,
                               lazy=self.lazy)

    def _cache_cls(self, key):
        if self.cache_dir:
            filename = SERVICE_CACHE_FILENAME.format(service=self.service,
                                                     cluster=self.cluster,
                                                     key=key)
            cache_path = os.path.join(self.cache_dir,
                                      to_legal_filename(filename))
            try:
                return CachedDict(cache_path)
            except Exception:
                self.logger.error('initializing cache failed, '
                                  'falling back to in-memory dict',
                                  exc_info=True)

        return {}

    def start(self):
        """Start internal ZooKeeper client and watching node changes."""
        if not self.local_mode:
            self.client.start()
            self.switch.start()
            self.config.start()

    def stop(self):
        """Stop watching nodes changes and internal ZooKeeper client"""
        if not self.local_mode:
            for name in ("config", "switch", "service_consumer"):
                component = getattr(self, '_{}'.format(name), None)
                if component is not None:
                    component.stop()
                    delattr(self, '_{}'.format(name))
            self.client.stop()
