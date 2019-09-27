from __future__ import absolute_import

import functools

from huskar_sdk_v2.utils import combine, encode_key, decode_key
from huskar_sdk_v2.six import iteritems
from huskar_sdk_v2.consts import CONFIG_SUBDOMAIN, CACHE_KEYS
from . import SignalComponent, Watchable, try_decode, require_connection


class Config(SignalComponent, Watchable):
    """A component of Huskar for retrieving configurations.

    Configurations are key-value data for specified service **scoped in one or
    all cluster**.
    """
    SUBDOMAIN = CONFIG_SUBDOMAIN

    def init(self):
        self.configs = self.cache_cls(CACHE_KEYS.CONFIG)
        self.overall_configs = self.cache_cls(CACHE_KEYS.OVERALL_CONFIG)
        self.started = False
        self.started_timeout = self.client.event_object()
        self.ready = self.client.event_object()
        self.lock = self.client.lock_object()
        self.provision = None
        Watchable.init(self)

    def iteritems(self):
        yielded = set()
        for k, v in iteritems(self.configs):
            yield k, v
            yielded.add(k)

        for k, v in iteritems(self.overall_configs):
            if k not in yielded:
                yield k, v

    def start(self):
        super(Config, self).start()
        try:
            # init cache
            self.configs.init()
            self.overall_configs.init()
        except AttributeError:
            pass

        if self.started:
            return
        with self.lock:
            if self.started:
                return
            self.started = True
            self.client.spawn(self._provision).join(10)
            self.started_timeout.set()

    def stop(self):
        super(Config, self).stop()
        with self.lock:
            if self.started:
                self.started = False
                self.started_timeout.clear()
                self.ready.clear()
                self.client.unwatch_path(self.base_path)
                self.client.unwatch_path(self.overall_base_path)
                for name in self.configs:
                    self.client.unwatch_key(combine(self.base_path, name))
                for name in self.overall_configs:
                    self.client.unwatch_key(
                        combine(self.overall_base_path, name))
        # close cache
        try:
            self.configs.close()
            self.overall_configs.close()
        except AttributeError:
            pass

    def _provision(self):
        # wait for the first established session
        self.client.start(timeout=None)
        if not self.started:
            return

        # ensure the paths exist, or the ChildrenWatch will not work.
        self.client.ensure_path(self.base_path)
        self.client.ensure_path(self.overall_base_path)

        # watch cluster path and overall path
        self.client.watch_path(self.base_path, self.register_config)
        self.client.watch_path(
            self.overall_base_path, self.register_overall_config)

        self.ready.set()

    def register_config(self, nodes):
        self._register_config(nodes, self.base_path, self.configs)

    def register_overall_config(self, nodes):
        self._register_config(
            nodes, self.overall_base_path, self.overall_configs)

    def _register_config(self, nodes, base_path, configs):
        callback = functools.partial(self._trigger_config, configs)
        for raw_key in nodes:
            self._connect_signal_by_basename_and_nodename(
                base_path, raw_key, callback)
            self.client.watch_key(combine(base_path, raw_key))

        for raw_key in set(configs):
            key = encode_key(raw_key)
            if key in nodes:
                continue
            self._disconnect_signal(combine(base_path, key))
            self.client.unwatch_key(combine(base_path, key))
            configs.pop(raw_key)

    def _trigger_config(self, configs, path, name, value_state):
        name = decode_key(name)
        value, state = value_state
        if state.is_deleted:
            self.logger.info('node: %s removed', combine(path, name))
            configs.pop(name, None)
            self.client.unwatch_key(combine(path, name))
        else:
            self.logger.debug('config changed: %s', name)
            configs[name] = value
        self.notify_watchers(name, self.get)

    @require_connection
    def exists(self, name):
        """To test if the given ``name`` is set as a configuration."""
        return name in self.configs or name in self.overall_configs

    @require_connection
    def get(self, name, default=None, raises=False, _force_overall=False):
        """Get configuration value by ``name``.

        We assume you invoke this method during application bootstrap. If you
        plan to invoke this in runtime, you may need to handle the
        ``RuntimeError`` by yourself.

        :arg str name: The configuration name which will be searched in
                       **cluster scope** then **overall scope**, that means
                       **cluster configuration** will override
                       **overall configuration** if they share the same name.
        :arg default: This will be returned if ``name`` not found.
        :raises RuntimeError: Raised if the filesystem cache is not available
                              and the ZooKeeper connection is lost.
        """
        r = default
        if not _force_overall and name in self.configs:
            r = try_decode(self.configs[name])
        elif name in self.overall_configs:
            r = try_decode(self.overall_configs[name])
        elif (
            not self.client.local_mode and
            not self.ready.is_set() and self.started_timeout.is_set() and
            not getattr(self.configs, 'is_loaded', False) and
            not getattr(self.overall_configs, 'is_loaded', False)
        ):
            raise RuntimeError(
                'The connection is lost and the cache is unavailable.')
        return r
