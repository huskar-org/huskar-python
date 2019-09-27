# -*- coding: utf-8 -*-

from ..statsd import record_update_event
from ..patterns import HookMixIn
from ..ioloops.events import WatchEvent
from ...six import iteritems


OVERALL_CLUSTER_NAME = 'overall'


class BaseComponent(HookMixIn):
    def __init__(self, app_id, cluster):
        self.init()
        self.app_id = app_id
        self.cluster = cluster

    def add_current_app_to_watchlist(self):
        self.add_watch(self.app_id, self.cluster)

    @property
    def client(self):
        raise NotImplementedError

    def add_watch(self, app_id, cluster, timeout=None):
        ret = self.client.add_watch(app_id, cluster, timeout=timeout)
        self.client.add_listener_for_app_id_at_cluster(
            app_id, cluster, self.handle_changes)
        return ret

    def handle_changes(self, watch_event):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def exists(self, key):
        raise NotImplementedError

    def watch(self, name, callback):
        self.add_listener(name, callback)

    def on_change(self, name):
        """Registers a callback function to listen changes of specified key.

        Example::

            @huskar.config.on_change('NAME_LIST')
            def on_name_list_change(value):
                for name in value:
                    print('name: %s' % value)

        :param name: The key of instance in Huskar.
        :returns: The decorator itself.
        """
        def wrapper(func):
            self.watch(name, func)
            return func
        return wrapper

    def set_critical(self):
        self.client.set_default_fail_strategy_to_raise()

    def set_uncritical(self):
        self.client.set_default_fail_strategy_to_ignore()


class OverAllOverlayMixin(BaseComponent):
    def __init__(self, app_id, cluster):
        super(OverAllOverlayMixin, self).__init__(app_id, cluster)
        # OverAllOverlayMixin subclasses should add running app to watch list,
        # because config/switch will always be requested.
        self.add_current_app_to_watchlist()

    def get(self, key, default=None, raises=False, _force_overall=False):
        """Gets a service, switch or config instance from Huskar.

        :param key: The key of instance in Huskar.
        :param default: Optional. Default: ``None``
        :param raises: Optional. Default: ``False``
        :param _force_overall: Bypass the origin-cluster to acquire the overall
            config when _force_overall is True, this is useful when one want to
            merge the valus that is in compound data structure, e.g. json

            .. versionadded: 0.14.5
        """
        if not _force_overall and self.client.exists(self.app_id, self.cluster,
                                                     key):
            value = self.client.get(self.app_id, self.cluster,
                                    key, raises=raises)
        else:
            value = self.client.get(self.app_id, OVERALL_CLUSTER_NAME,
                                    key, raises=raises)
        return value['value'] if value else default

    def exists(self, key):
        """Checks the instance with specified key does exist or not in Huskar.

        :param key: The key of instance in Huskar.
        """
        exists_in_cluster = self.client.exists(self.app_id, self.cluster, key)
        if not exists_in_cluster:
            return self.client.exists(self.app_id, OVERALL_CLUSTER_NAME, key)
        return exists_in_cluster

    def iteritems(self):
        """Generates key-value pairs of all instances from Huskar."""
        yielded = set()
        for cluster in (self.cluster, OVERALL_CLUSTER_NAME):
            for k, v in iteritems(self.client.get_values_by_app_id_cluster(
                    self.app_id, cluster)):
                if k not in yielded:
                    yield k, v['value']
                    yielded.add(k)

    def add_watch(self, app_id, cluster, timeout=None):
        for c in (cluster, OVERALL_CLUSTER_NAME):
            super(OverAllOverlayMixin, self).add_watch(app_id, c, timeout)
            self.client.add_listener_for_app_id_at_cluster(
                app_id, c, record_update_event)

    def handle_changes(self, watch_event):
        if watch_event.cluster == OVERALL_CLUSTER_NAME and \
                self.cluster != OVERALL_CLUSTER_NAME and \
                self.client.exists(self.app_id,
                                   self.cluster,
                                   watch_event.key,
                                   nowait=True,
                                   ):
            return
        if watch_event.kind == WatchEvent.KIND_UPDATE:
            self.notify(watch_event.key, watch_event.value['value'])
        elif watch_event.kind == WatchEvent.KIND_DELETE:
            self.notify(watch_event.key, self.get(watch_event.key))
