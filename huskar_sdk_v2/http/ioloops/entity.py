# -*- coding: utf-8 -*-

import os
import collections
import logging
import copy

from huskar_sdk_v2.utils.cached_dict import CachedDict
from ..patterns import HookMixIn

from .events import WatchEvent

logger = logging.getLogger(__name__)


class ProcessorException(Exception):
    pass


class Component(HookMixIn):
    FAIL_STRATEGY_IGNORE = "ignore"
    FAIL_STRATEGY_RAISE = "raise"

    value_processors = collections.defaultdict(list)

    def __init__(self, client, name, cache_dir):
        self.init()
        self.name = name
        self.client = client
        self.cache_dir = cache_dir
        self.app_id_cluster_map = collections.defaultdict(set)
        self.values = self.get_values_dict()
        self.fail_mode = False
        self.default_fail_strategy = self.FAIL_STRATEGY_IGNORE

    def set_default_fail_strategy(self, strategy):
        if strategy in (self.FAIL_STRATEGY_IGNORE, self.FAIL_STRATEGY_RAISE):
            self.default_fail_strategy = strategy

    def __str__(self):
        return "<{} id={} name={}>".format(
            self.__class__.__name__,
            id(self),
            self.name
        )

    def set_default_fail_strategy_to_raise(self):
        self.default_fail_strategy = self.FAIL_STRATEGY_RAISE

    def set_default_fail_strategy_to_ignore(self):
        self.default_fail_strategy = self.FAIL_STRATEGY_IGNORE

    def migrate_from(self, obj):
        self.migrate_listeners(obj)
        self.migrate_app_id_cluster_map(obj)
        self.migrate_default_fail_strategy(obj)

    def migrate_default_fail_strategy(self, obj):
        self.default_fail_strategy = obj.default_fail_strategy

    def migrate_app_id_cluster_map(self, obj):
        self.app_id_cluster_map = copy.deepcopy(obj.app_id_cluster_map)

    def add_value_processor(self, func):
        if func not in self.value_processors[self.name]:
            self.value_processors[self.name].append(func)

    def close(self):
        if isinstance(self.values, CachedDict):
            self.values.close()

    def get_values_dict(self):
        if self.cache_dir:
            filename = '{name}_cache.json'.format(name=self.name)
            cache_path = os.path.join(self.cache_dir, filename)
            try:
                return CachedDict(cache_path, default_factory=dict)
            except Exception:
                logger.error('initializing cache failed, '
                             'falling back to in-memory dict',
                             exc_info=True)
        return collections.defaultdict(lambda: collections.defaultdict(dict))

    def add_listener_for_app_id_at_cluster(self, app_id, cluster, func):
        self.add_listener((app_id, cluster), func)

    def __prepare_cluster_map(self, app_id, cluster):
        self.values[app_id].setdefault(cluster, {})

    def remove_watch(self, app_id, cluster, timeout=None):
        if cluster in self.app_id_cluster_map[app_id]:
            self.app_id_cluster_map[app_id].remove(cluster)
            self.client.on_watch_list_changed(self.name)

            self.clear_listeners((app_id, cluster))
            if timeout is not None:
                return self.client.wait_for_next_loop(timeout)

    def batch_add_watch(self, mappings, timeout=None):
        if not mappings:
            return

        added = False
        for app_id, clusters in mappings.items():
            for cluster in clusters:
                if cluster not in self.app_id_cluster_map[app_id]:
                    added = True
                    self.app_id_cluster_map[app_id].add(cluster)

        if added and timeout is not None:
            self.client.on_watch_list_changed(self.name)
            return self.client.wait_for_next_loop(timeout)

    def add_watch(self, app_id, cluster, timeout=None):
        if cluster not in self.app_id_cluster_map[app_id]:
            self.app_id_cluster_map[app_id].add(cluster)
            self.client.on_watch_list_changed(self.name)
            if timeout is not None:
                return self.client.wait_for_next_loop(timeout)

    def get_values_by_app_id_cluster(self, app_id, cluster):
        self.__prepare_cluster_map(app_id, cluster)
        return self.values[app_id][cluster]

    def is_data_loaded(self):
        if isinstance(self.values, CachedDict):
            return self.values.is_loaded
        return bool(self.values)

    def enter_fail_mode(self):
        logger.warning("Failed waiting for huskar connection, "
                       "entering fail_mode")
        self.fail_mode = True

    def test_fail_mode(self):
        if self.fail_mode and self.client.is_connected():
            logger.warning("Connection to huskar is established, "
                           "leaving fail_mode")
            self.fail_mode = False
        return self.fail_mode

    def get(self, app_id, cluster, key, nowait=False, raises=None):
        if not nowait and not self.fail_mode and self.client.wait() is False:
            if not self.is_data_loaded() and raises:
                raise RuntimeError("Startup failed when waiting for huskar")
            self.enter_fail_mode()

        if self.test_fail_mode() and not self.is_data_loaded():
            if self.default_fail_strategy == self.FAIL_STRATEGY_RAISE:
                raise RuntimeError("Startup failed when "
                                   "waiting for huskar connection")

        self.__prepare_cluster_map(app_id, cluster)
        if key in self.values[app_id][cluster]:
            return self.values[app_id][cluster][key]

        if raises:
            raise RuntimeError("Startup failed")
        elif self.fail_mode:
            logger.warning("Key({}) is not found".format(key))

    def exists(self, app_id, cluster, key, nowait=False):
        if not nowait and not self.fail_mode and self.client.wait() is False:
            self.enter_fail_mode()

        self.__prepare_cluster_map(app_id, cluster)
        return key in self.values[app_id][cluster]

    def update(self, values, full=False, raw=False):
        if not values:
            return

        changed = False
        for app_id, clusters in values.items():
            for cluster, entities in clusters.items():
                if cluster not in self.app_id_cluster_map[app_id]:
                    continue
                self.__prepare_cluster_map(app_id, cluster)
                notify_key = (app_id, cluster)
                for key, value in entities.items():
                    if not raw:
                        err = False
                        for processor in self.value_processors[self.name]:
                            try:
                                value = processor(value)
                            except ProcessorException:
                                err = True
                                break
                        if err:
                            continue

                    old_value = self.values[app_id][cluster].get(key)
                    if old_value != value:
                        changed = True
                        self.values[app_id][cluster][key] = value
                        self.notify(
                            notify_key,
                            WatchEvent.make(
                                WatchEvent.KIND_UPDATE,
                                app_id,
                                cluster,
                                key,
                                value)
                        )

        if full:
            # Use `list` to avoid in-place updating
            for app_id in list(self.values.keys()):
                cluster_map = self.values[app_id]
                values.setdefault(app_id, {})

                for cluster in list(cluster_map.keys()):
                    entities = cluster_map[cluster]
                    self.__prepare_cluster_map(app_id, cluster)
                    notify_key = app_id, cluster
                    values[app_id].setdefault(cluster, {})

                    for key in set(
                            entities).difference(values[app_id][cluster]):
                        changed = True
                        entities.pop(key, None)
                        self.notify(
                            notify_key,
                            WatchEvent.make(WatchEvent.KIND_DELETE, app_id,
                                            cluster, key, None)
                        )

        if changed:
            self.save_to_fs()

    def save_to_fs(self):
        if isinstance(self.values, CachedDict):
            self.values.save()

    def delete(self, values):
        if not values:
            return

        for app_id, clusters in values.items():
            for cluster, entities in clusters.items():
                notify_key = (app_id, cluster)
                self.__prepare_cluster_map(app_id, cluster)
                for key, value in entities.items():
                    if key in self.values[app_id][cluster]:
                        self.values[app_id][cluster].pop(key, None)
                        self.notify(
                            notify_key,
                            WatchEvent.make(
                                WatchEvent.KIND_DELETE,
                                app_id,
                                cluster,
                                key,
                                None)
                            )
        self.save_to_fs()

    @property
    def dict(self):
        return {key: list(values) for key, values
                in self.app_id_cluster_map.items()}
