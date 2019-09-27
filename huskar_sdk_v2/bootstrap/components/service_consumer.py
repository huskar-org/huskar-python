from __future__ import absolute_import

import functools
import logging
from collections import defaultdict

import simplejson as json
from blinker import Namespace

from huskar_sdk_v2.utils import combine, no_multiprocess_check
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN, COMPONENT_PATH, CACHE_KEYS
from huskar_sdk_v2.exceptions import OperationFailedException
from . import SignalComponent


logger = logging.getLogger(__name__)


class ServiceConsumer(SignalComponent):
    """ServiceConsumer is used to get service instances"""

    SUBDOMAIN = SERVICE_SUBDOMAIN

    def init(self, min_server_num=1):
        self.blinker = Namespace()

        self.services = {}
        self.service_list_change_signal = {}
        self.min_server_num = min_server_num
        self.linked_cluster = {}

        self.watched_service = {}
        self.watched_service_nodes = defaultdict(list)
        self.watched_service_nodes_signals = defaultdict(list)

    def set_min_server_num(self, min_server_num):
        self.min_server_num = min_server_num

    def get_service_cluster_node(self, service, cluster):
        return COMPONENT_PATH.format(
            subdomain=self.SUBDOMAIN, service=service, cluster=cluster)

    def service_instance_path(self, service, cluster):
        # TODO should we use filesystem cache here?
        cluster_path = self.get_service_cluster_node(service, cluster)
        try:
            cluster_info, _ = self.client.get(cluster_path)
        except OperationFailedException as e:
            logger.warning(
                'Failed to get link info, ignore cluster linking: %s', e)
        else:
            if cluster_info:
                try:
                    cluster_info = json.loads(cluster_info)
                    clusters_linked_to = cluster_info['link']
                    # Link to single cluster for now
                    chosen_cluster = clusters_linked_to[0]
                except (KeyError, IndexError, ValueError):
                    logger.warning(
                        'Linking skiped: {0}/{1}'.format(service, cluster))
                except Exception:
                    logger.warning(
                        'Linking skiped: {0}/{1}'.format(service, cluster),
                        exc_info=True)
                else:
                    self.linked_cluster[(service, cluster)] = chosen_cluster
                    cluster_path = self.get_service_cluster_node(
                        service, chosen_cluster)
        return cluster_path

    def unwatch_service(self, service, cluster):
        if (service, cluster) in self.watched_service:
            self.client.unwatch_path(self.watched_service[(service, cluster)])
            self.watched_service.pop((service, cluster))

        if (service, cluster) in self.watched_service_nodes:
            for watch_path in self.watched_service_nodes[(service, cluster)]:
                self.client.unwatch_key(watch_path)
            self.watched_service_nodes[(service, cluster)] = []

        if (service, cluster) in self.watched_service_nodes_signals:
            for path, n in self.watched_service_nodes_signals[(service,
                                                               cluster)]:
                self._disconnect_signal_by_path_name(path, n)
            self.watched_service_nodes_signals[(service, cluster)] = []

    def cluster_linking_changed_handler(self, service, cluster, meta):
        self.watch_service(service, cluster)

    def watch_cluster_for_link(self, service, cluster):
        cluster_path = self.get_service_cluster_node(service, cluster)
        self.client.watch_key(cluster_path)
        self._connect_signal(
            cluster_path,
            functools.partial(
                self.cluster_linking_changed_handler, service, cluster
            ),
        )

    def preprocess_service_mappings(self, mappings):
        return

    def watch_service(self, service, cluster):
        # If is already watched, let's unwatch it. So that if one cluster is
        # changed from an actual cluster to a linking one, changes occur in
        # the original cluster won't affect the apps using the linked cluster.
        self.unwatch_service(service, cluster)

        service_path = self.service_instance_path(service, cluster)

        self.client.ensure_path(service_path)
        self.watch_cluster_for_link(service, cluster)
        # watch service
        self.watched_service[(service, cluster)] = service_path
        self.client.watch_path(
            service_path,
            functools.partial(self._record_switch, service, cluster),
        )

    def get_service_list_change_signal(self, service, cluster):
        key = '{}_{}'.format(service, cluster)
        if key not in self.service_list_change_signal:
            self.service_list_change_signal[key] = self.blinker.signal(
                'service_list_change_signal_{}'.format(key))

        return self.service_list_change_signal[key]

    def trigger_service_list_change_signal(self, service, cluster):
        self.get_service_list_change_signal(service, cluster).send()

    def _record_switch(self, service, cluster, nodes):
        path = self.service_instance_path(service, cluster)
        for n in nodes:
            watch_patch = combine(path, n)
            self.watched_service_nodes[(service, cluster)].append(watch_patch)
            self.watched_service_nodes_signals[(service, cluster)].append(
                (path, n))

            self._connect_signal_by_basename_and_nodename(
                path, n, functools.partial(self._trigger_service, service,
                                           cluster,))
            self.client.watch_key(watch_patch)

        service_cache = self.get_service_cache(service, cluster)

        changed = False

        for n in service_cache.keys():
            if n not in nodes:
                if len(service_cache) >= self.min_server_num:
                    # ensure the services num is no less than min server num # noqa
                    self._disconnect_signal(combine(path, n))
                    service_cache.pop(n)
                    changed = True

        if changed:
            self.trigger_service_list_change_signal(service, cluster)

    def get_service_cache(self, service, cluster):
        key = '{}_{}'.format(service, cluster)
        if key not in self.services:
            # - can't be in table name #noqa
            self.services[key] = \
                self.cache_cls('{}_{}'.format(CACHE_KEYS.SERVICE,
                                              key.replace('-', '_')))
        return self.services[key]

    @no_multiprocess_check
    def _trigger_service(self, service, cluster,
                         service_path, instance_name, value_state):
        value, state = value_state
        service_cache = self.get_service_cache(service, cluster)
        try:
            service_cache[instance_name] = json.loads(value)
        except (TypeError, ValueError):
            logger.warning(
                'The service instance "{0}/{1}/{2}" is broken and '
                'ignored.'.format(
                    service, cluster, instance_name)
            )
        else:
            self.trigger_service_list_change_signal(service, cluster)

    def get_service_instance(self, service, cluster):
        key = '{}_{}'.format(service, cluster)

        if key not in self.services:
            self.services[key] = self.get_service_cache(service, cluster)
            self.watch_service(service, cluster)
        return self.services[key]

    def register_hook_function(self, service, cluster, hook_function,
                               trigger=True):
        """
        param hook_function: hook_function will be called when instance list
                              changes instance_list will be passed to
                              hook_function as a parameter
        param trigger : if True, the hook_function will be called as soon as
                        register_hook_function is called.
        """
        def wrapped_function(*args):
            linked_cluster = self.linked_cluster.get((service, cluster),
                                                     cluster)
            instance_list = self.get_service_instance(service, linked_cluster)
            if callable(hook_function):
                hook_function(instance_list)
        if trigger:
            wrapped_function()
        self.get_service_list_change_signal(service, cluster).\
            connect(wrapped_function, weak=False)
