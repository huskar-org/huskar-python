# -*- coding: utf-8 -*-

import json

from . import BaseComponent
from ..ioloops import IOLoop
from ..ioloops.events import WatchEvent


class Service(BaseComponent):
    add_service = BaseComponent.add_watch

    def __init__(self, app_id, cluster):
        super(Service, self).__init__(app_id, cluster)

    @property
    def client(self):
        return IOLoop.current().watched_services

    def notify_listeners_of_node_changes(self, app_id, cluster):
        self.notify(
            (app_id, cluster),
            self.get_service_node_list(app_id,
                                       cluster)
            )

    def handle_changes(self, watch_event):
        if watch_event.kind in (WatchEvent.KIND_UPDATE,
                                WatchEvent.KIND_DELETE):
            self.notify_listeners_of_node_changes(
                watch_event.app_id, watch_event.cluster
                )

    def get_service_node_list(self, app_id, cluster):
        return {
            name: json.loads(node['value'])
            for name, node in self.client.get_values_by_app_id_cluster(
                app_id, cluster).items()
            }

    def register_hook_function(self, app_id, cluster, hook_function,
                               trigger=True):
        if trigger:
            self.add_service(app_id, cluster, timeout=3.0)
        else:
            self.add_service(app_id, cluster)

        self.add_listener((app_id, cluster), hook_function)
        if trigger is True:
            self.notify_listeners_of_node_changes(app_id, cluster)

    def preprocess_service_mappings(self, mappings):
        return self.client.batch_add_watch(mappings=mappings, timeout=3.0)

    def set_min_server_num(self, min_server_num):
        pass

    def unwatch_service(self, app_id, cluster, timeout=None):
        return self.client.remove_watch(app_id, cluster, timeout=timeout)
