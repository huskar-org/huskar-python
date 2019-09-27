# -*- coding: utf-8 -*-
import time

from .ioloops.events import WatchEvent


statsd_client = None
ignore_until_time = None


def setup_statsd(client, ignore_until=None):
    global statsd_client, ignore_until_time
    statsd_client = client
    ignore_until_time = ignore_until or (time.time() + 10)


def record_update_event(event):
    if (statsd_client is None or ignore_until_time is None or
            time.time() < ignore_until_time):
        return

    type_ = 'update'
    if event.kind == WatchEvent.KIND_DELETE:
        type_ = 'delete'
    name = 'huskar.http.{}.{}.{}'.format(
        event.app_id, event.cluster, type_)
    statsd_client.incr(name)
