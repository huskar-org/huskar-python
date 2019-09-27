# -*- coding: utf-8 -*-

import json
import random
import time
import logging
import socket

import gevent
from gevent.event import Event

from huskar_sdk_v2.consts import (
    USER_AGENT, SOA_MODE_HEADER, SOA_CLUSTER_HEADER
)
from huskar_sdk_v2.exceptions import (
    HuskarDiscoveryException, HuskarDiscoveryUserError,
    HuskarDiscoveryServerError)
from huskar_sdk_v2.six import reraise, iteritems
from huskar_sdk_v2.utils import join_url, Counter
from . import IOLoop
from .entity import Component

logger = logging.getLogger(__name__)


class HuskarApiIOLoop(IOLoop):
    '''
    HuskarApiIOLoop is responsible for running eventloop connected to
    huskar api. The design is to use long polling for all requests, in
    disregard of whenever it's required.
    '''
    def initialize(self, url, token, cache_dir="/tmp/huskar",
                   max_alive_time=10*60, reconnect_gap=60):
        super(HuskarApiIOLoop, self).initialize(url, token, cache_dir)
        self.url_path = join_url(self.url, '/api/data/long_poll')
        self.init_session()
        self.connected = Event()
        self.stop_loop_event = Event()
        self.stopped = Event()
        self.stopped.set()
        self.is_disconnected = Event()
        self.next_watch_completed_event = Event()

        self.greenlet = None
        self.reconnect_gap = reconnect_gap
        self.has_once_connected = False
        self.max_alive_time = (0.8 + 0.2*random.random()) * max_alive_time

        self.watched_services = Component(self, 'services', cache_dir)
        self.watched_configs = Component(self, 'configs', cache_dir)
        self.watched_switches = Component(self, 'switches', cache_dir)

    def on_watch_list_changed(self, component_name):
        if self.connected.is_set():
            self.force_reinit_session_next_round()

    def force_reinit_session_next_round(self):
        # Race risks
        self.last_session_created_time = 0
        self.next_watch_completed_event.clear()

    def wait_for_next_loop(self, timeout):
        return self.next_watch_completed_event.wait(timeout)

    def check_refresh_session(self):
        if not self.next_watch_completed_event.is_set() and \
                self.last_session_created_time != 0:
            self.next_watch_completed_event.set()
        if time.time() - self.last_session_created_time > self.max_alive_time:
            self.init_session()
            return True
        return False

    def init_session(self):
        import requests
        self.session = requests.Session()
        self.session.headers['User-Agent'] = ' '.join([
            USER_AGENT, self.session.headers.get('User-Agent', '')
        ])
        self.session.headers['Authorization'] = self.token
        self.last_session_created_time = time.time()

        if self._soa_mode is None:
            return
        self.session.headers[SOA_MODE_HEADER] = self._soa_mode
        self.session.headers[SOA_CLUSTER_HEADER] = self._soa_cluster

    def is_running(self):
        return self.greenlet

    def run(self):
        if not self.greenlet:
            self.greenlet = gevent.spawn(self.start_long_poll)

    def stop(self, timeout=None, close_components=True):
        self.stop_loop_event.set()
        if close_components:
            self.watched_configs.close()
            self.watched_services.close()
            self.watched_switches.close()

        if timeout is not None:
            return self.stopped.wait(timeout)

    def wait(self, timeout=10.0):
        if not (self.has_once_connected or self.connected.is_set()):
            return self.connected.wait(timeout=timeout)

    def is_connected(self):
        return self.connected.is_set()

    def event_loop(self):
        try:
            from httplib import IncompleteRead  # Py2
        except ImportError:
            from http.client import IncompleteRead  # Py3
        import requests
        fail_count = Counter(0)

        def loop():
            # Use closure to jump around generator gc issue. See
            # https://groups.google.com/forum/#!topic/comp.lang.python/EhAY4ZmWaIw

            try:
                payload = {k: v for k, v in iteritems({
                    'service': self.watched_services.dict,
                    'config': self.watched_configs.dict,
                    'switch': self.watched_switches.dict
                }) if v}

                r = self.session.post(
                    self.url_path,
                    json=payload,
                    stream=True,
                    timeout=3,
                )
                if not r.ok:
                    logger.error(
                        'failed to watch: %d %r', r.status_code, r.text)
                    r.raise_for_status()

                for i in r.iter_lines(chunk_size=4096, decode_unicode=True):
                    self.handle_message(i)
                    fail_count.reset()
                    if not self.connected.is_set():
                        self.connected.set()
                    self.is_disconnected.clear()
                    self.has_once_connected = True
                    if self.stop_loop_event.is_set():
                        return True
                    if self.check_refresh_session():
                        break
            except (socket.gaierror, socket.error,
                    IncompleteRead,
                    requests.RequestException) as error:
                self.connected.clear()
                self.is_disconnected.set()
                if self.stop_loop_event.is_set():
                    logger.info("Stopping huskar connection event loop")
                    return True
                fail_count.incr()
                message = ''
                exc_cls = HuskarDiscoveryServerError
                if (isinstance(error, requests.RequestException) and
                        error.response is not None):
                    response = error.response
                    if response.status_code < 500:
                        exc_cls = HuskarDiscoveryUserError
                    message = 'status_code: {0}, body: {1!r}'.format(
                        response.status_code, response.content[:200])
                try:
                    reraise(exc_cls(error, self.url_path, message))
                except HuskarDiscoveryException as e:
                    self.notify('polling_error', e)
                retry_wait = (0.5+random.random()) * fail_count.get() *\
                    self.reconnect_gap
                logger.warning(
                    'Huskar connection disconnected, '
                    'will retry in %s' % retry_wait, exc_info=True)
                gevent.sleep(retry_wait)
        while True:
            if loop():
                return

    def start_long_poll(self):
        self.connected.clear()
        self.stopped.clear()
        try:
            self.event_loop()
        finally:
            self.stopped.set()
            self.stop_loop_event.clear()
            self.connected.clear()

    def update_watches(self, message, full=False):
        self.watched_services.update(message.get('service'), full=full)
        self.watched_configs.update(message.get('config'), full=full)
        self.watched_switches.update(message.get('switch'), full=full)

    def delete_watches(self, message):
        self.watched_services.delete(message.get('service'))
        self.watched_configs.delete(message.get('config'))
        self.watched_switches.delete(message.get('switch'))

    def handle_message(self, message):
        if self.stopped.is_set():
            return

        if not self.has_once_connected:
            logger.info("Got Huskar messages. Processing...")

        try:
            message = json.loads(message)
        except Exception:
            logger.warning("Error parsing huskar message: %r", message)
            return

        try:
            if message['message'] == 'ping':
                pass
            elif message['message'] == 'update':
                self.update_watches(message['body'])
            elif message['message'] == 'delete':
                self.delete_watches(message['body'])
            elif message['message'] == 'all':
                self.update_watches(message['body'], full=True)
        except Exception as err:
            logger.exception("Error handling huskar api message: %r", err)
