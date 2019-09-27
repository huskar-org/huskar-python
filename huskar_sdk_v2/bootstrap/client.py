from __future__ import absolute_import

import os
import time
import logging
import random
import warnings
from contextlib import contextmanager
from functools import partial

from blinker import Namespace
try:
    from kazoo.client import KazooClient, KazooState
    from kazoo.handlers.gevent import SequentialGeventHandler
    from kazoo.exceptions import KazooException
    from kazoo.security import make_digest_acl
    from kazoo.protocol.states import EventType
except ImportError:
    has_kazoo = False
else:
    has_kazoo = True

from huskar_sdk_v2.utils import combine
from huskar_sdk_v2.utils.format import char_encoding, char_decoding
from huskar_sdk_v2.exceptions import OperationFailedException


logger = logging.getLogger(__name__)
# TODO:
# 1. scoped session with long connections.


class ConfigMeta(object):
    """A wrap around Kazoo's ``ZnodeStat`` structure"""
    def __init__(self, kazoo_state):
        self.__kazoo_state = kazoo_state

    @property
    def version(self):
        """The number of changes to the data of this znode."""
        return self.__kazoo_state.version

    @property
    def is_deleted(self):
        return self.__kazoo_state is None


class BaseClient(object):
    """A wrap around Kazoo's ZooKeeper Client.

    :arg str server: Comma-separated list of hosts to connect to (e.g.
                     127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
    :arg str base_path: The prefix added to the path for all operation.
    """
    def __init__(self, servers=None, username=None, password=None,
                 base_path='/', retry_max_delay=2, max_retries=None,
                 handler=None, local_mode=False, lazy=True):
        if has_kazoo:
            self.local_mode = local_mode
            self.handler_class = SequentialGeventHandler
        else:
            self.local_mode = True
            self.handler_class = None
        self.base_path = base_path
        self.watched_blinker = Namespace()
        self.watched_node = {}
        self.watched_path = {}
        self.watched_path_callback = set()
        self.servers = servers or '127.0.0.1:2181'
        self.username = username
        self.password = password
        self._client = None
        self._client_lock = None
        self.default_acl = []
        self.retry_max_delay = retry_max_delay
        self.max_retries = max_retries
        self.lazy = lazy

        #: ``True`` if the :meth:`BaseClient.start` is in progress at least.
        #: You should never check the session state from this attribute. It
        #: will not be assigned ``False`` until the :meth:`BaseClient.stop`
        #: has being called explicitly, even if the connection is lost already.
        self.started = False

        #: The background threading to manage the aliveness of session.
        self.starting = None

        # record the pid when Huskar inits
        # Huskar can't be run in multiprocess env
        self.huskar_pid = os.getpid()

        # record zk actual connection server
        self.host = None

    @property
    def connected(self):
        """``True`` if the session is established."""
        if self._client is None:
            return False
        return self._client.connected

    @property
    def client(self):
        if self._client is None:
            # XXX The add_auth is not a concurrency safe operation for
            # ZooKeeper and it may cause xids mismatch. So we always pass
            # auth data via constructor then Kazoo will submit it immediately
            # after session established.
            auth_data = set()
            if self.username and self.password:
                digest_auth = '%s:%s' % (self.username, self.password)
                auth_data.add(('digest', digest_auth))
            self._client = KazooClient(
                hosts=self.servers,
                handler=self.handler_class(),
                retry_max_delay=self.retry_max_delay,
                max_retries=self.max_retries,
                auth_data=auth_data,
            )
        return self._client

    @client.deleter
    def client(self):
        self._client._reset()
        self._client = None
        self._client_lock = None

    @property
    def client_lock(self):
        if self._client_lock is None:
            self._client_lock = self.lock_object()
        return self._client_lock

    def spawn(self, func, *args, **kwargs):
        if not has_kazoo:
            from gevent import spawn
            return spawn(func, *args, **kwargs)
        return self.client.handler.spawn(func, *args, **kwargs)

    def lock_object(self):
        if not has_kazoo:
            from gevent.lock import Semaphore
            return Semaphore()
        return self.client.handler.lock_object()

    def event_object(self):
        if not has_kazoo:
            from gevent.event import Event
            return Event()
        return self.client.handler.event_object()

    def get_full_path(self, node):
        return combine(self.base_path, node)

    def _state_listener(self, state):
        """Record state changes of client"""
        if state == KazooState.LOST:
            logger.warning(
                'Register somewhere that the session was lost with %s',
                self.host)
            self.host = None
        elif state == KazooState.SUSPENDED:
            logger.warning('Zookeeper disconnected with %s', self.host)
            self.host = None
        else:
            try:
                self.host = self.client._connection._socket.getpeername()
            except Exception:
                pass
            logger.warning(
                'Zookeeper connected/reconnected with %s', self.host)

    def watch_key(self, key):
        """Watch a node for data updates and emits a signal each time it
        changes.

        Return ``False`` if ``node`` not exists else ``True``.

        The signal is named as the node, it's emitted together with the new
        value of node and an instance of :py:class:`ConfigMeta` as data.
        """
        if self.local_mode:
            logger.info('Huskar is in local mode, skip watch_key: %r', key)
            return False

        full_path = self.get_full_path(key)
        if not self.exists(key):
            logger.warn("Node %s doesn't exist, watch failed" % full_path)
            return False

        if key not in self.watched_node:
            data_watch = self.call_client('DataWatch', full_path)
            self.watched_node[key] = data_watch
            try:
                data_watch(partial(self.trigger_watched_key, self.client, key))
            except Exception:
                logger.exception('watch failed %s', full_path)
                return False
        return True

    def unwatch_key(self, key):
        self.watched_node.pop(key, None)

    def watch_path(self, path, callback):
        if self.local_mode:
            logger.info('Huskar is in local mode, skip watch_path: %r', path)
            return False

        full_path = self.get_full_path(path)

        if callback not in self.watched_path_callback:
            signal = self.watched_blinker.signal(('children', path))
            signal.connect(callback)
            self.watched_path_callback.add(callback)

        if path not in self.watched_path:
            data_watch = self.call_client('DataWatch', full_path)
            children_watch = self.call_client('ChildrenWatch', full_path)
            self.watched_path[path] = (data_watch, children_watch)
            data_watch(partial(
                self.trigger_watched_path_stat, self.client, path))
            children_watch(partial(
                self.trigger_watched_path, self.client, path))

    def unwatch_path(self, path):
        self.watched_path.pop(path, None)

    def trigger_watched_key(self, client, key, value, state):
        if client is not self.client:
            return False                    # client changed
        if key not in self.watched_node:
            return False                    # node unwatched
        if value is None:
            self.unwatch_key(key)           # node removed
        signal = self.watched_blinker.signal(key)
        signal.send((char_decoding(value), ConfigMeta(state)))

    def trigger_watched_path(self, client, path, children):
        if client is not self.client:
            return False                    # client changed
        if path not in self.watched_path:
            return False                    # path unwatched
        signal = self.watched_blinker.signal(('children', path))
        signal.send(children)

    def trigger_watched_path_stat(self, client, path, data, stat, event):
        if client is not self.client:
            return False                    # client changed
        data_watch, children_watch = self.watched_path.get(path, (None, None))
        if children_watch is None:
            return False                    # path unwatched
        if (event is not None and
                event.type == EventType.CREATED and
                children_watch._stopped):
            children_watch._stopped = False
            children_watch._watcher(event)  # watcher restart
            logger.info('%r restarted by %r', children_watch, event)

    def start(self, timeout=2):
        """Connect to ZooKeeper.

        This method will ensure that there is a background threading which
        trys to keep the session alive, until you call the
        :meth:`BaseClient.stop`.

        :arg int timeout: The maximun waiting seconds for connection
                          established.
        :returns: ``True`` if the connection is established.
        """
        if self.local_mode:
            logger.warn("Huskar working in local_mode, won't start")
            return True

        if self.connected:
            logger.debug('Huskar already connected, start canceled')
            return True

        with self.client_lock:
            if not self.started:
                self.started = True
                self.starting = self.spawn(self._start)
        return self.client._live.wait(timeout)

    def _start(self):
        while self.started:
            client = self.client
            try:
                client._connection.connection_stopped.wait()
                logger.info('Huskar is connecting to ZooKeeper')

                with self.client_lock, \
                        self.suppress_kazoo_exception('huskar_sdk_v2.start'):
                    client.stop()
                    client.start()
                    client.add_listener(self._state_listener)
                    client.ensure_path(self.get_full_path(''))

                logger.info('Huskar connected')
            except Exception:
                logger.exception('Unexpected error during Huskar connecting')

            # Sleep 1.0 ~ 3.0 seconds to prevent from re-connecting storm
            client.handler.sleep_func(random.randint(10, 30) / 10.0)

    def stop(self):
        """Stop client and close the connection."""
        # disconnect signals
        self.watched_blinker = Namespace()
        self.watched_node = {}
        self.watched_path = {}
        self.watched_path_callback = set()

        if not getattr(self, '_client', None):
            return
        if getattr(self.starting, 'kill', None):
            self.starting.kill()
        with self.client_lock:
            self.started = False
            with self.suppress_kazoo_exception('huskar_sdk_v2.stop'):
                self.client.remove_listener(self._state_listener)
                self.client.stop()
                self.client.close()
            del self.client
        if self.starting is not None:
            self.starting.join()
        self.starting = None

    def call_client(self, api_name, *args, **kwds):
        if self.local_mode:
            logger.info(
                'Huskar is in local mode, operation failed: %s', api_name)
            return

        if api_name in ('start', 'stop'):
            raise ValueError('Should use wrapped command instead.')

        if self.lazy and not self.started:
            self.start()

        with self.suppress_kazoo_exception(api_name):
            return getattr(self.client, api_name)(*args, **kwds)
        return False

    @contextmanager
    def suppress_kazoo_exception(self, api_name='unknown'):
        try:
            yield
        except (KazooException, self.client.handler.timeout_exception):
            logger.exception('ZooKeeper Error (%s)' % api_name)

    def get(self, key):
        result = self.call_client('get', self.get_full_path(key))
        if result:
            value, state = result
            return char_decoding(value), ConfigMeta(state)
        raise OperationFailedException(
            'get path %s failed: %s' % (key, result))

    def exists(self, path):
        """True if a path exists."""
        return self.call_client('exists', self.get_full_path(path))

    def add_listener(self, hook_function):
        warnings.warn(DeprecationWarning(
            'This method will be removed in next release. Please call client '
            'directly or use the call_client wrapper.'
        ))
        return self.call_client('add_listener', hook_function)

    def add_auth(self, username, password):
        warnings.warn(DeprecationWarning(
            'This method will be removed in next release. Please DO NOT USE.'
        ))

    def add_default_acl(self, username, password, read=False, write=False,
                        create=False, delete=False, admin=False, all=False):
        warnings.warn(DeprecationWarning(
            'This method will be removed in next release. Please DO NOT USE.'
        ))
        if username and password:
            acl = make_digest_acl(username, password, read, write, create,
                                  delete, admin, all)
            self.default_acl.append(acl)
            self.client.default_acl = self.default_acl

    def create(self, path, value=b"", ephemeral=False, makepath=False,
               retry_times=0, interval=2):
        """
        :arg retry_times: the times of retrying to create node when the node
                            has existed. Especially useful when the servers
                            restart quickly in server register
                            0: don't retry
        :arg interval:  time(second) between retry

        """
        while retry_times >= 0:
            retry_times -= 1
            if False is self.call_client('create',
                                         self.get_full_path(path),
                                         value=char_encoding(value),
                                         ephemeral=ephemeral,
                                         makepath=makepath):
                if retry_times > 0:
                    time.sleep(interval)
                    continue
            else:
                break

    def ensure_path(self, path, acl=None):
        return self.call_client('ensure_path', self.get_full_path(path), acl)

    def set_data(self, path, value):
        return self.call_client('set',
                                self.get_full_path(path),
                                char_encoding(value))

    def delete(self, path, recursive=False):
        return self.call_client('delete',
                                self.get_full_path(path),
                                recursive=recursive)
