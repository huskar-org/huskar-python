from __future__ import absolute_import

import logging
import functools
import collections

import simplejson as json
from blinker import Namespace

from huskar_sdk_v2.utils import combine
from huskar_sdk_v2.consts import COMPONENT_PATH, OVERALL, SIG_CLIENT_RESTART


logger = logging.getLogger(__name__)
blinker = Namespace()


def require_connection(func):
    """A decorator for :py:class:`~.components.BaseComponent` methods to ensure
       ``self.start`` had already invoked.

       Remember to set ``self.started`` in ``self.start`` and clear it in
       ``self.init``.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.lazy and not self.local_mode:
            if not self.started:
                self.start()
            self.started_timeout.wait()
        return func(self, *args, **kwargs)

    # The nowait version will be used by resursive callbacks
    wrapper.nowait = func

    return wrapper


class BaseComponent(object):
    def __init__(self, client, service, cluster=OVERALL, logger_name=None,
                 local_mode=False, lazy=True, **kwargs):
        self.client = client
        self.service = service
        self.cluster = cluster
        self.local_mode = local_mode
        self.lazy = lazy
        self.logger = logging.getLogger(
            logger_name or self.__class__.__module__)

        for k, v in kwargs.items():
            setattr(self, k, v)

        self.init()

    def init(self):
        pass

    @property
    def base_path(self):
        return COMPONENT_PATH.format(subdomain=self.SUBDOMAIN,
                                     service=self.service,
                                     cluster=self.cluster)

    @property
    def overall_base_path(self):
        return COMPONENT_PATH.format(subdomain=self.SUBDOMAIN,
                                     service=self.service,
                                     cluster=OVERALL)


class SignalComponent(BaseComponent):
    def __init__(self, *args, **kwargs):
        self._signal_callbacks = {}
        self._sig_client_restart = blinker.signal(SIG_CLIENT_RESTART)
        super(SignalComponent, self).__init__(*args, **kwargs)

    def _connect_signal_by_basename_and_nodename(self, path, name, callback):
        signal_name = combine(path, name)
        # path, name, value_state will be passed to callback
        _callback = functools.partial(callback, path, name)
        return self._connect_signal(signal_name, _callback)

    def _connect_signal(self, path, callback):
        signal_connector = self.client.watched_blinker.signal(path)
        # only if the signal of switch has no receivers
        if not signal_connector.receivers:
            signal_connector.connect(callback, weak=False)
            self._signal_callbacks[path] = callback

    def _disconnect_signal_by_path_name(self, path, name):
        signal_name = combine(path, name)
        self._disconnect_signal(signal_name)

    def _disconnect_signal(self, signal_name):
        callback = self._signal_callbacks.get(signal_name)
        self.client.watched_blinker.signal(signal_name).disconnect(callback)

    def _disconnect_all_signal(self):
        for signal_name in self._signal_callbacks:
            self._disconnect_signal(signal_name)

    def _on_restart(self, _):
        return self.start()

    def start(self):
        self._sig_client_restart.connect(self._on_restart)

    def stop(self):
        self._sig_client_restart.disconnect(self._on_restart)
        self._disconnect_all_signal()


def try_decode(value):
    try:
        return json.loads(value)
    except Exception:
        return value


class Watchable(object):
    def init(self):
        self.external_watchers = collections.defaultdict(set)

    @require_connection
    def watch(self, name, callback):
        """Watch the value of specified instance.

        :arg callable callback: will be invoked with the new value, when
                                instance value changes.
        """
        self.external_watchers[name].add(callback)

    def on_change(self, name):
        """Decorator for watching instance.

        .. code:: python

            @on_change("key")
            def update(new_value):
                # do some updating here

        :arg str name: the key of instance
        """
        def wrapper(func):
            self.watch(name, func)
            return func
        return wrapper

    def unwatch_key(self, name):
        return self.external_watchers.pop(name, None)

    def notify_watchers(self, name, getter):
        """Try to acquire a new value before triggering the callback to prevent
        from pushing inappropriate value in some edge cases, e.g. when someone
        changes the overall instance but the instance of origin cluster exists.
        """
        try:
            value = getter.nowait(self, name)
        except RuntimeError:
            self.logger.warning(
                'Failed to get value of %s, use the value pushed from kazoo '
                'client to trigger callback', name)
        for callback in self.external_watchers.get(name, []):
            try:
                callback(value)
            except Exception:
                self.logger.error(
                    'Failed to call callback: %r=>%r', name, callback,
                    exc_info=True)
            else:
                self.logger.debug(
                    'callback: %r(%r)=>%r', name, value, callback)
