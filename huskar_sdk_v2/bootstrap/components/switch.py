from __future__ import absolute_import

import time
import random
import functools

from huskar_sdk_v2.six import iteritems
from huskar_sdk_v2.utils import (
    combine, decode_key, encode_key, get_function_name)
from huskar_sdk_v2.consts import CACHE_KEYS, SWITCH_SUBDOMAIN
from . import SignalComponent, Watchable, require_connection


class Switch(SignalComponent, Watchable):
    """A component of Huskar for switching.

    Switch is usually used to enable or disable the API, it also has the
    ability to limit the passing rate.
    """
    SUBDOMAIN = SWITCH_SUBDOMAIN

    def init(self):
        self.default_rate = 100
        self.rand = random.Random(time.time())
        self.switches = self.cache_cls(CACHE_KEYS.SWITCH)
        self.overall_switches = self.cache_cls(CACHE_KEYS.OVERALL_SWITCH)
        self.started = False
        self.started_timeout = self.client.event_object()
        self.ready = self.client.event_object()
        self.lock = self.client.lock_object()
        Watchable.init(self)

    def iteritems(self):
        yielded = set()
        for k, v in iteritems(self.switches):
            yield k, v['value']
            yielded.add(k)

        for k, v in iteritems(self.overall_switches):
            if k not in yielded:
                yield k, v['value']

    def start(self):
        super(Switch, self).start()
        try:
            # init cache
            self.switches.init()
            self.overall_switches.init()
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
        super(Switch, self).stop()
        with self.lock:
            if self.started:
                self.started = False
                self.started_timeout.clear()
                self.ready.clear()
                self.client.unwatch_path(self.base_path)
                self.client.unwatch_path(self.overall_base_path)
                for name in self.switches:
                    self.client.unwatch_key(combine(self.base_path, name))
                for name in self.overall_switches:
                    self.client.unwatch_key(
                        combine(self.overall_base_path, name))
        # close cache
        try:
            self.switches.close()
            self.overall_switches.close()
        except AttributeError:
            pass

    def _provision(self):
        # wait for the first established session
        self.client.start(timeout=None)
        if not self.started:
            return

        # ensure the paths exist, or the ChildrenWatch will not work.
        self.client.ensure_path(self.overall_base_path)
        self.client.ensure_path(self.base_path)

        # watch cluster path and overall path
        self.client.watch_path(self.base_path, self.register_switch)
        self.client.watch_path(
            self.overall_base_path, self.register_overall_switch)

        self.ready.set()

    def register_switch(self, nodes):
        self._register_switch(self.base_path, nodes, self.switches)

    def register_overall_switch(self, nodes):
        self._register_switch(self.overall_base_path,
                              nodes,
                              self.overall_switches)

    def _register_switch(self, path, nodes, switches):
        callback = functools.partial(self._trigger_switch, switches)
        for n in nodes:
            self._connect_signal_by_basename_and_nodename(path, n, callback)
            self.client.watch_key(combine(path, n))

        for n in set(switches):
            s = switches[n]
            if not isinstance(s, dict):
                self.logger.warn("malformed data in switches: %s", switches)
                continue
            if path == s.get('path', None) and encode_key(n) not in nodes:
                self._disconnect_signal(combine(path, encode_key(n)))
                self.client.unwatch_key(combine(path, encode_key(n)))
                switches.pop(n)

    def _trigger_switch(self, switches, path, name, value_state):
        name = decode_key(name)
        value, state = value_state
        if state.is_deleted:
            self.logger.info('node: %s removed', combine(path, name))
            self.client.unwatch_key(combine(path, name))
            switches.pop(name, None)
        else:
            self.logger.debug('switch triggered: %s -> %s', name, value)
            try:
                switches[name] = {'value': float(value), 'path': path}
            except (TypeError, ValueError):
                self.logger.warning(
                    "wrong value type for switch %s: %s", name, value)
        self.notify_watchers(name, self.is_switched_on)

    def set_default_rate(self, rate):
        """Set default state by percentage.

        :arg int rate: ``0-100(default)`` as percentage, e.g. ``30`` means the
                       switch has 30% chance of being on. ``0`` for always off,
                       ``100`` for always on, see :meth:`set_default_rate`"""
        if not isinstance(rate, int):
            raise TypeError("Default rate should be int, get: %s" % rate)
        self.default_rate = rate

    def set_default_state(self, state):
        """Set default state of switch. This is equivalent to
        ``self.set_default_rate(100 if state else 0)``

        :arg state: ``True`` for **ON** (default) ``False`` for **OFF**.
        """
        self.set_default_rate(100 if state else 0)

    def get_full_name(self, name=None):
        return combine(self.base_path, name) if name else self.base_path

    # TODO: this method name is confusing, the default should be eliminated
    #       otherwise the method name should change to verb
    @require_connection
    def is_switched_on(self, name, default=None):
        """Get the current state of switch by ``name``.

        The result of this method may be outdated if the ZooKeeper connection
        is lost. If this happened, a warning logging will be recorded.

        :param default: This will be returned if the switch is not found.
        :returns: ``True`` or ``False`` decided by the pass rate of switch.
        """
        if (not self.client.local_mode and not self.ready.is_set() and
                self.started_timeout.is_set()):
            self.logger.warning(
                'Switch %r may be outdated caused by lost connection', name)

        if name in self.switches:
            pass_percent = self.switches[name].get('value')
        elif name in self.overall_switches:
            pass_percent = self.overall_switches[name].get('value')
        elif default is not None:
            return default
        else:
            pass_percent = self.default_rate

        if pass_percent == 100:
            return True
        elif pass_percent == 0:
            return False
        else:
            # to support float pass_percent, e.g. 0.01 means 1/10000
            return self.rand.randint(0, 10000) / 100.0 <= pass_percent

    @require_connection
    def bind(self, name=None, default=None):
        """Decorator for binding switch.

        .. code:: python

            @bind(default='default')
            def api():
                return 'value'

        :arg str name: indicates the name of switch, the name of function is
                       used if not provided.
        :arg default: will be returned if switch is **OFF** return
                      ``None`` if not provided.

        """
        def wrapper(func):
            switch_name = get_function_name(func) if name is None else name

            @functools.wraps(func)
            def wrapper2(*args, **kwds):
                is_switched_on = self.is_switched_on(switch_name)
                self.logger.debug(
                    'Switch %s => %r on calling %r, alternative staff is %r.',
                    switch_name, is_switched_on, func, default)
                if is_switched_on:
                    return func(*args, **kwds)
                elif callable(default):
                    return default()
                else:
                    return default
            return wrapper2
        return wrapper
