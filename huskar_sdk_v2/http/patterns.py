# -*- coding: utf-8 -*-

import logging
import collections


logger = logging.getLogger(__name__)


class HookMixIn(object):
    def init(self):
        self.event_listeners = collections.defaultdict(set)

    def migrate_listeners(self, obj):
        for event, listeners in obj.event_listeners.items():
            self.event_listeners[event] |= listeners

    def add_listener(self, key, func):
        self.event_listeners[key].add(func)

    def notify(self, key, value):
        for method in self.event_listeners[key]:
            try:
                method(value)
            except:  # noqa
                logger.exception('notify listerners got:')

    def clear_listeners(self, key):
        self.event_listeners[key].clear()


class Configurable(object):
    """A configurable interface is an (abstract) class whose constructor
    acts as a factory function for one of its implementation subclasses.
    The implementation subclass as well as optional keyword arguments to
    its initializer can be set globally at runtime with `configure`.
    """
    __impl_class = None
    __impl_kwargs = None

    def __new__(cls, *args, **kwargs):
        base = cls.configurable_base()
        init_kwargs = {}
        if cls is base:
            impl = cls.configured_class()
            if base.__impl_kwargs:
                init_kwargs.update(base.__impl_kwargs)
        else:
            impl = cls
        init_kwargs.update(kwargs)
        instance = super(Configurable, cls).__new__(impl)
        instance.initialize(*args, **kwargs)
        return instance

    @classmethod
    def configurable_base(cls):
        raise NotImplementedError()

    @classmethod
    def configurable_default(cls):
        raise NotImplementedError()

    @classmethod
    def configure(cls, impl, **kwargs):
        base = cls.configurable_base()
        if not issubclass(impl, cls):
            raise ValueError('Invalid subclass of {}'.format(cls))
        base.__impl_class = impl
        base.__impl_kwargs = kwargs

    @classmethod
    def clear_configure(cls):
        base = cls.configurable_base()
        base.__impl_class = None
        base.__impl_kwargs = None

    @classmethod
    def configured_class(cls):
        base = cls.configurable_base()
        if cls.__impl_class is None:
            base.__impl_class = cls.configurable_default()
        return base.__impl_class
