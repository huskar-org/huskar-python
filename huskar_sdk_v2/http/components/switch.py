# -*- coding: utf-8 -*-

import time
import random
import functools
import logging

from ..ioloops import IOLoop, ProcessorException
from . import OverAllOverlayMixin

logger = logging.getLogger(__name__)


def try_convert_to_float(value):
    try:
        return float(value)
    except Exception:
        return value


class Switch(OverAllOverlayMixin):
    def __init__(self, app_id, cluster):
        super(Switch, self).__init__(app_id, cluster)
        self.rand = random.Random(time.time())
        self.default_state = True
        self.client.add_value_processor(self.value_processor)

    @property
    def client(self):
        return IOLoop.current().watched_switches

    @classmethod
    def value_processor(cls, value):
        try:
            value['value'] = try_convert_to_float(value['value'])
            return value
        except Exception:
            raise ProcessorException

    def set_default_state(self, state):
        self.default_state = state

    def is_switched_on(self, name, default=None):
        value = self.get(name)
        if isinstance(value, (int, float)):
            return self.rand.randint(1, 10000) / 100.0 <= value
        return default if default is not None else self.default_state

    def bind(self, name, default=None):
        def wrapper(func):
            switch_name = func.func_name if name is None else name

            @functools.wraps(func)
            def wrapper2(*args, **kwds):
                is_switched_on = self.is_switched_on(switch_name)
                logger.debug(
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
