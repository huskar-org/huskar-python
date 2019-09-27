# -*- coding: utf-8 -*-

import simplejson as json

from . import OverAllOverlayMixin
from ..ioloops import IOLoop


def try_decode(value):
    try:
        return json.loads(value)
    except Exception:
        return value


class Config(OverAllOverlayMixin):
    def __init__(self, app_id, cluster):
        super(Config, self).__init__(app_id, cluster)
        self.client.add_value_processor(self.value_processor)

    @property
    def client(self):
        return IOLoop.current().watched_configs

    @classmethod
    def value_processor(cls, value):
        value['value'] = try_decode(value['value'])
        return value
