# -*- coding: utf-8 -*-

import operator


class WatchEvent(tuple):
    KIND_DELETE = 'entity_deleted_event'
    KIND_UPDATE = 'entity_updated_event'

    kind = property(operator.itemgetter(0))
    app_id = property(operator.itemgetter(1))
    cluster = property(operator.itemgetter(2))
    key = property(operator.itemgetter(3))
    value = property(operator.itemgetter(4))

    @classmethod
    def make(cls, kind, app_id, cluster, key, value):
        return cls((kind, app_id, cluster, key, value))
