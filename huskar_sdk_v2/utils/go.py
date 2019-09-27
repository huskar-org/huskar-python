from __future__ import absolute_import

import gevent
import threading


ENV_THREADING = 1
ENV_GREENLET = 2


class GoGreenlet(gevent.greenlet.Greenlet):
    def is_alive(self):
        return self and not self.dead


class ContextAwareMixin(object):
    ENV = ENV_THREADING

    def set_gevent(self):
        self.ENV = ENV_GREENLET

    def set_threading(self):
        self.ENV = ENV_THREADING

    def spawn(self, target, *args):
        if self.ENV is ENV_GREENLET:
            return GoGreenlet.spawn(target, *args)
        elif self.ENV is ENV_THREADING:
            td = threading.Thread(target=target, args=args)
            td.daemon = True
            td.start()
            return td
        else:
            raise Exception("unknown ENV")
