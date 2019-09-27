# -*- coding: utf-8 -*-

import sys

__all__ = ['iteritems']

PY2 = sys.version_info[0] == 2


if not PY2:
    def iteritems(d):
        return iter(d.items())
else:
    def iteritems(d):
        return d.iteritems()

if not PY2:
    def reraise(exception):
        _, _, exc_tb = sys.exc_info()
        raise exception.with_traceback(exc_tb)
else:
    exec('''
def reraise(exception):
    _, _, exc_tb = sys.exc_info()
    raise exception, None, exc_tb
''')

if not PY2:
    unicode = str
else:
    unicode = unicode
