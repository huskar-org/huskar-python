from __future__ import absolute_import

import os
import time
import errno
import fcntl
import logging
import functools
import threading


logger = logging.getLogger(__name__)


def _exclusive_ctx(func):
    @functools.wraps(func)
    def _wrap(self, *args, **kwargs):
        if os.getpid() != self.pid:
            # in child process thus clear ctx
            self.ctx.__dict__.clear()
        return func(self, *args, **kwargs)
    return _wrap


class FileLock(object):
    """A thread-safe advisory lock on a file."""
    def __init__(self, filename, timeout=0):
        self.filename = filename
        self.timeout = timeout
        self.pid = os.getpid()
        self.ctx = threading.local()

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *args):
        self.release()

    @_exclusive_ctx
    def acquire(self, timeout=None):
        if timeout is not 0:
            timeout = timeout or self.timeout

        if not getattr(self.ctx, "fl", None):
            try:
                self.ctx.fl = open(self.filename, 'w')
            except Exception:
                logger.error("acquiring lock failed", exc_info=True)
                return False

        started_at = time.time()
        acquired = False
        while True:
            try:
                fcntl.flock(
                    self.ctx.fl.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                # acquired
                acquired = self.ctx.fl
                break
            except (ValueError, IOError) as e:
                # ValueError occurs if fd closed
                if getattr(e, "errno", None) == errno.EAGAIN:  # try again
                    if time.time() - started_at > timeout:
                        break
                else:
                    break
            time.sleep(0.1)

        if not acquired:
            try:
                self.ctx.fl.close()
            except Exception:
                logger.debug("acquired failed, closing fd failed",
                             exc_info=True)
            finally:
                setattr(self.ctx, "fl", None)
        return bool(acquired)

    @_exclusive_ctx
    def release(self):
        success = True
        fl = getattr(self.ctx, "fl", None)
        if fl and not fl.closed:
            # unlock
            try:
                fl.close()
            except Exception:
                success = False
                logger.debug("releasing lock failed", exc_info=True)

        self.ctx.fl = None

        self.delete()
        return success

    def delete(self):
        # removing file
        try:
            os.remove(self.filename)
        except OSError:
            pass
            # logger.debug("lock file removing failed", exc_info=True)
