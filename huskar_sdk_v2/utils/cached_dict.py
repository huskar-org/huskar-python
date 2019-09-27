from __future__ import absolute_import

import os
import logging
import collections

import simplejson as json
from atomicfile import AtomicFile

from .filelock import FileLock
from .format import char_encoding


logger = logging.getLogger(__name__)


def loads(s):
    return json.loads(s)


def dumps(o):
    return json.dumps(o, sort_keys=True)


class CachedDict(collections.MutableMapping):
    """A file-backed dictionary. Values are serialized to **JSON**. A single
    file contains a single dict. **Do not use this on windows**.
    """
    def __init__(self, filename, default_factory=None):
        self.filename = filename
        self.writer_lock = FileLock("{}.wlock".format(self.filename))
        self.is_writer = False
        self.is_loaded = False
        self.default_factory = default_factory

        self.init()

    def acquire_write(self):
        self.is_writer = self.writer_lock.acquire()
        try:
            logger.debug("changing lock file permission to 666")
            os.chmod(self.writer_lock.filename, 0o666)
        except OSError:
            logger.debug("changing lock file permission failed")
        return self.is_writer

    def release_write(self):
        self.is_writer = False
        return self.writer_lock.release()

    def init(self):
        self._d = {}
        self._make_folder()
        # load data from file
        self.reload()

    def _make_folder(self):
        # create folder and change its permission
        dirname = os.path.dirname(self.filename)
        if dirname:
            if not os.path.isdir(dirname):
                logger.debug("creating cache folder: %s", dirname)
                try:
                    os.makedirs(dirname)
                except OSError:
                    logger.debug("create cache dir failed: %s", dirname)
            try:
                logger.debug("changing cache folder permission to 777...")
                os.chmod(dirname, 0o777)
            except OSError:
                logger.debug(
                    "cache folder permission change failed: %s", dirname)

    def reload(self):
        if not os.path.isfile(self.filename):
            logger.debug("no cache file found, nothing loaded")
            return

        try:
            with open(self.filename, "rb") as f:
                content = f.read()
        except Exception:
            logger.warn("reading cache file failed", exc_info=True)
            content = None

        if not content:
            return

        try:
            obj = loads(content)
            if not isinstance(obj, dict):
                raise Exception
            else:
                self._d = obj
        except Exception:
            logger.warn("malformed cache file", exc_info=True)
        else:
            self.is_loaded = True

    def __repr__(self):
        return "CachedDict({})".format(self.filename)

    def __contains__(self, key):
        return key in self._d

    def __getitem__(self, key):
        if key not in self._d and self.default_factory:
            self._d[key] = self.default_factory()
        return self._d[key]

    def __setitem__(self, key, value):
        if self._d.get(key) == value:
            logger.debug("redundant write ignored: %s", key)
            return

        # check for JSON serializable
        dumps(value)
        dumps(key)

        self._d[key] = value
        # write to file
        self.save()

    def __delitem__(self, key):
        if key not in self._d:
            logger.debug("redundant delete ignored: %s", key)
            return

        self._d.pop(key)
        # delete from file
        self.save()

    def __len__(self):
        return len(self._d)

    def __iter__(self):
        return iter(self._d)

    def save(self):
        if not self.acquire_write():
            logger.debug("writer exists, will not save")
        else:
            content = dumps(self._d)
            # write to file
            try:
                with AtomicFile(self.filename, createmode=0o666) as f:
                    f.write(char_encoding(content))
                    f._fp.flush()
                    os.fsync(f.fileno())
            except Exception:
                logger.error("save to cache file failed", exc_info=True)
                self.release_write()
            # change permission
            try:
                os.chmod(self.filename, 0o666)
            except Exception:
                logger.debug(
                    "cache file permission change failed: %s", self.filename)

    def clear(self):
        self._d.clear()
        # clear in file
        self.save()

    def close(self):
        logger.debug("releasing write lock")
        self.release_write()
