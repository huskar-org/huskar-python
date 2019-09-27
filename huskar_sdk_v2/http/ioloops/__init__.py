# -*- coding: utf-8 -*-

import os
import logging

from huskar_sdk_v2.utils.filelock import FileLock
from huskar_sdk_v2.http.patterns import Configurable, HookMixIn
from huskar_sdk_v2.consts import SOA_MODE_CHOICES
from .entity import ProcessorException

logger = logging.getLogger(__name__)

__all__ = ['IOLoop', 'ProcessorException']


class IOLoop(Configurable, HookMixIn):
    _lockpath = '/tmp/huskar.master'
    _soa_mode = None
    _soa_cluster = None
    _instance = None
    _filelock = None
    _is_writer = False

    def initialize(self, url, token, cache_dir):
        self.url = url
        self.token = token
        self.cache_dir = cache_dir
        self.watched_configs = None
        self.watched_services = None
        self.watched_switches = None

        super(IOLoop, self).init()

    @staticmethod
    def current():
        return IOLoop._instance

    def install(self):
        old_io_loop = IOLoop._instance

        if old_io_loop is not None:
            old_watched_configs = old_io_loop.watched_configs
            old_watched_services = old_io_loop.watched_services
            old_watched_switches = old_io_loop.watched_switches

            self.watched_configs.migrate_from(old_watched_configs)
            self.watched_services.migrate_from(old_watched_services)
            self.watched_switches.migrate_from(old_watched_switches)

            self.migrate_listeners(old_io_loop)

        IOLoop._instance = self

    @classmethod
    def clear_instance(cls):
        if cls._filelock is not None and cls._is_writer:
            cls._filelock.release()
        if cls._instance is not None:
            cls._instance = None

    @classmethod
    def acquire_writer_lock(cls):
        if cls._filelock is None:
            cls._filelock = FileLock(cls._lockpath)
        cls._is_writer = cls._filelock.acquire()
        try:
            os.chmod(cls._filelock.filename, 0o666)
        except OSError:
            logger.debug("changing huskar-writer lock permission failed")
        return cls._is_writer

    @classmethod
    def set_lockpath(cls, path):
        cls._lockpath = path

    @classmethod
    def set_soa_mode_cluster(cls, mode, cluster):
        assert mode is None or mode in SOA_MODE_CHOICES
        cls._soa_mode = mode
        cls._soa_cluster = cluster

    @classmethod
    def configurable_base(cls):
        return IOLoop

    @classmethod
    def configurable_default(cls):
        from .http import HuskarApiIOLoop
        from .file import FileCacheIOLoop
        if cls.acquire_writer_lock():
            return HuskarApiIOLoop
        return FileCacheIOLoop

    def wait(self, timeout=None):
        return True

    def on_watch_list_changed(self, component_name):
        pass

    def wait_for_next_loop(self, timeout):
        return True

    def is_running(self):
        return False

    def run(self):
        raise NotImplementedError

    def stop(self, timeout=None, close_components=True):
        raise NotImplementedError
