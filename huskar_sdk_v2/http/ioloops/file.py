# -*- coding: utf-8 -*-

import os
import json
import logging

import gevent
from gevent.event import Event

from huskar_sdk_v2.exceptions import (
    HuskarDiscoveryException, HuskarDiscoveryUserError)
from huskar_sdk_v2.six import reraise
from . import IOLoop
from .entity import Component


logger = logging.getLogger(__name__)


def _file_mtime(fpath):
    if not os.path.isfile(fpath):
        logger.debug('cache file %s is not generated', fpath)
        return None
    try:
        return os.stat(fpath).st_mtime
    except OSError:
        logger.warning('stat file %s error:', fpath, exc_info=True)
        return None


def _file_content(fpath):
    try:
        with open(fpath, 'r') as f:
            return json.load(f)
    except Exception:
        logger.warning('read file %s error:', fpath, exc_info=True)
        return None


class FileCacheIOLoop(IOLoop):
    '''
    FileCacheClient is responsible for monitoring local cache dir.
    '''
    def initialize(self, url, token, cache_dir="/tmp/huskar",
                   retry_acquire_gap=60, check_file_stat_gap=5):
        super(FileCacheIOLoop, self).initialize(url, token, cache_dir)
        self.retry_acquire_gap = retry_acquire_gap
        self.check_file_stat_gap = check_file_stat_gap

        self.started = Event()
        self.stopped = Event()
        self.tick_loop = None
        self.check_loop = None

        self.watched_switches = Component(self, 'switches', None)
        self.watched_configs = Component(self, 'configs', None)
        self.watched_services = Component(self, 'services', None)

        self.components = {
            'configs': self.watched_configs,
            'switches': self.watched_switches,
            'services': self.watched_services
        }
        self.components_paths = {
            name: os.path.join(self.cache_dir, name + '_cache.json')
            for name in self.components.keys()
        }
        self.files_stat = {}.fromkeys(self.components.keys(), 0)
        self.first_all_file_changed = False

    def on_watch_list_changed(self, component_name):
        fpath = self.components_paths[component_name]
        self.update_component(fpath, component_name)

    def wait(self, timeout=11.0):
        if not self.started.is_set():
            res = self.started.wait(timeout=timeout)
            self.started.set()
            return res
        return True

    def is_running(self):
        return self.tick_loop or self.check_loop

    def is_connected(self):
        return self.started.is_set()

    def run(self):
        self.started.clear()
        self.stopped.clear()
        self.tick_loop = gevent.spawn(self.try_to_be_writer)
        self.check_loop = gevent.spawn(self.start_check_file_stat)

    def stop(self, timeout=None, close_components=True):
        self.started.clear()
        self.stopped.set()
        if close_components:
            self.watched_configs.close()
            self.watched_services.close()
            self.watched_switches.close()
        if self.is_running() and timeout:
            gevent.sleep(timeout)
        return not self.is_running()

    def update_component(self, fpath, component_name):
        values = _file_content(fpath)
        if values is not None:
            self.components[component_name].update(values, full=True, raw=True)

    def start_check_file_stat(self):
        # TODO: We should only check if files are changed, and then call
        # responding `component.cache_dict.reload()` to refresh data.
        first_changed_files = set()

        while not self.stopped.is_set():
            try:
                for name, fpath in self.components_paths.items():
                    st_mtime = _file_mtime(fpath)

                    if st_mtime and st_mtime != self.files_stat[name]:
                        if not self.started.is_set():
                            first_changed_files.add(name)

                        self.files_stat[name] = st_mtime
                        self.update_component(fpath, name)

                if not self.started.is_set():
                    if len(first_changed_files) == len(self.components):
                        self.started.set()
                    else:
                        gevent.sleep(0.3)
                        continue

                gevent.sleep(self.check_file_stat_gap)
            except Exception as error:
                try:
                    reraise(HuskarDiscoveryUserError(
                        error, self.url, 'check file stat failed'))
                except HuskarDiscoveryException as e:
                    self.notify('polling_error', e)
                logger.exception('unexpected error:')

    def try_to_be_writer(self):
        while not self.stopped.is_set():
            if self.acquire_writer_lock():
                from .http import HuskarApiIOLoop
                self.stop(timeout=0.5, close_components=True)
                ioloop = HuskarApiIOLoop(self.url, self.token, self.cache_dir)
                ioloop.install()
                ioloop.run()
                # do not move log upon, if log gives error...
                logger.warning('writer process is down, %d become writer..',
                               os.getpid())
                break
            else:
                gevent.sleep(self.retry_acquire_gap)
