# -*- coding: utf-8 -*-

import os
import hashlib
import logging

from .ioloops import IOLoop
from .components.config import Config
from .components.switch import Switch
from .components.service import Service
from ..consts import (
    OVERALL,
    ENV_CACHE_DIR_NAMESPACE,
    ENV_SUPERVISOR_GROUP_NAME,
    ENV_DOCKER_CONTAINER_ID,
)

logger = logging.getLogger(__name__)


class HttpHuskar(object):
    MODE_SINGLEPROCESS = 0
    MODE_MULTIPROCESS = 1

    def __init__(self, app_id, cluster=OVERALL, url=None, token=None,
                 cache_dir="/tmp/huskar", soa_mode=None, soa_cluster=None):
        if not cluster:
            cluster = OVERALL

        # The IOLoop will be initialized once only. Because we don't hope the
        # filesystem cache in division paths.
        if IOLoop.current() is None:
            # The filesystem cache of Huskar SDK need to be isolated between
            # applications even if they are in the same machine.
            # We use 4-tuple (namespace, first_used_app_id, first_used_cluster,
            # token_sha256), to decide the path of cache files.
            namepsace, cache_mode = self._get_namespace_and_mode()
            digest = hashlib.sha256(token.encode('ascii')).hexdigest()[:6]
            cache_dir = os.path.join(cache_dir, "{}@{}@{}@{}".format(
                namepsace, app_id, cluster, digest))
            soa_cluster = soa_cluster or cluster
            self.setup_ioloop(url, token, soa_mode, soa_cluster,
                              cache_dir, cache_mode)

        self.app_id = app_id
        self.cluster = cluster
        #: The instance of :class:`.Config`
        self.config = Config(self.app_id, self.cluster)
        #: The instance of :class:`.Switch`
        self.switch = Switch(self.app_id, self.cluster)
        #: The instance of :class:`.Service`
        self.service_consumer = Service(self.app_id, self.cluster)

    def setup_ioloop(self, url, token, soa_mode, soa_cluster,
                     cache_dir, cache_mode):
        if not os.path.exists(cache_dir):
            try:
                os.makedirs(cache_dir)
            except OSError as e:
                logger.warning("Failed to create dir({}): {}".format(
                    cache_dir, e))

        if not os.path.exists(cache_dir):
            raise RuntimeError("Cache dir {} doesn't exists".format(cache_dir))

        IOLoop.set_soa_mode_cluster(soa_mode, soa_cluster)
        if cache_mode == self.MODE_SINGLEPROCESS:
            from .ioloops.http import HuskarApiIOLoop
            IOLoop.configure(HuskarApiIOLoop)
        elif cache_mode == self.MODE_MULTIPROCESS:
            IOLoop.set_lockpath(os.path.join(cache_dir, 'huskar.writer'))
        else:
            raise ValueError('Unsupport cache mode: {}'.format(cache_mode))

        IOLoop(url, token, cache_dir).install()

    def register_ioloop_hook(self, key, func):
        IOLoop.current().add_listener(key, func)

    def start(self):
        IOLoop.current().run()

    def stop(self):
        ioloop = IOLoop.current()
        if ioloop is not None:
            ioloop.stop()
            IOLoop.clear_instance()

    @classmethod
    def _get_namespace_and_mode(cls):
        namepsace = os.environ.get(ENV_CACHE_DIR_NAMESPACE)
        contianer_id = os.environ.get(ENV_DOCKER_CONTAINER_ID)
        supervisor_group = os.environ.get(ENV_SUPERVISOR_GROUP_NAME)

        if contianer_id:
            namepsace = namepsace or contianer_id
            mode = cls.MODE_MULTIPROCESS
        elif supervisor_group:
            namepsace = namepsace or supervisor_group
            mode = cls.MODE_MULTIPROCESS
        else:
            namepsace = namepsace or 'default'
            mode = cls.MODE_SINGLEPROCESS

        return namepsace, mode
