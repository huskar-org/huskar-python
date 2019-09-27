from __future__ import absolute_import

import os
import json
import socket

from .consts import ENV_DOCKER_CONTAINER_ID


class ServiceInstance(object):
    """ServiceInstance represents a service instance.

    :arg name: service name
    :arg ip: ip address
    :arg port: a dict recording service ports.
               Note: main port muist be specified for healthy check
    :arg meta: a dict where you can add other info
    :arg state: up or down. when down, this service can not be used
                even it exists
    """
    def __init__(self, name, ip, port, meta=None, state='up'):
        self.name = name
        self.ip = ip
        self.port = port  # specify main port for healthy check
        self.meta = meta
        self.state = state  # up or down, default to started
        if not type(port) == dict:
            raise ValueError("port must be a dict")
        if not port.get("main", None):
            raise ValueError("main must be specified in port")
        if state not in ('up', 'down'):
            raise ValueError("state must be 'up' or 'down'")

    def to_hash(self):
        """Return a dict contains:

        - name
        - ip
        - port
        - meta
        - state
        """
        return {'name': self.name,
                'ip': self.ip,
                'port': self.port,
                'meta': self.meta,
                'state': self.state}

    def to_string(self):
        """Return the json representation of the result of :py:meth:`.to_hash`.
        """
        return json.dumps(self.to_hash())

    def mark_up(self):
        """set ``state`` as 'up'"""
        self.state = 'up'

    def mark_down(self):
        """set ``state`` as 'down'"""
        self.state = 'down'

    def is_ready(self):
        """indicate if ``state`` is 'up'"""
        return self.state == 'up'

    @property
    def fingerprint(self):
        """fingerprint of service, format: 'ip_mainPort',
        especially, return contianer id if in docker container.
        """
        container_id = os.environ.get(ENV_DOCKER_CONTAINER_ID)
        if container_id:
            return container_id
        main_port = self.port.get('main', None)
        return "{}_{}".format(self.ip, main_port)

    def port_open(self):
        """Test if the service's main port is open."""
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        try:
            port = self.port.get('main', None)
            if port:
                return False
            sk.connect((self.ip, port))
        except Exception:
            return False
        else:
            return True
        finally:
            sk.close()
