from __future__ import absolute_import

try:
    from kazoo.exceptions import NoAuthError, NodeExistsError, NoNodeError
except ImportError:
    class FakeKazooException(Exception):
        pass
    NoAuthError = NodeExistsError = NoNodeError = FakeKazooException

from huskar_sdk_v2.common import ServiceInstance
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN
from huskar_sdk_v2.utils import combine
from ..client import logger
from . import BaseComponent


class ServiceRegistry(BaseComponent):
    """ ServiceRegistry is used to register service instances"""
    SUBDOMAIN = SERVICE_SUBDOMAIN

    def _get_instance_path(self, instance_id):
        return combine(self.base_path, str(instance_id))

    def build_instance(self, ip, port, meta=None, state='up'):
        """Create a instance of current service.

        Return an instance of :py:class:`.ServiceInstance`.
        """
        return ServiceInstance(self.service, ip, port, meta, state=state)

    def exists(self, service_instance):
        """Returns ``True`` if specified service instance is already registered.
        """
        instance_id = service_instance.fingerprint
        path = self._get_instance_path(instance_id)
        return self.client.exists(path)

    def register(self, service_instance, retry_times=float("inf")):
        """Register service instance to Huskar.

        :arg service_instance: an instance of :py:class:`ServiceInstance`
                               indicates the service to be registered.
        """
        try:
            instance_id = service_instance.fingerprint
            path = self._get_instance_path(instance_id)
            if self.exists(service_instance):
                self.client.set_data(path, service_instance.to_string())
            else:
                self.client.create(path,
                                   value=service_instance.to_string(),
                                   ephemeral=False,
                                   makepath=True,
                                   retry_times=retry_times,
                                   interval=2)
            return instance_id
        except NoAuthError:
            logger.error("NoAuthError in service register: "
                         "don't have auth on %s" % path)
        except NodeExistsError:
            logger.error("NodeExistsError in service register: "
                         "%s has existed" % path)

    def register_instance(self, *args, **kwargs):
        instance = self.build_instance(*args, **kwargs)
        return self.register(instance)

    def unregister(self, instance_id, ignore=False):
        """Unregister a service by its id.

        :arg str instance_id: :py:attr:`~ServiceInstance.fingerprint` of the
                              service instance.
        :arg bool ingore: do not log error if service not found.
        """
        try:
            path = self._get_instance_path(instance_id)
            return self.client.delete(path)
        except NoAuthError:
            logger.error("NoAuthError in service unregister: "
                         "don't have auth on %s" % path)
        except NoNodeError:
            if not ignore:
                logger.error("NoNodeError in service unregister: "
                             "%s doesn't exist" % path)

    def update_instance(self, old_instance_id, service_instance):
        new_instance_id = service_instance.fingerprint
        if new_instance_id == old_instance_id:
            path = self._get_instance_path(new_instance_id)
            self.client.set_data(path, service_instance.to_string())
        else:
            try:
                self.unregister(old_instance_id)
            except NoNodeError:
                pass
            self.register(service_instance)
        return new_instance_id
