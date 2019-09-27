from __future__ import absolute_import


class HuskarException(Exception):
    pass


class OperationFailedException(HuskarException):
    pass


class RegistryFailedException(HuskarException):
    pass


class HuskarDiscoveryException(HuskarException):
    def __init__(self, orig_exc, huskar_url, addition_msg=''):
        super(HuskarDiscoveryException, self).__init__(
            orig_exc, huskar_url, addition_msg)
        self.orig_exc = orig_exc
        self.huskar_url = huskar_url
        self.addition_msg = addition_msg


class HuskarDiscoveryUserError(HuskarDiscoveryException):
    pass


class HuskarDiscoveryServerError(HuskarDiscoveryException):
    pass
