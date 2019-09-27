API References
==============

Huskar Client
-------------

There is a client based on ZooKeeper instead of Huskar API in the
``huskar_sdk_v2.bootstrap``. It is used by Huskar API itself. DO NOT USE IT and
choose ``huskar_sdk_v2.http`` always.

.. autoclass:: huskar_sdk_v2.http.HttpHuskar
    :members:

Utilities
---------

These module contains utilities for internal implementation.

CachedDict
**********

.. autoclass:: huskar_sdk_v2.utils.cached_dict.CachedDict
    :members:
    :undoc-members:

FileLock
********

.. autoclass:: huskar_sdk_v2.utils.filelock.FileLock
    :members:
    :undoc-members:

Internal Components
-------------------

Config
******

.. autoclass:: huskar_sdk_v2.http.components.config.Config
    :members:
    :inherited-members:

Switch
******

.. autoclass:: huskar_sdk_v2.http.components.switch.Switch
    :members:
    :inherited-members:

Service Instance
****************

.. autoclass:: huskar_sdk_v2.common.ServiceInstance
    :members:

Service Consumer
****************

.. autoclass:: huskar_sdk_v2.http.components.service.Service
    :members:
    :inherited-members:

Service Registry
****************

.. autoclass:: huskar_sdk_v2.http.service_registry.ServiceRegistry
    :members:
