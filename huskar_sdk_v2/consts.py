from __future__ import absolute_import

import pkg_resources


# Huskar ZooKeeper Schema
OVERALL = 'overall'
BASE_PATH = '/huskar'
COMPONENT_PATH = '{subdomain}/{service}/{cluster}'


# Huskar API Schema
SOA_MODE_HEADER = 'X-SOA-Mode'
SOA_MODE_CHOICES = frozenset(['orig', 'prefix', 'route'])

SOA_CLUSTER_HEADER = 'X-Cluster-Name'


# Signal Names
#
# emit with delay if huskar launch failed
SIG_CLIENT_RESTART = "client_restart"


# Subdomain
CONFIG_SUBDOMAIN = 'config'
SWITCH_SUBDOMAIN = 'switch'
SERVICE_SUBDOMAIN = 'service'


# Cache
SERVICE_CACHE_FILENAME = 'cache__{service}__{cluster}__{key}.json'


class CACHE_KEYS(object):
    SWITCH = "switch"
    OVERALL_SWITCH = "overall_switch"

    CONFIG = "config"
    OVERALL_CONFIG = "overall_config"

    SERVICE = 'service'


# Distribution Information
USER_AGENT = u'{dist.project_name}/{dist.version}'.format(
    dist=pkg_resources.get_distribution('huskar_sdk_v2')
)


# environment variables
ENV_CACHE_DIR_NAMESPACE = 'HUSKAR_CACHE_DIR_NAMESPACE'
ENV_SUPERVISOR_GROUP_NAME = 'SUPERVISOR_GROUP_NAME'
ENV_DOCKER_CONTAINER_ID = 'MESOS_TASK_ID'
