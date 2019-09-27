# -*- coding: utf-8 -*-

import logging

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from huskar_sdk_v2.consts import (
    USER_AGENT,
    SOA_MODE_HEADER,
    SOA_MODE_CHOICES,
    SOA_CLUSTER_HEADER,
)
from huskar_sdk_v2.utils import join_url
from huskar_sdk_v2.common import ServiceInstance


logger = logging.getLogger(__name__)


class ServiceRegistry(object):
    PATH = '/api/service/{service}/{cluster}'

    def __init__(self, service, cluster, url, token, retry_times=float('inf'),
                 soa_mode=None):
        assert soa_mode is None or soa_mode in SOA_MODE_CHOICES

        self.service = service
        self.url = join_url(url, self.PATH.format(
            service=service, cluster=cluster))

        self.session = requests.Session()
        self.session.headers['Authorization'] = token
        self.session.headers['User-Agent'] = ' '.join([
            USER_AGENT, self.session.headers.get('User-Agent', '')
        ])
        if soa_mode is not None:
            self.session.headers[SOA_MODE_HEADER] = soa_mode
            self.session.headers[SOA_CLUSTER_HEADER] = cluster

        # Requests Retry
        # http://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
        retries = Retry(total=retry_times, backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount(url, HTTPAdapter(max_retries=retries))

    def register(self, service_instance):
        instance_id = service_instance.fingerprint
        data = {
            'key': instance_id,
            'value': service_instance.to_string(),
        }
        try:
            resp = self.session.post(self.url, data=data)
            if resp.ok:
                return instance_id
            resp_data = resp.json()
            logger.error('failed to register service, %d %s: %s',
                         resp.status_code, resp_data['status'],
                         resp_data['message'])
        except Exception:
            logger.error('unexpected error when register service:',
                         exc_info=True)
        finally:
            self.session.close()

    def register_instance(self, ip, port, meta=None, state='up'):
        service_instance = ServiceInstance(self.service, ip, port, meta, state)
        return self.register(service_instance)

    def unregister(self, instance_id, ignore=False):
        data = {'key': instance_id}
        try:
            self.session.delete(self.url, data=data)
            return True
        except Exception:
            logger.error('unexpected error when unregister service:',
                         exc_info=True)
        finally:
            self.session.close()
