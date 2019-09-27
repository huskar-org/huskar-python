from __future__ import absolute_import

import os
import sys
import signal
import logging


logger = logging.getLogger(__name__)


def init_loggers():
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(
        logging.Formatter('[HUSKAR %(levelname)-7s] %(message)s'))
    logger.addHandler(console)
    logger.setLevel(logging.INFO)


def launch_master_controller(
        huskar_options, ip, master_pid, instances=None,
        unregister_on_exit=False, service_checker=None, boot_wait_time=0,
        use_http=False):
    import gevent
    from .utils import setproctitle

    # Init Loggers
    init_loggers()

    # Wait 5 seconds for parent to fully boot.
    gevent.sleep(boot_wait_time)

    # Set process name
    name = "{}@{}".format(huskar_options['service'], huskar_options['cluster'])
    setproctitle(name)

    # Begin instances registration
    instances = instances or []
    instance_ids = []

    if use_http:
        from .http.service_registry import ServiceRegistry
        huskar = None
        registry = ServiceRegistry(**huskar_options)
    else:
        from .bootstrap import BootstrapHuskar
        huskar = BootstrapHuskar(**huskar_options)
        registry = huskar.service_registry

    for ins in instances:
        if service_checker is not None:
            try:
                is_healthy = service_checker(ip, ins)
            except Exception:
                logger.exception('Error on checking service instance')
                continue
            if not is_healthy:
                continue
        instance_id = registry.register_instance(
            ip,
            ins.get("port"),
            meta=ins.get("meta"),
            state=ins.get("state")
        )
        if instance_id is not None:
            logger.info("Service node %r registered to huskar", ins)
            instance_ids.append(instance_id)

    import gevent.event
    quiting_signal = gevent.event.Event()

    def exit(sig, frame):
        quiting_signal.set()

    signal.signal(signal.SIGTERM, exit)
    signal.signal(signal.SIGINT, exit)

    while True:
        if os.getppid() != master_pid or quiting_signal.is_set():
            break
        gevent.sleep(1)

    cleaned_up = gevent.event.Event()

    def on_quit():
        if unregister_on_exit:
            logger.info("Huskar daemon exiting")
            for instance_id in instance_ids:
                try:
                    if registry.unregister(instance_id):
                        logger.info("Unregister huskar instance %s success",
                                    instance_id)
                    else:
                        logger.warn("Unregister huskar instance %s failed",
                                    instance_id)
                except Exception as e:
                    logger.warn("Error unregistrying instance: %s %r",
                                instance_id, e)
            try:
                if huskar is not None:
                    huskar.stop()
            except Exception:
                logger.warn("Error to stop Huskar.", exc_info=True)
        cleaned_up.set()

    gevent.spawn(on_quit)
    cleaned_up.wait(3)
    return sys.exit()


class ServiceWatcher(object):
    def __init__(self, service_watcher_conf, gunicorn_master=None,
                 boot_wait_time=5):
        # validate service_watcher_conf
        for key in ('hosts', 'username', 'password',
                    'cluster', 'service_name'):
            if key not in service_watcher_conf:
                raise ValueError("%s is not in service register config" % key)

        self.config = service_watcher_conf
        self.boot_wait_time = boot_wait_time
        self.forked = False

    def check_service(self, ip, instance):
        """Check the service health during requesting service registry.

        Subclasses could override this method at will.

        :arg str ip: The IP address of service instance.
        :arg dict instance: The port, meta and state of service instance.
        :raises huskar_sdk_v2.exceptions.RegistryFailedException: registry failed.
        :returns: is this instance healthy or not.
        """  # noqa
        return True

    def post_fork(self):
        """Initialize post fork master controller"""

    def register_instances(self, instances=None):
        """
        set up a process for service register,it is called in the top(master)
        process. NOTE: it is especially  useful  for multi-process or
        multi-thread environment: if the huskar connection is shared between
        process or thread, strange things may happen.
        """
        import socket
        if self.forked is True:
            raise RuntimeError("Can only be called once")

        self.forked = True

        master_pid = os.getpid()
        use_http = self.config.get('use_http', False)
        if use_http:
            huskar_options = {
                'service': self.config["service_name"],
                'cluster': self.config["cluster"],
                'url': self.config['url'],
                'token': self.config['token'],
                'soa_mode': self.config.get('soa_mode'),
            }
        else:
            huskar_options = {
                "service": self.config["service_name"],
                "servers": self.config["hosts"],
                "username": self.config.get("username"),
                "password": self.config.get("password"),
                "cluster": self.config.get("cluster")
            }
        ip = socket.gethostbyname(socket.gethostname())
        # fork a process for registering instances to huskar
        pid = os.fork()

        if pid == 0:
            unregister_on_exit = self.config.get('unregister_on_exit', False)
            self.post_fork()
            launch_master_controller(
                huskar_options,
                ip=ip,
                master_pid=master_pid,
                instances=instances,
                unregister_on_exit=unregister_on_exit,
                use_http=use_http,
                service_checker=self.check_service,
                boot_wait_time=self.boot_wait_time,
            )
        else:
            self.pid = pid

    def transmit_signal(self, server, sig):
        """Called by gunicorn hook `on_signal`"""
        if sig in [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]:
            os.kill(self.pid, signal.SIGTERM)
