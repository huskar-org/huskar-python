import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(process)d %(message)s")
logging.getLogger("huskar_sdk_v2").setLevel(logging.DEBUG)
