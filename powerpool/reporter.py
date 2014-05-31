import logging

from gevent import Greenlet
from gevent.queue import Queue


logger = logging.getLogger('reporter')


class NoopReporter(Greenlet):
    """ An example of main methods and argument patters... """
    def __init__(self, server):
        pass

    def _run(self):
        pass

    def add_one_minute(self, address, acc, stamp, worker, dup, low, stale):
        pass

    def add_shares(self, address, shares):
        pass

    def agent_send(self, address, worker, typ, data, time):
        pass

    def transmit_block(self, address, worker, height, total_subsidy, fees,
                       hex_bits, hash, merged):
        pass


class CeleryReporter(Greenlet):
    def __init__(self, server):
        self.celery = server.celery
        self.config = server.config
        self.queue = Queue()

        # dynamically make some boilerplate functions...
        for attr in ['add_one_minute', 'add_shares', 'agent_send', 'transmit_block']:
            def closure(self, *args, **kwargs):
                self.queue.put((attr, args, kwargs))
            setattr(self, attr, closure)

    def _run(self):
        while True:
            name, args, kwargs = self.queue.peek()
            try:
                self.celery.send_task(
                    self.config['celery_task_prefix'] + '.' + name, args, kwargs)
            except Exception:
                logger.error("Unable to communicate with celery broker!")
            else:
                self.queue.get()
