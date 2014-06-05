import logging
import time

from gevent import Greenlet, sleep
from gevent.queue import Queue
from celery import Celery

from .utils import time_format
from .stratum_server import StratumClient


logger = logging.getLogger('reporter')


class NoopReporter(Greenlet):
    """ An example of main methods and argument patters... """
    def __init__(self, server):
        raise NotImplementedError

    def _run(self):
        raise NotImplementedError

    def add_one_minute(self, address, acc, stamp, worker, dup, low, stale):
        raise NotImplementedError

    def add_share(self, address, shares):
        raise NotImplementedError

    def agent_send(self, address, worker, typ, data, time):
        raise NotImplementedError

    def transmit_block(self, address, worker, height, total_subsidy, fees,
                       hex_bits, hash, merged):
        raise NotImplementedError


class CeleryReporter(Greenlet):
    def _set_config(self, **config):
        self.config = dict(celery_task_prefix=None,
                           celery={'CELERY_DEFAULT_QUEUE': 'celery'},
                           share_batch_interval=60)
        self.config.update(config)

        # check that we have at least one configured coin server
        if not self.config['celery_task_prefix']:
            logger.error("You need to specify a celery prefix")
            exit()

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self._set_config(**config)

        # setup our celery agent and monkey patch
        self.celery = Celery()
        self.celery.conf.update(self.config['celery'])

        self.queue = Queue()

    # Remote methods to send information to other servers
    ########################
    def add_one_minute(self, *args):
        self.queue.put(("add_one_minute", args, {}))
        logger.info("Calling celery task {} with {}"
                    .format("add_one_minute", args))

    def add_share(self, *args):
        self.queue.put(("add_share", args, {}))
        logger.info("Calling celery task {} with {}"
                    .format("add_shares", args))

    def agent_send(self, *args):
        self.queue.put(("agent_send", args, {}))
        logger.info("Calling celery task {} with {}"
                    .format("agent_send", args))

    def add_block(self, *args):
        self.queue.put(("add_block", args, {}))
        logger.info("Calling celery task {} with {}"
                    .format("transmit_block", args))

    def _run(self):
        self.report_shares()
        while True:
            name, args, kwargs = self.queue.peek()
            try:
                self.celery.send_task(
                    self.config['celery_task_prefix'] + '.' + name, args, kwargs)
            except Exception:
                logger.error("Unable to communicate with celery broker!")
            else:
                self.queue.get()

    def report_shares(self):
        while True:
            sleep(self.config['share_batch_interval'])
            logger.info("Reporting shares for {:,} users"
                        .format(len(self.address_lut)))
            t = time.time()
            for tracker in self.address_lut.itervalues():
                tracker.report()
            logger.info("Shares reported (queued) in {}"
                        .format(time_format(time.time() - t)))

            logger.info("Reporting one minute shares for {:,} address/workers"
                        .format(len(self.addr_worker_lut)))
            t = time.time()
            upper = (t // 60) * 60
            for tracker in self.addr_worker_lut.itervalues():
                tracker.report(upper=upper)
            logger.info("One minute shares reported (queued) in {}"
                        .format(time_format(time.time() - t)))

    def log_share(self, address, worker, amount, typ):
        """ Logs a share for a user """
        # collecting for reporting to the website for display in graphs
        self.addr_worker_lut[(address, worker)].count_share(amount, typ)
        # reporting for payout share logging and vardiff rates
        if typ == StratumClient.VALID_SHARE:
            # for tracking vardiff speeds
            self.address_lut[address].count_share(amount)

    def kill(self, *args, **kwargs):
        logger.info("Shutting down CeleryReporter..")
        Greenlet.kill(self, *args, **kwargs)


class WorkerTracker(object):
    """ Records stats about a worker and tracks all associated stratum
    connections. """
    def __init__(self, reporter, client):
        self.reporter = reporter
        self.slices = {}
        self.clients = []
        self.address, self.worker = client.address, client.worker

    def count_share(self, amount, typ):
        t = (time() // 60) * 60
        self.slices.setdefault(t, [0, 0, 0, 0])
        self.slices[t][typ] += amount

    def report(self, flush=False, upper=None):
        # only report minutes that are complete unless we're flushing
        if not upper:  # allow precomputing upper for batch submission
            upper = (time() // 60) * 60
        if flush:
            upper += 120
        for stamp in self.slices.keys():
            if stamp < upper:
                acc, dup, low, stale = self.slices[stamp]
                self.reporter.add_one_minute(self.address, acc, stamp,
                                             self.worker, dup, low, stale)
                del self.slices[stamp]


class AddressTracker(object):
    """ Records stats about an address and tracks all associated stratum
    connections. """
    def __init__(self, reporter, client):
        self.reporter = reporter
        self.unreported = 0
        self.minutes = {}
        self.clients = []
        self.address = client.address

    def report(self):
        # Clear it before running a block call that might context switch...
        val = self.unreported
        self.unreported = 0
        self.reporter.add_share(self.address, val)

    def count_share(self, amount):
        t = (int(time()) // 60) * 60
        self.minutes.setdefault(t, 0)
        self.minutes[t] += amount
        self.unreported += amount

    @property
    def spm(self):
        """ Called by the client code to determine how many shares per second
        are currently being submitted. Automatically cleans up the times older
        than 10 minutes. """
        ten_ago = ((time() // 60) * 60) - 600
        mins = 0
        total = 0
        for stamp in self.minutes.keys():
            if stamp < ten_ago:
                del self.minutes[stamp]
            else:
                total += self.minutes[stamp]
                mins += 1

        return total / mins
