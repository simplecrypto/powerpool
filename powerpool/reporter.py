import logging
import time

from gevent import Greenlet, sleep, spawn
from gevent.queue import Queue
from celery import Celery

from .utils import time_format
from .stratum_server import StratumClient


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
                           share_batch_interval=60,
                           tracker_expiry_time=180)
        self.config.update(config)

        # check that we have at least one configured coin server
        if not self.config['celery_task_prefix']:
            logger.error("You need to specify a celery prefix")
            exit()

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self._set_config(**config)
        self.logger = server.register_logger('reporter')

        # setup our celery agent and monkey patch
        self.celery = Celery()
        self.celery.conf.update(self.config['celery'])

        self.share_reporter = None

        self.queue = Queue()
        self.addresses = {}
        self.workers = {}

    # Remote methods to send information to other servers
    ########################
    def add_one_minute(self, *args):
        self.queue.put(("add_one_minute", args, {}))
        self.logger.info("Calling celery task {} with {}"
                    .format("add_one_minute", args))

    def add_share(self, *args):
        self.queue.put(("add_share", args, {}))
        self.logger.info("Calling celery task {} with {}"
                    .format("add_shares", args))

    def agent_send(self, *args):
        self.queue.put(("agent_send", args, {}))
        self.logger.info("Calling celery task {} with {}"
                    .format("agent_send", args))

    def add_block(self, *args):
        self.queue.put(("add_block", args, {}))
        self.logger.info("Calling celery task {} with {}"
                    .format("transmit_block", args))

    def _run(self):
        self.share_reporter = spawn(self.report_shares)
        while True:
            name, args, kwargs = self.queue.peek()
            try:
                self.celery.send_task(
                    self.config['celery_task_prefix'] + '.' + name, args, kwargs)
            except Exception:
                self.logger.error("Unable to communicate with celery broker!")
            else:
                self.queue.get()

    def report_shares(self):
        while True:
            sleep(self.config['share_batch_interval'])
            self.logger.info("Reporting shares for {:,} users"
                        .format(len(self.addresses)))
            t = time.time()
            for address, tracker in self.addresses.iteritems():
                tracker.report()
                # if the last log time was more than expiry time ago...
                if (tracker.last_log + self.config['tracker_expiry_time']) < t:
                    del self.addresses[address]
            self.logger.info("Shares reported (queued) in {}"
                        .format(time_format(time.time() - t)))

            self.logger.info("Reporting one minute shares for {:,} address/workers"
                        .format(len(self.workers)))
            t = time.time()
            upper = (t // 60) * 60
            for worker_addr, tracker in self.workers.iteritems():
                tracker.report(upper=upper)
                # if the last log time was more than expiry time ago...
                if (tracker.last_log + self.config['tracker_expiry_time']) < t:
                    del self.workers[worker_addr]
            self.logger.info("One minute shares reported (queued) in {}"
                        .format(time_format(time.time() - t)))

    def log_share(self, address, worker, amount, typ):
        """ Logs a share for a user """
        # collecting for reporting to the website for display in graphs
        addr_worker = (address, worker)
        if addr_worker not in self.workers:
            self.workers[addr_worker] = WorkerTracker(self, address, worker)
        self.workers[(address, worker)].count_share(amount, typ)
        # reporting for payout share logging and vardiff rates
        if typ == StratumClient.VALID_SHARE:
            if address not in self.addresses:
                self.addresses[address] = AddressTracker(self, address)
            # for tracking vardiff speeds
            self.addresses[address].count_share(amount)

    def kill(self, *args, **kwargs):
        self.logger.info("Shutting down CeleryReporter..")
        self.share_reporter.kill(*args, **kwargs)
        Greenlet.kill(self, *args, **kwargs)


class WorkerTracker(object):
    """ Records stats about a worker and tracks all associated stratum
    connections. """
    def __init__(self, reporter, address, worker):
        self.reporter = reporter
        self.slices = {}
        self.address, self.worker = address, worker
        self.last_log = None

    def count_share(self, amount, typ):
        curr = time.time()
        t = (curr // 60) * 60
        self.slices.setdefault(t, [0, 0, 0, 0])
        self.slices[t][typ] += amount
        self.last_log = curr

    def report(self, flush=False, upper=None):
        # only report minutes that are complete unless we're flushing
        if not upper:  # allow precomputing upper for batch submission
            upper = (time.time() // 60) * 60
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
    def __init__(self, reporter, address):
        self.reporter = reporter
        self.unreported = 0
        self.minutes = {}
        self.address = address
        self.last_log = None

    def report(self):
        # Clear it before running a block call that might context switch...
        val = self.unreported
        self.unreported = 0
        self.reporter.add_share(self.address, val)

    def count_share(self, amount):
        curr = time.time()
        t = (int(curr) // 60) * 60
        self.minutes.setdefault(t, 0)
        self.minutes[t] += amount
        self.unreported += amount
        self.last_log = curr

    @property
    def spm(self):
        """ Called by the client code to determine how many shares per second
        are currently being submitted. Automatically cleans up the times older
        than 10 minutes. """
        ten_ago = ((time.time() // 60) * 60) - 600
        mins = 0
        total = 0
        for stamp in self.minutes.keys():
            if stamp < ten_ago:
                del self.minutes[stamp]
            else:
                total += self.minutes[stamp]
                mins += 1

        return total / mins
