import time

from gevent import Greenlet, sleep, spawn
from gevent.queue import Queue

from . import WorkerTracker, AddressTracker, Reporter
from ..utils import time_format
from ..stratum_server import StratumClient


class CeleryReporter(Reporter):
    one_min_stats = []
    one_sec_stats = ['queued']

    def _set_config(self, **config):
        self.config = dict(celery_task_prefix='simplecoin.tasks',
                           celery={'CELERY_DEFAULT_QUEUE': 'celery'},
                           report_pool_stats=True,
                           share_batch_interval=60,
                           tracker_expiry_time=180)
        self.config.update(config)

        # check that we have at least one configured coin server
        if not self.config['celery_task_prefix']:
            self.logger.error("You need to specify a celery prefix")
            exit()

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self.logger = server.register_logger('reporter')
        self._set_config(**config)

        # setup our celery agent and monkey patch
        from celery import Celery
        self.celery = Celery()
        self.celery.conf.update(self.config['celery'])

        self.share_reporter = None

        self.server = server
        self.server.register_stat_counters(self.one_min_stats, self.one_sec_stats)

        self.queue = Queue()
        self.addresses = {}
        self.workers = {}

    @property
    def status(self):
        dct = dict(queue_size=self.queue.qsize(),
                   addresses_count=len(self.addresses),
                   workers_count=len(self.workers))
        dct.update({key: self.server[key].summary()
                    for key in self.one_min_stats + self.one_sec_stats})
        return dct

    # Remote methods to send information to other servers
    ########################
    def add_one_minute(self, address, worker, stamp, typ, amount):
        self.server['queued'].incr()
        kwargs = {'user': address, 'worker': worker, 'minute': stamp,
                  'valid_shares': 0}
        if typ == StratumClient.VALID_SHARE:
            kwargs['valid_shares'] = amount
        if typ == StratumClient.DUP_SHARE:
            kwargs['dup_shares'] = amount
        if typ == StratumClient.LOW_DIFF:
            kwargs['low_diff_shares'] = amount
        if typ == StratumClient.STALE_SHARE:
            kwargs['stale_shares'] = amount
        self.queue.put(("add_one_minute", [], kwargs))
        self.logger.info("Calling celery task {} with {}"
                         .format("add_one_minute", kwargs))

    def add_share(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("add_share", args, kwargs))
        self.logger.info("Calling celery task {} with {}"
                         .format("add_shares", args))

    def agent_send(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("agent_receive", args, kwargs))

    def add_block(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("add_block", args, kwargs))
        self.logger.info("Calling celery task {} with {}"
                         .format("transmit_block", args))

    def _run(self):
        self.share_reporter = spawn(self.report_loop)
        while True:
            self._queue_proc()

    def _queue_proc(self):
            name, args, kwargs = self.queue.peek()
            try:
                self.celery.send_task(
                    self.config['celery_task_prefix'] + '.' + name, args, kwargs)
            except Exception as e:
                self.logger.error("Unable to communicate with celery broker! {}"
                                  .format(e))
            else:
                self.queue.get()

    def report_loop(self):
        """ Repeatedly do our share reporting on an interval """
        while True:
            sleep(self.config['share_batch_interval'])
            try:
                self._report_shares()
            except Exception:
                self.logger.error("Unhandled error in report shares", exc_info=True)

    def _report_shares(self, flush=False):
        """ Goes through our internal aggregated share data structures and
        reports them to our external storage. If asked to flush it will report
        all one minute shares, otherwise it will only report minutes that have
        passed. """
        if flush:
            self.logger.info("Flushing all aggreated share data...")

        self.logger.info("Reporting shares for {:,} users"
                         .format(len(self.addresses)))
        t = time.time()
        for address, tracker in self.addresses.items():
            tracker.report()
            # if the last log time was more than expiry time ago...
            if (tracker.last_log + self.config['tracker_expiry_time']) < t:
                assert tracker.unreported == 0
                del self.addresses[address]

        self.logger.info("Shares reported (queued) in {}"
                         .format(time_format(time.time() - t)))
        self.logger.info("Reporting one minute shares for {:,} address/workers"
                         .format(len(self.workers)))
        t = time.time()
        if flush:
            upper = t + 10
        else:
            upper = (t // 60) * 60
        for worker_addr, tracker in self.workers.items():
            tracker.report(upper)
            # if the last log time was more than expiry time ago...
            if (tracker.last_log + self.config['tracker_expiry_time']) < t:
                assert sum(tracker.slices.itervalues()) == 0
                del self.workers[worker_addr]
        self.logger.info("One minute shares reported (queued) in {}"
                         .format(time_format(time.time() - t)))

    def log_share(self, address, worker, amount, typ, job):
        """ Logs a share for a user and user/worker into all three share
        aggregate sources. """
        # log the share for the pool cache total as well
        if address != "pool" and self.config['report_pool_stats']:
            self.log_share("pool", '', amount, typ, job)

        # collecting for reporting to the website for display in graphs
        addr_worker = (address, worker)
        if addr_worker not in self.workers:
            self.workers[addr_worker] = WorkerTracker(self, address, worker)
        self.workers[(address, worker)].count_share(amount, typ)

        # reporting for payout share logging and vardiff rates
        if typ == StratumClient.VALID_SHARE and address != "pool":
            if address not in self.addresses:
                self.addresses[address] = AddressTracker(self, address)
            # for tracking vardiff speeds
            self.addresses[address].count_share(amount)

    def kill(self, *args, **kwargs):
        self.share_reporter.kill(*args, **kwargs)
        self._report_shares(flush=True)
        self.logger.info("Flushing the reporter task queue, {} items blocking "
                         "exit".format(self.queue.qsize()))
        while not self.queue.empty():
            self._queue_proc()
        self.logger.info("Shutting down CeleryReporter..")
        Greenlet.kill(self, *args, **kwargs)
