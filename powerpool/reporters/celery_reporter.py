from gevent.queue import Queue

from . import StatReporter
from ..lib import loop
from ..stratum_server import StratumClient


class CeleryReporter(StatReporter):
    """ A legacy wrapper around old log reporting system to allow testing
    PowerPool 0.6 with SimpleCoin 0.7 """
    one_sec_stats = ['queued']
    gl_methods = ['_queue_proc', '_report_one_min', '_report_payout_share_aggrs']
    defaults = StatReporter.defaults.copy()
    defaults.update(dict(celery_task_prefix='simplecoin.tasks',
                         celery={'CELERY_DEFAULT_QUEUE': 'celery'},
                         share_batch_interval=60))

    def __init__(self, config):
        self._configure(config)
        super(CeleryReporter, self).__init__()

        # setup our celery agent and monkey patch
        from celery import Celery
        self.celery = Celery()
        self.celery.conf.update(self.config['celery'])

        self.queue = Queue()
        self._aggr_shares = {}

    @property
    def status(self):
        dct = dict(queue_size=self.queue.qsize(),
                   unrep_shares=len(self._aggr_shares))
        dct.update({key: counter.summary() for key, counter in self.counters})
        return dct

    def log_one_minute(self, address, worker, algo, stamp, typ, amount):
        self._incr('queued')
        kwargs = {'user': address, 'worker': worker, 'minute': stamp,
                  'valid_shares': 0}
        if typ == StratumClient.VALID_SHARE:
            kwargs['valid_shares'] = amount
        if typ == StratumClient.DUP_SHARE:
            kwargs['dup_shares'] = amount
        if typ == StratumClient.LOW_DIFF_SHARE:
            kwargs['low_diff_shares'] = amount
        if typ == StratumClient.STALE_SHARE:
            kwargs['stale_shares'] = amount
        self.queue.put(("add_one_minute", [], kwargs))

    def log_share(self, client, diff, typ, params, job=None, header_hash=None,
                  header=None):
        super(CeleryReporter, self).log_share(
            client, diff, typ, params, job=job, header_hash=header_hash, header=header)

        # Aggregate valid shares to be reported in batches. SimpleCoin's Celery
        # worker can't really handle high load share logging with the way it's
        # built
        address = client.address
        if typ == StratumClient.VALID_SHARE:
            if address not in self._aggr_shares:
                self._aggr_shares[address] = diff
            else:
                self._aggr_shares[address] += diff

    def agent_send(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("agent_receive", args, kwargs))

    def add_block(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("add_block", args, kwargs))

    @loop()
    def _queue_proc(self):
            name, args, kwargs = self.queue.peek()
            try:
                self.logger.info("Calling celery task {} with args: {}, kwargs: {}"
                                 .format(name, args, kwargs))
                self.celery.send_task(
                    self.config['celery_task_prefix'] + '.' + name, args, kwargs)
            except Exception as e:
                self.logger.error("Unable to communicate with celery broker! {}"
                                  .format(e))
            else:
                self.queue.get()

    @loop(interval='share_batch_interval', precise=True, fin='_report_payout_shares')
    def _report_payout_share_aggrs(self):
        self._report_payout_shares()

    def _report_payout_shares(self, exit_exc=None, caller=None):
        """ Goes through our internal aggregated share data and adds a celery
        task for each unque address. """
        self.logger.info("Reporting shares for {:,} users"
                         .format(len(self._aggr_shares)))
        for address, shares in self._aggr_shares.items():
            self.queue.put(("add_share", [address, shares], {}))
            del self._aggr_shares[address]
