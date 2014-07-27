import time

from gevent import sleep, Greenlet, spawn
from redis import Redis, RedisError
from gevent.queue import Queue
from powerpool.reporter import WorkerTracker, AddressTracker
from powerpool.stratum_server import StratumClient
from powerpool.utils import time_format


class RedisReporter(Greenlet):
    one_min_stats = []
    one_sec_stats = ['queued']

    def _set_config(self, **config):
        self.config = dict(report_pool_stats=True,
                           algo='',
                           share_batch_interval=60,
                           tracker_expiry_time=180)
        self.config.update(config)
        self.config['current_block'] = "current_block_{}".format(self.config['algo'])
        if not self.config['algo']:
            self.logger.error("must define an algorithm!")
            exit(1)

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self.logger = server.register_logger('reporter')
        self._set_config(**config)

        # setup our celery agent and monkey patch
        self.redis = Redis(**self.config['redis'])

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
    def add_one_minute(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("add_one_minute", args, kwargs))
        self.logger.info("Calling celery task {} with {}"
                         .format("add_one_minute", args))

    def add_block(self, *args, **kwargs):
        self.server['queued'].incr()
        self.queue.put(("add_block", args, kwargs))
        self.logger.info("Calling celery task {} with {}"
                         .format("transmit_block", args))

    def _run(self):
        self.one_min_reporter = spawn(self.one_min_loop)
        while True:
            self._queue_proc()

    def _queue_proc(self):
            name, args, kwargs = self.queue.peek()
            try:
                if name == "add_one_minute":
                    address, acc, stamp, worker, dup, low, stale = args
                    with self.redis.pipeline(transaction=False) as pipe:
                        # bleh, messy
                        key = "{}.{}".format(address, worker)
                        if acc:
                            pipe.hincrbyfloat("min_acc_{}_{}".format(self.config['algo'], stamp), key, acc)
                        if dup:
                            pipe.hincrbyfloat("min_dup_{}_{}".format(self.config['algo'], stamp), key, dup)
                        if low:
                            pipe.hincrbyfloat("min_low_{}_{}".format(self.config['algo'], stamp), key, low)
                        if stale:
                            pipe.hincrbyfloat("min_stale_{}_{}".format(self.config['algo'], stamp), key, stale)
                        pipe.execute()
                elif name == "add_block":
                    address, height, total_subsidy, fees, hex_bits, hash = args
                    if 'merged' in kwargs:
                        block_key = 'current_block_{}'.format(kwargs['merged'])
                    else:
                        block_key = self.config['current_block']
                    with self.redis.pipeline(transaction=False) as pipe:
                        pipe.hmset(block_key,
                                   dict(solve_time=str(time.time()),
                                        address=address, height=height,
                                        total_subsidy=total_subsidy, fees=fees,
                                        hex_bits=hex_bits, hash=hash,
                                        algo=self.config['algo'],
                                        **kwargs))
                        pipe.rename(block_key, "unproc_block_{}".format(hash))
                        pipe.hset(block_key, "start_time", str(time.time()))
                        pipe.execute()
                else:
                    self.logger.error("Invalid queue item added!")
            except RedisError as e:
                self.logger.error("Unable to communicate with Redis! {} Name: {}; Args: {}; Kwargs: {};".format(e, name, args, kwargs))
                sleep(1)
            except Exception:
                self.logger.error("Unkown error! Name: {}; Args: {}; Kwargs: {};"
                                  .format(name, args, kwargs), exc_info=True)
                self.queue.get()
            else:
                self.queue.get()

    def one_min_loop(self):
        """ Repeatedly do our share reporting on an interval """
        while True:
            now = time.time()
            target = ((now // 60) * 60) + 61
            sleep(target - now)
            try:
                self._report_one_min()
            except Exception:
                self.logger.error("Unhandled error in report one mine", exc_info=True)

    def _report_one_min(self, flush=False):
        """ Goes through our internal aggregated share data structures and
        reports them to our external storage. If asked to flush it will report
        all one minute shares, otherwise it will only report minutes that have
        passed. """
        if flush:
            self.logger.info("Flushing all aggreated share data...")

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
        if (typ == StratumClient.VALID_SHARE or typ == StratumClient.BLOCK_FOUND) and address != "pool":
            if address not in self.addresses:
                self.addresses[address] = RedisAddressTracker(self, address)
            # for tracking vardiff speeds
            self.addresses[address].count_share(amount)
            with self.redis.pipeline(transaction=False) as pipe:
                for currency in job.merged_data:
                    pipe.hincrbyfloat('current_block_' + currency, address, amount)
                pipe.hincrbyfloat(self.config['current_block'], address, amount)
                pipe.execute()

    def kill(self, *args, **kwargs):
        self.share_reporter.kill(*args, **kwargs)
        self._report_one_min(flush=True)
        self.logger.info("Flushing the reporter task queue, {} items blocking "
                         "exit".format(self.queue.qsize()))
        while not self.queue.empty():
            self._queue_proc()
        self.logger.info("Shutting down CeleryReporter..")
        Greenlet.kill(self, *args, **kwargs)


class RedisAddressTracker(AddressTracker):
    """ Records stats about an address and tracks all associated stratum
    connections. """
    def count_share(self, amount):
        curr = time.time()
        t = (int(curr) // 60) * 60
        self.minutes.setdefault(t, 0)
        self.minutes[t] += amount
        self.last_log = curr
