import time

from gevent import sleep
from gevent.queue import Queue
from . import StatReporter
from ..lib import loop


class RedisReporter(StatReporter):
    one_sec_stats = ['queued']
    gl_methods = ['_queue_proc']
    defaults = StatReporter.defaults.copy()
    defaults.update(dict(redis={}))

    def __init__(self, config):
        self._configure(config)
        super(RedisReporter, self)._setup()
        # Import reporter type specific modules here as to not require them
        # for using powerpool with other reporters
        import redis
        # A list of exceptions that would indicate that retrying a queue item
        # COULD EVENTUALLY work (ie, bad connection because server
        # maintenince).  Errors that are likely to occur because of bad
        # coding/edge cases should be let through and data discarded after a
        # few attempts.
        self.queue_exceptions = (redis.exceptions.ConnectionError,
                                 redis.exceptions.InvalidResponse,
                                 redis.exceptions.TimeoutError,
                                 redis.exceptions.ConnectionError)
        self.redis = redis.Redis(**self.config['redis'])
        self.queue = Queue()

    @property
    def status(self):
        dct = dict(queue_size=self.queue.qsize())
        dct.update({key: self.manager[key].summary()
                    for key in self.one_min_stats + self.one_sec_stats})
        return dct

    def _queue_log_one_minute(self, address, worker, algo, stamp, typ, amount):
        # Include worker info if defined
        address += "." + worker
        self.redis.hincrbyfloat(
            "min_{}_{}_{}".format(self.share_type_strings[typ], algo, stamp),
            address, amount)

    def _queue_add_block(self, address, height, total_subsidy, fees, hex_bits,
                         hex_hash, currency, algo, merged=False, worker=None,
                         **kwargs):
        if merged:
            block_key = 'current_block_{}'.format(currency)
        else:
            block_key = 'current_block_{}'.format(algo)

        with self.redis.pipeline(transaction=False) as pipe:
            pipe.hmset(block_key, dict(solve_time=str(time.time()),
                                       address=address,
                                       height=height,
                                       total_subsidy=total_subsidy,
                                       fees=fees,
                                       hex_bits=hex_bits,
                                       hash=hash,
                                       algo=algo))
            pipe.rename(block_key, "unproc_block_{}".format(hash))
            pipe.hset(block_key, "start_time", str(time.time()))
            pipe.execute()

    def _queue_log_share(self, address, shares, algo, currency, merged=False):
        if merged:
            block_key = 'current_block_{}'.format(currency)
        else:
            block_key = 'current_block_{}'.format(algo)

        # Actual logging for payouts
        self.redis.hincrbyfloat(block_key, address, shares)

    @loop()
    def _queue_proc(self):
        name, args, kwargs = self.queue.peek()
        try:
            getattr(self, name)(*args, **kwargs)
        except self.queue_exceptions as e:
            self.logger.error("Unable to communicate with Redis! "
                              "{} Name: {}; Args: {}; Kwargs: {};"
                              .format(e, name, args, kwargs))
            sleep(1)
            return
        except Exception:
            # Log any unexpected problem, but don't retry because we might
            # end up endlessly retrying with same failure
            self.logger.error("Unkown error! Name: {}; Args: {}; Kwargs: {};"
                              .format(name, args, kwargs), exc_info=True)
        # By default we want to remove the item from the queue
        self.queue.get()

    def log_one_minute(self, *args, **kwargs):
        self.queue.put(("_queue_log_one_minute", args, kwargs))

    def log_share(self, client, diff, typ, params, job=None, header_hash=None, header=None):
        super(RedisReporter, self).log_share(
            client, diff, typ, params, job=job, header_hash=header_hash, header=header)

        for currency in job.merged_data:
            self.queue.put(("_queue_log_share", [], dict(address=client.address,
                                                         shares=diff,
                                                         algo=job.algo,
                                                         currency=currency,
                                                         merged=True)))
        self.queue.put(("_queue_log_share", [], dict(address=client.address,
                                                     shares=diff,
                                                     algo=job.algo,
                                                     currency=job.currency,
                                                     merged=False)))

    def add_block(self, *args, **kwargs):
        self.queue.put(("_queue_add_block", args, kwargs))
