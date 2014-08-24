import time

from gevent import sleep
from gevent.queue import Queue
from . import StatReporter
from ..lib import loop


# Parameters: {"current block"'s key name,
#              current timestamp,
#              new key name for "current block" (something like unproc_block_{block_hash}}
solve_rotate_multichain = """
-- Get all the keys so we can find all the sharechains that contributed
local keys = redis.call('HKEYS', ARGV[1])
-- Set the end time of block solve. This also serves to guarentee the key is there...
redis.call('HSET', ARGV[1], 'solve_time', ARGV[2])
-- Rename to new home
redis.call('rename', ARGV[1], ARGV[3])
-- Initialize the new block key with a start time
redis.call('HSET', ARGV[1], 'start_time', ARGV[2])

-- Parse out and rotate all share chains. I'm sure this is terrible, no LUA skillz
local idx_map = {}
for key, val in pairs(keys) do
    local t = {}
    local i = 0
    for w in string.gmatch(val, "%w+") do
        t[i] = w
        i = i + 1
     end
     if t[0] == "chain" and t[2] == "shares" then
         local base = "chain_" .. t[1] .. "_slice"
         local idx = redis.call('incr', base .. "_index")
         redis.pcall('renamenx', base, base .. "_" .. idx)
         table.insert(idx_map, t[1] .. ":" .. idx)
     end
end
return idx_map
"""


class RedisReporter(StatReporter):
    one_sec_stats = ['queued']
    gl_methods = ['_queue_proc']
    defaults = StatReporter.defaults.copy()
    defaults.update(dict(redis={},
                         chain=1))

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
        self.solve_cmd = self.redis.register_script(solve_rotate_multichain)
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
        block_key = 'current_block_{}_{}'.format(currency, algo)
        new_block_key = "unproc_block_{}".format(hex_hash)

        chain_indexes_serial = self.solve_cmd(keys=[], args=[block_key, time.time(), new_block_key])
        chain_indexs = {}
        for chain in chain_indexes_serial:
            chain_id, last_index = chain.split(":")
            chain_indexs["chain_{}_solve_index".format(chain_id)] = last_index
        self.redis.hmset(new_block_key, dict(address=address,
                                             worker=worker,
                                             height=height,
                                             total_subsidy=total_subsidy,
                                             fees=fees,
                                             hex_bits=hex_bits,
                                             hash=hex_hash,
                                             currency=currency,
                                             algo=algo,
                                             **chain_indexs))

    def _queue_log_share(self, address, shares, algo, currency, merged=False):
        block_key = 'current_block_{}_{}'.format(currency, algo)
        chain_key = 'chain_{}_shares'.format(self.config['chain'])
        chain_slice = 'chain_{}_slice'.format(self.config['chain'])
        user_shares = '{}:{}'.format(address, shares)
        self.redis.hincrbyfloat(block_key, chain_key, shares)
        self.redis.rpush(chain_slice, user_shares)

    @loop()
    def _queue_proc(self):
        name, args, kwargs = self.queue.peek()
        self.logger.debug("Queue running {} with args '{}' kwargs '{}'"
                          .format(name, args, kwargs))
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
