import time
import json

from . import QueueStatReporter
from ..stratum_server import StratumClient


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
         redis.pcall('HSET', ARGV[1], "chain_" .. t[1] .. "_start_index", "" .. idx)
         redis.pcall('renamenx', base, base .. "_" .. idx)
         table.insert(idx_map, t[1] .. ":" .. idx)
     end
end
return idx_map
"""


class RedisReporter(QueueStatReporter):
    one_sec_stats = ['queued']
    gl_methods = ['_queue_proc', '_report_one_min']
    defaults = QueueStatReporter.defaults.copy()
    defaults.update(dict(redis={}, chain=1))

    def __init__(self, config):
        self._configure(config)
        super(RedisReporter, self).__init__()
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

    @property
    def status(self):
        return dict(queue_size=self.queue.qsize())

    def _queue_log_one_minute(self, address, worker, algo, stamp, typ, amount):
        # Include worker info if defined
        address += "." + worker
        self.redis.hincrbyfloat(
            "min_{}_{}_{}".format(StratumClient.share_type_strings[typ], algo, stamp),
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
                                             merged=int(bool(merged)),
                                             **chain_indexs))

    def _queue_log_share(self, address, shares, algo, currency, merged=False):
        block_key = 'current_block_{}_{}'.format(currency, algo)
        chain_key = 'chain_{}_shares'.format(self.config['chain'])
        chain_slice = 'chain_{}_slice'.format(self.config['chain'])
        user_shares = '{}:{}'.format(address, shares)
        self.redis.hincrbyfloat(block_key, chain_key, shares)
        self.redis.rpush(chain_slice, user_shares)

    def log_share(self, client, diff, typ, params, job=None, header_hash=None, header=None,
                  **kwargs):
        super(RedisReporter, self).log_share(
            client, diff, typ, params, job=job, header_hash=header_hash,
            header=header, **kwargs)

        if typ != StratumClient.VALID_SHARE:
            return

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

    def _queue_agent_send(self, address, worker, typ, data, stamp):
        if typ == "hashrate" or typ == "temp":
            stamp = (stamp // 60) * 60
            for did, val in enumerate(data):
                self.redis.hset("{}_{}".format(typ, stamp),
                                "{}_{}_{}".format(address, worker, did),
                                val)
        elif typ == "status":
            # Set time so we know how fresh the data is
            data['time'] = time.time()
            # Remove the data in 1 day
            self.redis.setex("status_{}_{}".format(address, worker),
                             json.dumps(data), 86400)
        else:
            self.logger.warn("Recieved unsupported ppagent type {}"
                             .format(typ))

    def agent_send(self, *args, **kwargs):
        self.queue.put(("_queue_agent_send", args, kwargs))


#import redis
#redis = redis.Redis()
#solve_cmd = redis.register_script(solve_rotate_multichain)
#redis.hincrbyfloat("current_block_testing", "chain_1_shares", 12.5)
#print solve_cmd(keys=[], args=["current_block_testing", time.time(),
#                               "unproc_block_testing"])
#exit(0)
