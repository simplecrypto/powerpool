import time
import gevent

from gevent import spawn, GreenletExit
from hashlib import sha256
from binascii import hexlify

from ..lib import Component, loop
from ..utils import time_format
from ..stratum_server import StratumClient


class Reporter(Component):
    """ An abstract base class to document the Reporter interface. """
    one_min_stats = ['reject_low_share_n1', 'reject_dup_share_n1', 'reject_stale_share_n1',
                     'acc_share_n1', 'reject_low_share_count', 'reject_dup_share_count',
                     'reject_stale_share_count', 'acc_share_count']

    def agent_send(self, address, worker, typ, data, time):
        """ Called when valid data is recieved from a PPAgent connection. """
        raise NotImplementedError

    def add_block(self, address, height, total_subsidy, fees,
                  hex_bits, hash, merged, worker, algo):
        """ Called when a share is submitted with a hash that is valid for the
        network. """
        raise NotImplementedError

    def log_share(self, client, diff, typ, params, job=None, header_hash=None,
                  header=None):
        """ Logs a share to external sources for payout calculation and
        statistics """
        self.logger.debug("Running log share with args {} kwargs {}"
                          .format((client, diff, typ, params), dict(job=job,
                                  header_hash=header_hash, header=header)))

        # Log the share to our stat counters
        key = ""
        if typ > 0:
            key += "reject_"
        key += StratumClient.share_type_strings[typ] + "_share"
        self._incr(key + "_n1", diff)
        self._incr(key + "_count")

        if typ == StratumClient.VALID_SHARE:
            start = time.time()
            self.logger.debug("Valid share accepted from worker {}.{}!"
                              .format(client.address, client.worker))
            # Grab the raw coinbase out of the job object before gevent can preempt
            # to another thread and change the value. Very important!
            coinbase_raw = job.coinbase.raw

            # Some coins use POW function to do blockhash, while others use SHA256.
            # Allow toggling
            if job.pow_block_hash:
                header_hash_raw = client.algo(header)[::-1]
            else:
                header_hash_raw = sha256(sha256(header).digest()).digest()[::-1]
            hash_hex = hexlify(header_hash_raw)

            submission_threads = []
            # valid network hash?
            if header_hash <= job.bits_target:
                submission_threads.append(spawn(
                    job.found_block,
                    coinbase_raw,
                    client.address,
                    client.worker,
                    hash_hex,
                    header,
                    job,
                    start))

            # check each aux chain for validity
            for chain_id, data in job.merged_data.iteritems():
                if header_hash <= data['target']:
                    submission_threads.append(spawn(
                        data['found_block'],
                        client.address,
                        client.worker,
                        header,
                        coinbase_raw,
                        job,
                        start))

            for gl in gevent.iwait(submission_threads):
                spawn(self.add_block, **gl.value)


class StatReporter(Reporter):
    """ The stat reporter groups all shares into one minute chunks and reports
    them to allow separation of statistics reporting and payout related
    logging. """

    defaults = dict(report_pool_stats=True, pool_worker='', chain=1)
    gl_methods = ['_report_one_min']

    def _setup(self):
        self._minute_slices = {}
        self._per_address_slices = {}

    def log_one_minute(self, address, worker, algo, stamp, typ, amount):
        """ Called to log a minutes worth of shares that have been submitted
        by a unique (address, worker, algo). """
        raise NotImplementedError("If you're not logging the one minute chunks"
                                  "don't use the StatReporter!")

    def log_share(self, client, diff, typ, params, job=None, header_hash=None,
                  header=None):
        super(StatReporter, self).log_share(
            client, diff, typ, params, job=job, header_hash=header_hash, header=header)
        address, worker = client.address, client.worker
        algo = job.algo
        slc_time = (int(time.time()) // 60) * 60
        slc = self._minute_slices.setdefault(slc_time, {})
        # log the share under user "pool" to allow easy/fast display of pool stats
        if self.config['report_pool_stats']:
            self._aggr_one_min("pool", self.config['pool_worker'], algo, typ, diff, slc)
        self._aggr_one_min(address, worker, algo, typ, diff, slc)

        # reporting for vardiff rates
        if typ == StratumClient.VALID_SHARE:
            slc = self._per_address_slices.setdefault(slc_time, {})
            if address not in slc:
                slc[address] = diff
            else:
                slc[address] += diff

    def _aggr_one_min(self, address, worker, algo, typ, amount, slc):
        key = (address, worker, algo, typ)
        if key not in slc:
            slc[key] = amount
        else:
            slc[key] += amount

    @loop(interval=61, precise=60)
    def _report_one_min(self):
        try:
            self._process_minute_slices()
        except GreenletExit:
            self.logger.info("Flushing all aggreated one minute share data...")
            self._process_minute_slices(flush=True)

    def _process_minute_slices(self, flush=False):
        """ Goes through our internal aggregated share data structures and
        reports them to our external storage. If asked to flush it will report
        all one minute shares, otherwise it will only report minutes that have
        passed. """
        self.logger.info("Reporting one minute shares for address/workers")
        t = time.time()
        if not flush:
            upper = (int(t) // 60) * 60
        for stamp, data in self._minute_slices.items():
            if flush or stamp < upper:
                for (address, worker, algo, typ), amount in data.iteritems():
                    self.log_one_minute(address, worker, algo, stamp, typ, amount)
                    # XXX: GreenletExit getting raised here might cause some
                    # double reporting!
                del self._minute_slices[stamp]

        self.logger.info("One minute shares reported in {}"
                         .format(time_format(time.time() - t)))

        # Clean up old per address slices as well
        ten_ago = ((time.time() // 60) * 60) - 600
        for stamp in self._per_address_slices.keys():
            if stamp < ten_ago:
                del self._per_address_slices[stamp]

    def spm(self, address):
        """ Called by the client code to determine how many shares per second
        are currently being submitted. Automatically cleans up the times older
        than 10 minutes. """
        mins = 0
        total = 0
        for stamp in self._per_address_slices.keys():
            val = self._per_address_slices[stamp].get(address)
            if val is not None:
                total += val
                mins += 1

        return total / (mins or 1)  # or 1 prevents divison by zero error
