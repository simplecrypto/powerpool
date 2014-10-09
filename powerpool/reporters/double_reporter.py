import time
import gevent

from gevent import spawn
from hashlib import sha256
from binascii import hexlify

from ..stratum_server import StratumClient
from ..lib import loop
from ..exceptions import ConfigurationError
from . import Reporter


class DoubleReporter(Reporter):
    defaults = dict(reporters=[])
    gl_methods = ['_process_minute_slices']

    def __init__(self, config):
        self._configure(config)
        super(DoubleReporter, self).__init__()
        # Terrible, messy hack to get the child reporters to not log shares...
        self.child_reporters = []
        self._per_address_slices = {}
        Reporter.log_share = lambda *args, **kwargs: None

    def start(self):
        Reporter.start(self)
        for key in self.config['reporters']:
            if key in self.manager.components:
                self.child_reporters.append(self.manager.components[key])
            else:
                raise ConfigurationError("Couldn't find {}".format(key))

        if not self.child_reporters:
            raise ConfigurationError("Must have at least one reporter!")

    def log_share(self, client, diff, typ, params, job=None, header_hash=None,
                  header=None, **kwargs):
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
                header_hash_raw = client.algo['module'](header)[::-1]
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
                ret = gl.value
                if ret:
                    spawn(self.add_block, **ret)
                else:
                    self.logger.error("Submission gl {} returned nothing!"
                                      .format(gl))

        for reporter in self.child_reporters:
            reporter.log_share(client, diff, typ, params, job=job,
                               header_hash=header_hash, header=header, **kwargs)

        # reporting for vardiff rates
        slc_time = (int(time.time()) // 60) * 60
        address = client.address
        if typ == StratumClient.VALID_SHARE:
            slc = self._per_address_slices.setdefault(slc_time, {})
            if address not in slc:
                slc[address] = diff
            else:
                slc[address] += diff

    @loop(interval=61)
    def _process_minute_slices(self):
        # Clean up old per address slices as well
        self.logger.info("Cleaning up old vardiff trackers")
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

    def agent_send(self, *args, **kwargs):
        for reporter in self.child_reporters:
            reporter.agent_send(*args, **kwargs)

    def add_block(self, *args, **kwargs):
        for reporter in self.child_reporters:
            reporter.add_block(*args, **kwargs)
