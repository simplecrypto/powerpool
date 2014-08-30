import time
import gevent

from gevent import spawn, GreenletExit
from hashlib import sha256
from binascii import hexlify

from ..stratum_server import StratumClient
from . import Reporter


class DoubleReporter(Reporter):
    defaults = dict(reporters=[])

    def __init__(self, config):
        self._configure(config)
        super(DoubleReporter, self).__init__()
        # Terrible, messy hack to get the child reporters to not log shares...
        self.child_reporters = []
        Reporter.log_share = lambda *args, **kwargs: None

    def start(self):
        Reporter.start(self)
        for rep in self.manager.component_types['Reporter']:
            if rep.config.get('name') in self.config['reporters']:
                self.child_reporters.append(rep)

    def log_share(self, client, diff, typ, params, job=None, header_hash=None,
                  header=None):
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
                ret = gl.value
                if ret:
                    spawn(self.add_block, **gl.value)
                else:
                    self.logger.error("Submission gl {} returned nothing!"
                                      .format(gl))

        for reporter in self.child_reporters:
            reporter.log_share(client, diff, typ, params, job=job,
                               header_hash=header_hash, header=header)

    def agent_send(self, *args, **kwargs):
        for reporter in self.child_reporters:
            reporter.agent_send(*args, **kwargs)

    def add_block(self, *args, **kwargs):
        for reporter in self.child_reporters:
            reporter.add_block(*args, **kwargs)
