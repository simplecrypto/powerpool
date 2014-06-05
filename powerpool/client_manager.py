import logging

from time import time
from gevent import sleep, Greenlet

from .stratum_server import StratumClient
from .utils import time_format


logger = logging.getLogger('client_manager')


class ClientManager(Greenlet):
    """ This class does housekeeping for client lookup mappings and share
    repoting and scheduling. We collect all shares into three different grousp:

        1. Slices for reporting all accepted and rejected shares in one minute
           chuncks per worker
        2. Slices for tracking an addresses shares/second speed
        3. A running counter that gets emptied every time we report shares
           for payout purposes

    They're grouped outside of client data stratuctures to limit reporting
    overhead for multiple workers connected with the same address/worker name.
    """

    def __init__(self, server):
        Greenlet.__init__(self)
        self.server = server
        self.reporter = server.reporter
        self.config = server.config

        # lookup tables for finding trackers
        self.clients = {}
        self.addr_worker_lut = {}
        self.address_lut = {}

    def _run(self):
        self.report_shares()

    def set_user(self, client):
        # Add the client (or create) appropriate worker and address trackers
        user_worker = (client.address, client.worker)
        self.addr_worker_lut.setdefault(
            user_worker, WorkerTracker(self.reporter, client))
        self.addr_worker_lut[user_worker].clients.append(client)

        self.address_lut.setdefault(
            user_worker[0], AddressTracker(self.reporter, client))
        self.address_lut[user_worker[0]].clients.append(client)

    def __delitem__(self, key):
        """ Manages removing the client from the luts on regular delete """
        obj = self[key]
        del self.clients[key]
        address, worker = obj.address, obj.worker

        # it won't appear in the luts if these values were never set
        if address is None and worker is None:
            return

        # wipe the client from the address tracker
        if address in self.address_lut:
            # remove from lut for address
            self.address_lut[address].clients.remove(obj)
            # if it's the last client in the object, delete the entry
            if not len(self.address_lut[address].clients):
                self.address_lut[address].report()
                del self.address_lut[address]

        # wipe the client from the address/worker tracker
        key = (address, worker)
        if key in self.addr_worker_lut:
            self.addr_worker_lut[key].clients.remove(obj)
            # if it's the last client in the object, delete the entry
            if not len(self.addr_worker_lut[key].clients):
                self.addr_worker_lut[key].report(flush=True)
                del self.addr_worker_lut[key]

    def __getitem__(self, key):
        return self.clients[key]

    def __setitem__(self, key, val):
        self.clients[key] = val

    def log_share(self, address, worker, amount, typ):
        """ Logs a share for a user """
        # collecting for reporting to the website for display in graphs
        self.addr_worker_lut[(address, worker)].count_share(amount, typ)
        # reporting for payout share logging and vardiff rates
        if typ == StratumClient.VALID_SHARE:
            # for tracking vardiff speeds
            self.address_lut[address].count_share(amount)

    def report_shares(self):
        while True:
            sleep(self.config['share_batch_interval'])
            logger.info("Reporting shares for {:,} users"
                        .format(len(self.address_lut)))
            t = time()
            for tracker in self.address_lut.itervalues():
                tracker.report()
            logger.info("Shares reported (queued) in {}"
                        .format(time_format(time() - t)))

            logger.info("Reporting one minute shares for {:,} address/workers"
                        .format(len(self.addr_worker_lut)))
            t = time()
            upper = (time() // 60) * 60
            for tracker in self.addr_worker_lut.itervalues():
                tracker.report(upper=upper)
            logger.info("One minute shares reported (queued) in {}"
                        .format(time_format(time() - t)))


class WorkerTracker(object):
    """ Records stats about a worker and tracks all associated stratum
    connections. """
    def __init__(self, reporter, client):
        self.reporter = reporter
        self.slices = {}
        self.clients = []
        self.address, self.worker = client.address, client.worker

    def count_share(self, amount, typ):
        t = (time() // 60) * 60
        self.slices.setdefault(t, [0, 0, 0, 0])
        self.slices[t][typ] += amount

    def report(self, flush=False, upper=None):
        # only report minutes that are complete unless we're flushing
        if not upper:  # allow precomputing upper for batch submission
            upper = (time() // 60) * 60
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
    def __init__(self, reporter, client):
        self.reporter = reporter
        self.unreported = 0
        self.minutes = {}
        self.clients = []
        self.address = client.address

    def report(self):
        # Clear it before running a block call that might context switch...
        val = self.unreported
        self.unreported = 0
        self.reporter.add_share(self.address, val)

    def count_share(self, amount):
        t = (int(time()) // 60) * 60
        self.minutes.setdefault(t, 0)
        self.minutes[t] += amount
        self.unreported += amount

    @property
    def spm(self):
        """ Called by the client code to determine how many shares per second
        are currently being submitted. Automatically cleans up the times older
        than 10 minutes. """
        ten_ago = ((time() // 60) * 60) - 600
        mins = 0
        total = 0
        for stamp in self.minutes.keys():
            if stamp < ten_ago:
                del self.minutes[stamp]
            else:
                total += self.minutes[stamp]
                mins += 1

        return total / mins
