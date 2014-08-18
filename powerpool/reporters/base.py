import time

from gevent import Greenlet


class Reporter(Greenlet):
    """ An example of main methods and argument patters... """
    def __init__(self, server):
        raise NotImplementedError

    def _run(self):
        raise NotImplementedError

    def add_one_minute(self, address, acc, stamp, worker, dup, low, stale):
        raise NotImplementedError

    def add_share(self, address, shares):
        raise NotImplementedError

    def agent_send(self, address, worker, typ, data, time):
        raise NotImplementedError

    def transmit_block(self, address, height, total_subsidy, fees,
                       hex_bits, hash, merged, worker):
        raise NotImplementedError


class WorkerTracker(object):
    """ Records stats about a worker and tracks all associated stratum
    connections. """
    def __init__(self, reporter, address, worker):
        self.reporter = reporter
        self.slices = {}
        self.address, self.worker = address, worker
        self.last_log = None

    def count_share(self, amount, typ):
        curr = int(time.time())
        t = (curr // 60) * 60
        self.slices.setdefault(t, [0, 0, 0, 0])
        self.slices[t][typ] += amount
        self.last_log = curr

    def report(self, upper):
        # only report minutes that are complete unless we're flushing
        for stamp in self.slices.keys():
            if stamp < upper:
                acc, dup, low, stale = self.slices[stamp]
                self.reporter.add_one_minute(self.address, acc, stamp,
                                             self.worker, dup, low, stale)
                del self.slices[stamp]


class AddressTracker(object):
    """ Records stats about an address and tracks all associated stratum
    connections. """
    def __init__(self, reporter, address):
        self.reporter = reporter
        self.unreported = 0
        self.minutes = {}
        self.address = address
        self.last_log = None

    def report(self):
        # Clear it before running a block call that might context switch...
        val = self.unreported
        self.unreported = 0
        if val != 0:
            self.reporter.add_share(self.address, val)

    def count_share(self, amount):
        curr = time.time()
        t = (int(curr) // 60) * 60
        self.minutes.setdefault(t, 0)
        self.minutes[t] += amount
        self.unreported += amount
        self.last_log = curr

    @property
    def status(self):
        spm = self.spm
        return dict(megahashrate=self.reporter.server.jobmanager.config['hashes_per_share'] * spm / 60.0 / 1000000,
                    spm=spm)

    @property
    def spm(self):
        """ Called by the client code to determine how many shares per second
        are currently being submitted. Automatically cleans up the times older
        than 10 minutes. """
        ten_ago = ((time.time() // 60) * 60) - 600
        mins = 0
        total = 0
        for stamp in self.minutes.keys():
            if stamp < ten_ago:
                del self.minutes[stamp]
            else:
                total += self.minutes[stamp]
                mins += 1

        return total / (mins or 1)  # or 1 prevents divison by zero error
