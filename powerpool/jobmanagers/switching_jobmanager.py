import redis
import simplejson
import time
import operator

from cryptokit import bits_to_difficulty
from gevent.event import Event
from powerpool.lib import loop
from powerpool.jobmanagers import Jobmanager
from binascii import hexlify


class MonitorNetworkMulti(Jobmanager):
    defaults = config = dict(jobmanagers=None,
                             profit_poll_int=1,
                             redis={},
                             margin_switch=1.2,
                             exchange_manager={})

    def __init__(self, config):
        self._configure(config)

        # Since some MonitorNetwork objs are polling and some aren't....
        self.gl_methods = ['update_profit']

        # Child jobmanagers
        self.jobmanagers = {}
        self.price_data = {}
        self.profit_data = {}
        self.next_network = None
        self.current_network = None

        # Currently active jobs keyed by their unique ID
        self.jobs = {}
        self.new_job = Event()

        self.redis = redis.Redis(**self.config['redis'])

    @property
    def latest_job(self):
        """ Proxy the jobmanager we're currently mining ons job """
        return self.jobmanagers[self.current_network].latest_job

    @property
    def status(self):
        """ For display in the http monitor """
        return dict(price_data=self.price_data,
                    profit_data=self.profit_data,
                    next_network=self.next_network,
                    current_network=self.current_network)

    @loop(interval='profit_poll_int')
    def update_profit(self):
        """ Continually check redis for new profit information """
        # Acessing Redis can cause greenlet switches because new jobs. We don't
        # want to potentially switch jobs multiple times quickly, so we update
        # the profitability information all at once after the loop to avoid
        # multiple network switches
        new_price_data = {}
        for manager in self.jobmanagers.itervalues():
            currency = manager.config['currency']
            pscore = self.redis.get("{}_profit".format(currency))

            # Deserialize
            if pscore:
                try:
                    pscore = simplejson.loads(pscore, use_decimal=True)
                except Exception:
                    self.logger.warn(
                        "Error parsing profit score for {}! Setting it to 0.."
                        .format(currency))
                    pscore = 0
                    pass
            # If no score was grabbed, pass a 0 value score
            else:
                self.logger.warn("Unable to grab profit info for {}!"
                                 .format(currency))
                pscore = 0

            ratio = self.redis.get("{}_ratio".format(currency)) or 1.0
            ratio = float(ratio)

            # Only set updated if it actually changed
            if self.price_data[currency][0] != pscore or self.price_data[currency][1] != ratio:
                new_price_data[currency] = (pscore, ratio, time.time())

        # If we have some new information, adjust accordingly
        if new_price_data:
            self.logger.info("Updated price information for {}"
                             .format(new_price_data.keys()))
            # Atomic update in gevent
            self.price_data.update(new_price_data)

            # Update all the profit info. No preemption, just maths
            for currency in self.jobmanagers.iterkeys():
                self.update_profitability(currency)

            self.logger.debug(
                "Re-checking best network after new price data for {}"
                .format(new_price_data.keys()))
            self.check_best()

    def check_best(self):
        """ Assuming that `profit_data` is completely up to date, evaluate the
        most profitable network and switch immediately if there's a big enough
        difference. Otherwise set it to be changed at next block notification.
        """
        # Get the most profitable network based on our current data
        new_best = max(self.profit_data.iteritems(),
                       key=operator.itemgetter(1))[0]

        if self.current_network is None:
            self.logger.info(
                "No active network, so switching to {} with profit of {:,.4f}"
                .format(new_best, self.profit_data[new_best]))
            self.next_network = new_best
            self.switch_network()
            return

        # If the currently most profitable network is 120% the profitability
        # of what we're mining on, we should switch immediately
        margin_switch = self.config['margin_switch']
        if (margin_switch and
                self.profit_data[self.next_network] >
                (self.profit_data[self.current_network] * margin_switch)):
            self.logger.info(
                "Network {} {:,.4f} now more profitable than current network "
                "{} {:,.4f} by a fair margin. Switching NOW."
                .format(new_best, self.profit_data[new_best], self.current_network,
                        self.profit_data[self.current_network]))
            self.next_network = new_best
            self.switch_network()
            return

        if new_best != self.next_network:
            self.logger.info(
                "Network {} {:,.4f} now more profitable than current best "
                "{} {:,.4f}. Switching on next block from current network {}."
                .format(new_best, self.profit_data[new_best], self.next_network,
                        self.profit_data[self.next_network], self.current_network))
            self.next_network = new_best
            return

        self.logger.debug("Network {} {:,.4f} still most profitable"
                          .format(new_best, self.profit_data[new_best]))

    def switch_network(self):
        """ Pushes a network change to the user if it's needed """
        if self.next_network != self.current_network:
            job = self.jobmanagers[self.next_network].latest_job
            if job is None:
                self.logger.error(
                    "Tried to switch network to {} that has no job!"
                    .format(self.next_network))
                return
            if self.current_network:
                self.logger.info(
                    "Switching from {} {:,.4f} -> {} {:,.4f} and pushing job NOW"
                    .format(self.current_network, self.profit_data[self.current_network],
                            self.next_network, self.profit_data[self.next_network]))
            self.current_network = self.next_network
            job.type = 0
            self.new_job.job = job
            self.new_job.set()
            self.new_job.clear()
            return True
        return False

    def update_profitability(self, currency):
        """ Recalculates the profitability for a specific currency """
        jobmanager = self.jobmanagers[currency]
        last_job = jobmanager.latest_job
        pscore, ratio, _ = self.price_data[currency]
        # We can't update if we don't have a job and profit data
        if last_job is None or pscore is None:
            return False

        max_blockheight = jobmanager.config['max_blockheight']
        if max_blockheight is not None and last_job.block_height >= max_blockheight:
            self.profit_data[currency] = 0
            self.logger.debug(
                "{} height {} is >= the configured maximum blockheight of {}, "
                "setting profitability to 0."
                .format(currency, last_job.block_height, max_blockheight))
            return True

        block_value = last_job.total_value / 100000000.0
        diff = bits_to_difficulty(hexlify(last_job.bits))

        self.profit_data[currency] = (block_value * float(pscore) / diff) * ratio * 1000000
        self.logger.debug(
            "Updating {} profit data;\n\tblock_value {};\n\tavg_price {:,.8f}"
            ";\n\tdiff {};\n\tratio {};\n\tresult {}"
            .format(currency, block_value, float(pscore), diff,
                    ratio, self.profit_data[currency]))
        self.manager.log_event("{name}.profitability.{curr}:{metric}|g"
                               .format(name=self.manager.config['procname'],
                                       curr=currency,
                                       metric=self.profit_data[currency]))
        return True

    def new_job_notif(self, event):
        currency = event.job.currency
        flush = event.job.type == 0
        if currency == self.current_network:
            self.logger.info("Recieved new job on most profitable network {}"
                             .format(currency))
            # See if we need to switch now that we're done with that block. If
            # not, push a new job on this network
            if not self.switch_network():
                self.new_job.job = event.job
                self.new_job.set()
                self.new_job.clear()

        # If we're recieving a new block then diff has changed, so update the
        # network profit and recompute best network
        if flush and self.update_profitability(currency):
            self.logger.debug("Re-checking best network after new job from {}"
                              .format(currency))
            self.check_best()

    def start(self):
        Jobmanager.start(self)

        self.config['jobmanagers'] = set(self.config['jobmanagers'])
        found_managers = set()
        for manager in self.manager.component_types['Jobmanager']:
            if manager.key in self.config['jobmanagers']:
                currency = manager.config['currency']
                self.jobmanagers[currency] = manager
                self.profit_data[currency] = 0
                self.price_data[currency] = (None, None, None)
                found_managers.add(manager.key)
                manager.new_job.rawlink(self.new_job_notif)

        for monitor in self.config['jobmanagers'] - found_managers:
            self.logger.error("Unable to locate Jobmanager(s) '{}'".format(monitor))
