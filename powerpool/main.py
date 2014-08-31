import yaml
import argparse
import datetime
import setproctitle
import gevent
import gevent.hub
import signal
import subprocess
import powerpool
import time
import logging
import sys

from gevent import sleep
from gevent.monkey import patch_all
patch_all()

from .utils import import_helper
from .lib import MinuteStatManager, SecondStatManager, Component
from .jobmanagers import Jobmanager
from .reporters import Reporter
from .stratum_server import StratumServer


def main():
    parser = argparse.ArgumentParser(description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    parser.add_argument('-d', '--dump-config', action="store_true",
                        help='print the result of the YAML configuration file and exit')
    args = parser.parse_args()

    # override those defaults with a loaded yaml config
    raw_config = yaml.load(args.config) or {}
    if args.dump_config:
        import pprint
        pprint.pprint(raw_config)
        exit(0)

    PowerPool.from_raw_config(raw_config).start()


class PowerPool(Component):
    """ This is a singelton class that manages starting/stopping of the server,
    along with all statistical counters rotation schedules. It takes the raw
    config and distributes it to each module, as well as loading dynamic modules.

    It also handles logging facilities by being the central logging registry.
    Each module can "register" a logger with the main object, which attaches
    it to configured handlers.
    """
    manager = None
    gl_methods = ['_tick_stats']
    defaults = dict(procname="powerpool",
                    term_timeout=10,
                    extranonce_serv_size=4,
                    extranonce_size=4,
                    default_component_log_level='INFO',
                    loggers=[{'type': 'StreamHandler', 'level': 'NOTSET'}],
                    algorithms=dict(
                        x11={"module": "drk_hash.getPoWHash",
                             "hashes_per_share": 4294967296},
                        scrypt={"module": "ltc_scrypt.getPoWHash",
                                "hashes_per_share": 65536},
                        scryptn={"module": "vtc_scrypt.getPoWHash",
                                 "hashes_per_share": 65536},
                        blake256={"module": "blake_hash.getPoWHash",
                                  "hashes_per_share": 65536},
                        sha256={"module": "cryptokit.sha256d",
                                "hashes_per_share": 65536}
                    ))

    @classmethod
    def from_raw_config(self, raw_config):
        components = {}
        types = [PowerPool, Reporter, Jobmanager, StratumServer]
        component_types = {cls.__name__: [] for cls in types}
        component_types['other'] = []
        for key, item in raw_config.iteritems():
            obj = import_helper(item['type'])(item)
            obj.key = key
            for typ in types:
                if isinstance(obj, typ):
                    component_types[typ.__name__].append(obj)
                    break
            else:
                component_types['other'].append(obj)
            components[key] = obj

        pp = component_types['PowerPool'][0]
        assert len(component_types['PowerPool']) == 1
        pp.components = components
        pp.component_types = component_types
        return pp

    def __init__(self, config):
        self._configure(config)
        self._log_handlers = []
        # setup all our log handlers
        for log_cfg in self.config['loggers']:
            if log_cfg['type'] == "StreamHandler":
                kwargs = dict(stream=sys.stdout)
            else:
                kwargs = dict()
            handler = getattr(logging, log_cfg['type'])(**kwargs)
            log_level = getattr(logging, log_cfg['level'].upper())
            handler.setLevel(log_level)
            fmt = log_cfg.get('format', '%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)
            self._log_handlers.append((log_cfg.get('listen'), handler))
        self.logger = self.register_logger(self.__class__.__name__)

        setproctitle.setproctitle(self.config['procname'])
        self.version = powerpool.__version__
        self.version_info = powerpool.__version_info__
        self.sha = getattr(powerpool, '__sha__', "unknown")
        self.rev_date = getattr(powerpool, '__rev_date__', "unknown")
        if self.sha == "unknown":
            # try and fetch the git version information
            try:
                output = subprocess.check_output("git show -s --format='%ci %h'",
                                                 shell=True).strip().rsplit(" ", 1)
                self.sha = output[1]
                self.rev_date = output[0]
            # celery won't work with this, so set some default
            except Exception as e:
                self.logger.info("Unable to fetch git hash info: {}".format(e))

        self.algos = {}
        self.server_start = datetime.datetime.utcnow()
        self.logger.info("=" * 80)
        self.logger.info("PowerPool stratum server ({}) starting up..."
                         .format(self.config['procname']))

        # Detect and load all the hash functions we can find
        for name, algo_data in self.config['algorithms'].iteritems():
            self.algos[name] = algo_data.copy()
            self.algos[name]['name'] = name
            mod = algo_data['module']
            try:
                self.algos[name]['module'] = import_helper(mod)
            except ImportError:
                self.algos[name]['module'] = None
            else:
                self.logger.info("Enabling {} hashing algorithm from module {}"
                                 .format(name, mod))

        # Setup all our stat managers
        self._min_stat_counters = []
        self._sec_stat_counters = []

    def start(self):
        for comp in self.components.itervalues():
            comp.manager = self
            comp.counters = self.register_stat_counters(comp, comp.one_min_stats, comp.one_sec_stats)
            if comp is not self:
                comp.logger = self.register_logger(comp.name)
                comp.start()

        # Starts the greenlet
        Component.start(self)

        # This is the main thread of execution, so just continue here waiting
        # for exit signals
        ######
        # Register shutdown signals
        gevent.signal(signal.SIGINT, exit, "SIGINT")
        gevent.signal(signal.SIGTERM, exit, "SIGTERM")

        try:
            gevent.wait()
        # Allow a force exit from multiple exit signals
        finally:
            self.logger.info("Exiting requested, allowing {} seconds for cleanup."
                             .format(self.config['term_timeout']))
            try:
                for comp in self.components.itervalues():
                    self.logger.debug("Calling stop on component {}".format(comp))
                    comp.stop()
                if gevent.wait(timeout=self.config['term_timeout']):
                    self.logger.info("All threads exited normally")
                else:
                    self.logger.info("Timeout reached, shutting down forcefully")
            except gevent.GreenletExit:
                self.logger.info("Shutdown requested again by system, "
                                 "exiting without cleanup")
            self.logger.info("Exit")
            self.logger.info("=" * 80)

    def exit(self, signal=None):
        """ Handle an exit request """
        self.logger.info("{} {}".format(signal, "*" * 80))
        # Kill the top level greenlet
        gevent.kill(gevent.hub.get_hub().parent)

    @property
    def status(self):
        """ For display in the http monitor """
        return dict(uptime=str(datetime.datetime.utcnow() - self.server_start),
                    server_start=str(self.server_start),
                    version=dict(
                        version=self.version,
                        version_info=self.version_info,
                        sha=self.sha,
                        rev_date=self.rev_date)
                    )

    def _tick_stats(self):
        """ A greenlet that handles rotation of statistics """
        last_tick = int(time.time())
        last_send = (last_tick // 60) * 60
        while True:
            now = time.time()
            # time to rotate minutes?
            if now > (last_send + 60):
                for manager in self._min_stat_counters:
                    manager.tock()
                for manager in self._sec_stat_counters:
                    manager.tock()
                last_send += 60

            # time to tick?
            if now > (last_tick + 1):
                for manager in self._sec_stat_counters:
                    manager.tick()
                last_tick += 1

            sleep(last_tick - time.time() + 1.0)

    def register_logger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, self.config['default_component_log_level']))
        for keys, handler in self._log_handlers:
            # If the keys are blank then we assume it wants all loggers
            # registered
            if not keys or name in keys:
                logger.addHandler(handler)

        return logger

    def register_stat_counters(self, comp, min_counters, sec_counters=None):
        """ Creates and adds the stat counters to internal tracking dictionaries.
        These dictionaries are iterated to perform stat rotation, as well
        as accessed to perform stat logging """
        counters = {}
        for key in min_counters:
            new = MinuteStatManager()
            new.owner = comp
            new.key = key
            counters[key] = new
            self._min_stat_counters.append(new)

        for key in sec_counters or []:
            new = SecondStatManager()
            new.owner = comp
            new.key = key
            counters[key] = new
            self._sec_stat_counters.append(new)

        return counters
