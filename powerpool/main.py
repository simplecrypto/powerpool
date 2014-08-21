import yaml
import argparse
import datetime
import setproctitle
import gevent
import signal
import time
import sys
import subprocess

from gevent import sleep
from gevent.monkey import patch_all
from gevent.event import Event
patch_all()
import logging

from .monitor import MonitorWSGI, MinuteStatManager, SecondStatManager
from .utils import import_helper
from .lib import Component
import powerpool


def main():
    global manager
    parser = argparse.ArgumentParser(description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # override those defaults with a loaded yaml config
    raw_config = yaml.load(args.config) or {}
    raw_config.setdefault('powerpool', {})

    # check that config has a valid address
    manager = Manager(raw_config['powerpool'])
    manager.start(raw_config)


class Manager(Component):
    """ This is a singelton class that manages starting/stopping of the server,
    along with all statistical counters rotation schedules. It takes the raw
    config and distributes it to each module, as well as loading dynamic modules.

    It also handles logging facilities by being the central logging registry.
    Each module can "register" a logger with the main object, which attaches
    it to configured handlers.
    """
    defaults = dict(procname="powerpool",
                    term_timeout=3,
                    loggers=[{'type': 'StreamHandler', 'level': 'DEBUG'}])
    gl_methods = ['_tick_stats']

    def _setup(self):
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

        # Setup all our stat managers
        self.min_stat_counters = []
        self.sec_stat_counters = []

    def register_logger(self, name):
        logger = logging.getLogger(name)
        for keys, handler in self._log_handlers:
            # If the keys are blank then we assume it wants all loggers
            # registered
            if not keys or name in keys:
                logger.addHandler(handler)
                # handlers will manage level, so just propogate everything
                logger.setLevel(logging.DEBUG)

        return logger

    def register_stat_counters(self, comp, min_counters, sec_counters=None):
        """ Creates and adds the stat counters to internal tracking dictionaries.
        These dictionaries are iterated to perform stat rotation, as well
        as accessed to perform stat logging """
        counters = {}
        for key in min_counters:
            new = MinuteStatManager()
            new.owner = comp
            counters[key] = new
            self.min_stat_counters.append(new)

        for key in sec_counters or []:
            new = SecondStatManager()
            new.owner = comp
            counters[key] = new
            self.sec_stat_counters.append(new)

        return counters

    def __getattr__(self, key):
        """ Allow convenient access to stat counters"""
        return self.components[key]

    def start(self, raw_config):
        """ Start all components and register them so we can request their
        graceful termination at exit time. """
        self.server_start = datetime.datetime.utcnow()
        self.logger.info("=" * 80)
        self.logger.info("PowerPool stratum server ({}) starting up..."
                         .format(self.config['procname']))
        super(Manager, self).start()

        for key in raw_config.keys():
            if key in ['reporter', 'stratum', 'jobmanager']:
                # Start the main chain network monitor and aux chain monitors
                cls = import_helper(self.config['types'][key])
                comp = cls(raw_config[key])
                self.components[key] = comp

        for comp in self.components.itervalues():
            comp.start()

        # the monitor server. a simple flask http server that lets you view
        # internal data structures to monitor server health
        #self.monitor_server = MonitorWSGI(self, **raw_config.get('monitor', {}))
        #if self.monitor_server:
        #    self.monitor_server.start()
        #    self.servers.append(self.monitor_server)

        # Register shutdown signals
        gevent.signal(signal.SIGINT, self.exit, "SIGINT")
        gevent.signal(signal.SIGHUP, self.exit, "SIGHUP")

        self._exit_signal = Event()
        # Wait for the exit signal to be called
        self._exit_signal.wait()
        self.stop()

        try:
            if gevent.wait(timeout=self.config['term_timeout']):
                self.logger.info("All threads exited normally")
            else:
                self.logger.info("Timeout reached, shutting down forcefully")

        # Allow a force exit from multiple exit signals
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested again by system, "
                             "exiting without cleanup")

        self.logger.info("=" * 80)

    def exit(self, signal=None):
        """ Handle an exit request """
        self.logger.info("*" * 80)
        self.logger.info("Exiting requested via {}, allowing {} seconds for cleanup."
                         .format(signal, self.config['term_timeout']))
        self._exit_signal.set()

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
        try:
            self.logger.info("Stat rotater starting up")
            last_tick = int(time.time())
            last_send = (last_tick // 60) * 60
            while True:
                now = time.time()
                # time to rotate minutes?
                if now > (last_send + 60):
                    for manager in self.min_stat_counters:
                        manager.tock()
                    for manager in self.sec_stat_counters:
                        manager.tock()
                    last_send += 60

                # time to tick?
                if now > (last_tick + 1):
                    for manager in self.sec_stat_counters:
                        manager.tick()
                    last_tick += 1

                sleep(last_tick - time.time() + 1.0)
        except gevent.GreenletExit:
            self.logger.info("Stat manager exiting...")
