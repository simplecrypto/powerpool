import yaml
import argparse
import datetime
import setproctitle
import gevent
import signal
import time
import sys
import subprocess

from gevent import spawn, sleep
from gevent.monkey import patch_all
from gevent.event import Event
patch_all()
import logging
from pprint import pformat

from .stratum_server import StratumManager
from .monitor import MonitorWSGI, MinuteStatManager, SecondStatManager
from .utils import import_helper
import powerpool


def main():
    parser = argparse.ArgumentParser(description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # override those defaults with a loaded yaml config
    raw_config = yaml.load(args.config) or {}
    raw_config.setdefault('powerpool', {})

    # check that config has a valid address
    server = PowerPool(raw_config, **raw_config['powerpool'])
    server.run()


class PowerPool(object):
    def __init__(self, raw_config, procname="powerpool", term_timeout=3, loggers=None):
        if not loggers:
            loggers = [{'type': 'StreamHandler', 'level': 'DEBUG'}]

        self.log_handlers = []

        # setup all our log handlers
        for log_cfg in loggers:
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
            self.log_handlers.append((log_cfg.get('listen'), handler))

        self.logger = self.register_logger('manager')

        self.logger.info("=" * 80)
        self.logger.info("PowerPool stratum server ({}) starting up...".format(procname))
        self.logger.debug(pformat(raw_config))

        setproctitle.setproctitle(procname)
        self.term_timeout = term_timeout
        self.raw_config = raw_config
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

        # bookkeeping for things to request exit from at exit time
        # A list of all the greenlets that are running
        self.greenlets = []
        # A list of all the StreamServers
        self.servers = []

        # Primary systems
        self.stratum_manager = None
        # The network monitor object
        self.jobmanager = None
        # The module that reports everything to the outside
        self.reporter = None

        self.stratum_servers = []
        self.agent_servers = []
        self.monitor_server = None

        # Stats tracking for the whole server
        #####
        self.server_start = datetime.datetime.utcnow()

        # Setup all our stat managers
        self.min_stat_counters = []
        self.sec_stat_counters = []
        self.stat_counters = {}

    def register_logger(self, name):
        logger = logging.getLogger(name)
        for keys, handler in self.log_handlers:
            if not keys or name in keys:
                logger.addHandler(handler)
                # handlers will manage level, so just propogate everything
                logger.setLevel(logging.DEBUG)

        return logger

    def register_stat_counters(self, min_counters, sec_counters=None):
        for key in min_counters:
            if key in self.stat_counters:
                raise ValueError("{} stat counter key has already been registered!"
                                 .format(key))
            new = MinuteStatManager()
            self.stat_counters[key] = new
            self.min_stat_counters.append(new)

        for key in sec_counters or []:
            if key in self.stat_counters:
                raise ValueError("{} stat counter key has already been registered!"
                                 .format(key))
            new = SecondStatManager()
            self.stat_counters[key] = new
            self.sec_stat_counters.append(new)

    def __getitem__(self, key):
        return self.stat_counters[key]

    def run(self):
        # Start the main chain network monitor and aux chain monitors
        self.logger.info("Reporter engine starting up")
        cls = import_helper(self.raw_config['reporter']['type'])
        self.reporter = cls(self, **self.raw_config['reporter'])
        self.reporter.start()
        self.greenlets.append(self.reporter)

        # main stratum server manager, not actually a greenelt but starts
        # several servers and manages data structures
        self.stratum_manager = StratumManager(self, **self.raw_config.get('stratum', {}))
        self.servers.extend(self.stratum_manager.stratum_servers)
        self.servers.extend(self.stratum_manager.agent_servers)

        # Network monitor is in charge of job generation...
        self.logger.info("Network monitor starting up")
        cls = import_helper(self.raw_config['jobmanager']['type'])
        self.jobmanager = cls(self, **self.raw_config['jobmanager'])
        self.jobmanager.start()
        self.greenlets.append(self.jobmanager)

        # a simple greenlet that rotates some of the servers stats
        self.stat_rotater = spawn(self.tick_stats)
        self.greenlets.append(self.stat_rotater)

        # the monitor server. a simple flask http server that lets you view
        # internal data structures to monitor server health
        self.monitor_server = MonitorWSGI(self, **self.raw_config.get('monitor', {}))
        if self.monitor_server:
            self.monitor_server.start()
            self.servers.append(self.monitor_server)

        gevent.signal(signal.SIGINT, self.exit, "SIGINT")
        gevent.signal(signal.SIGHUP, self.exit, "SIGHUP")

        self._exit_signal = Event()
        self._exit_signal.wait()

        # stop all stream servers
        for server in self.servers:
            # timeout is actually the time we wait before killing the greenlet,
            # so don't bother waiting, no cleanup is needed from our servers
            spawn(server.stop, timeout=0)

        # stop all greenlets
        for gl in self.greenlets:
            gl.kill(timeout=self.term_timeout, block=False)

        try:
            if gevent.wait(timeout=self.term_timeout):
                self.logger.info("All threads exited normally")
            else:
                self.logger.info("Timeout reached, shutting down forcefully")
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested again by system, "
                             "exiting without cleanup")

        self.logger.info("=" * 80)

    def exit(self, signal=None):
        self.logger.info("*" * 80)
        self.logger.info("Exiting requested via {}, allowing {} seconds for cleanup."
                         .format(signal, self.term_timeout))
        self._exit_signal.set()

    @property
    def status(self):
        return dict(uptime=str(datetime.datetime.utcnow() - self.server_start),
                    server_start=str(self.server_start),
                    version=dict(
                        version=self.version,
                        version_info=self.version_info,
                        sha=self.sha,
                        rev_date=self.rev_date)
                    )

    def tick_stats(self):
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
