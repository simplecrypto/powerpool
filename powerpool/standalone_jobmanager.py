import yaml
import argparse
import setproctitle
import gevent
import signal

from gevent.monkey import patch_all
from gevent.event import Event
patch_all()
import logging
from pprint import pformat

from .utils import import_helper


def main():
    parser = argparse.ArgumentParser(description='Run powerpool standalone job manager!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # override those defaults with a loaded yaml config
    raw_config = yaml.load(args.config) or {}

    # check that config has a valid address
    server = JobManager(raw_config, **raw_config['powerpool'])
    server.run()


class JobManager(object):
    def __init__(self, raw_config, procname="powerpool", term_timeout=3, loggers=None):
        if not loggers:
            loggers = [{'type': 'StreamHandler', 'level': 'DEBUG'}]

        self.log_handlers = []

        # setup all our log handlers
        for log_cfg in loggers:
            handler = getattr(logging, log_cfg['type'])()
            log_level = getattr(logging, log_cfg['level'].upper())
            handler.setLevel(log_level)
            fmt = log_cfg.get('format', '%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)
            self.log_handlers.append((log_cfg.get('listen'), handler))

        self.logger = self.register_logger('manager')

        self.logger.info("=" * 80)
        self.logger.info("PowerPool standalone job manager ({}) starting up...".format(procname))
        self.logger.debug(pformat(raw_config))

        setproctitle.setproctitle(procname)
        self.term_timeout = term_timeout
        self.raw_config = raw_config

        # bookkeeping for things to request exit from at exit time
        # A list of all the greenlets that are running
        self.greenlets = []
        self.reporter = None

    def register_logger(self, name):
        logger = logging.getLogger(name)
        for keys, handler in self.log_handlers:
            if not keys or name in keys:
                logger.addHandler(handler)
                # handlers will manage level, so just propogate everything
                logger.setLevel(logging.DEBUG)

        return logger

    def run(self):
        # Network monitor is in charge of job generation...
        self.logger.info("Network monitor starting up")
        cls = import_helper(self.raw_config['jobmanager']['type'])
        self.jobmanager = cls(self, **self.raw_config['jobmanager'])
        self.jobmanager.start()
        self.greenlets.append(self.jobmanager)

        gevent.signal(signal.SIGINT, self.exit, "SIGINT")
        gevent.signal(signal.SIGHUP, self.exit, "SIGHUP")

        self._exit_signal = Event()
        self._exit_signal.wait()

        # stop all greenlets
        for gl in self.greenlets:
            gl.kill(timeout=self.term_timeout, block=False)

        try:
            if gevent.wait(timeout=self.term_timeout):
                self.logger.info("All threads exited normally")
            else:
                from greenlet import greenlet
                import gc
                import traceback
                for ob in gc.get_objects():
                    if not isinstance(ob, greenlet):
                        continue
                    if not ob:
                        continue
                    self.logger.error(''.join(traceback.format_stack(ob.gr_frame)))
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
