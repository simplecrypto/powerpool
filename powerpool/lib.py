import time

from gevent import sleep, spawn
from functools import wraps

from .utils import recursive_update
from .exceptions import ConfigurationError


# A sufficiently random number to not collide with real default requirement values
REQUIRED = 2345987234589723495872345


def loop(interval=None, precise=False):
    def loop_deco(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            while True:
                if interval and precise:
                    now = time.time()
                    target = ((now // 60) * 60) + interval
                    sleep(target - now)
                elif interval:
                    sleep(interval)

                # Make our loop resiliant!
                try:
                    f(self, *args, **kwargs)
                except Exception:
                    self.logger.error(
                        "Unhandled error in {}".format(f.__name__), exc_info=True)

        return wrapper
    return loop_deco


manager = None


class Component(object):
    """ Abstract base class documenting the component architecture expectations
    """
    # Provides default configuration values. To make a configuration key required
    # simply make the value = REQUIRED
    defaults = dict()
    # A list of class methods that are independent greenlets. These will
    # automatically get started and stopped at appropriate times.
    gl_methods = []
    one_min_stats = []
    one_sec_stats = []

    def update_config(self, updated_config):
        """ A call performed when the configuration file gets reloaded at
        runtime. self.raw_config will have bee pre-populated by the manager
        before call is made.

        Since configuration values of certain components can't be reloaded at
        runtime it's good practice to log a warning when a change is detected
        but can't be implemented. """
        pass

    def _check_config(self):
        pass

    @property
    def status(self):
        """ Should return a json convertable data structure to be shown in the
        web interface. """
        return dict()

    def _name(self):
        return self.__class__.__name__

    def __init__(self, config):
        """ This generally shouldn't be overriden by subclasses. Handles
        common setup. """
        global manager
        if manager is None:
            manager = self
        self.greenlets = {}
        self.components = {}
        self._configure(config)
        self._check_config()
        self.name = self._name()
        self._setup()
        self.logger = manager.register_logger(self.name)
        self.counters = manager.register_stat_counters(self,
                                                       self.one_min_stats,
                                                       self.one_sec_stats)

    def _incr(self, counter, amount=1):
        self.counters[counter].incr(amount)

    def _configure(self, config):
        # Apply defaults
        self.config = self.defaults.copy()
        # Override defaults with provided config information
        recursive_update(self.config, config)
        for key, value in self.config.iteritems():
            if value == REQUIRED:
                raise ConfigurationError(
                    "Key {} is a required configuration value for "
                    "component type".format(key, self.__class__.__name__))

    def __getitem__(self, key):
        """ Easy access to configuration values! """
        return self.config[key]

    def _setup(self):
        """ Do initial setup here """
        pass

    def _start_thread(self, method, *args, **kwargs):
        gl = spawn(getattr(self, method), *args, **kwargs)
        self.logger.info("Starting greenlet {}".format(method))
        self.greenlets[method] = gl

    def start(self):
        """ Called when the application is starting. """
        self.logger.info("Component {} starting up".format(self.name))
        for method in self.gl_methods:
            self._start_thread(method)
        for comp in self.components.itervalues():
            comp.start()

    def stop(self):
        """ Called when the application is trying to exit. Should not block. """
        for gl in self.greenlets.itervalues():
            gl.kill()
        for comp in self.components.itervalues():
            comp.stop()
