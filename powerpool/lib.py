import time
import logging

from copy import deepcopy
from collections import deque
from gevent import sleep, spawn
from functools import wraps

from .utils import recursive_update
from .exceptions import ConfigurationError


# A sufficiently random number to not collide with real default requirement values
REQUIRED = 2345987234589723495872345
manager = None


def loop(interval=None, precise=False, fin=None, exit_exceptions=None, setup=None, backoff=1):
    """ Causes the function to loop infinitely at the specified interval.

    Precise allows timing to follow the desired interval as closely as
    possible.  For example, we might desire a function to execute as close to
    1 second after the minute. Simply sleeping for 60 seconds every iteration
    doesn't take into account the execution time of the loop and inaccuracy of
    the sleep function, so we adjust the amount we sleep to meet our desired
    time. Using an example ficticious execution log...
    t = 0.0
    ... loop operations happen ...
    t = 0.3
    sleep(10)
    t = 10.3
    ... loop operations happen ...
    t = 10.6
    sleep(10)

    With a precise setting of 10 and a interval of 10.
    t = 0.00
    ... loop operation happens ...
    t = 0.30
    sleep((t // 10) * 10 + 10)  # 9.7
    t = 10.01
    ... loop operation happens ...
    t = 10.31
    sleep((t // 10) * 10 + 10)  # 9.69

    And our desired interval of every ten seconds is maintained with good accuracy.
    Setting interval larger than precise allows us to execute some amount of
    time after a certain period, for example with an interval of 61 and a
    precise of 60 we will execute as close to 1 second after the minute as
    possible. Precise cannot be lower than interval, otherwise negative sleep
    values will always be generated...

    Exit exceptions are exceptions that will cause the loop to end. By default
    all exceptions of subclass Exception are absorbed and the loop is continued.

    `fin` should define either a class method name (as a string) or a
    callable that will be called with either an exception matching
    `exit_exceptions` instance or None upon no exception exit.

    `setup` should define either a class method name (as a string) or a
    callable that will be called on loop entry.
    """
    def loop_deco(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            if kwargs.pop('_single_exec', False):
                return f(self, *args, **kwargs)
            if isinstance(interval, basestring):
                interval_val = self.config[interval]
            else:
                interval_val = interval
            if precise and not interval:
                raise ValueError("Cannot perform precise timing without an interval")
            if precise is True:
                precise_val = interval_val
            else:
                precise_val = precise

            # Make class methods properly bounded
            if isinstance(fin, basestring):
                fin_func = getattr(self, fin)
            else:
                fin_func = fin
            if isinstance(setup, basestring):
                setup_func = getattr(self, setup)
            else:
                setup_func = setup

            if setup_func:
                setup_func()

            res = None
            exit_exc = None
            try:
                while True:
                    try:
                        res = f(self, *args, **kwargs)
                    except Exception as e:
                        if exit_exceptions and isinstance(e, exit_exceptions):
                            exit_exc = e
                            return
                        sleep(backoff)
                        self.logger.error(
                            "Unhandled error in {}".format(f.__name__),
                            exc_info=True)
                        continue

                    if res is False:
                        continue

                    if precise:
                        # Integer computation is about twice as fast as float,
                        # and we don't need the precision of floating point
                        # anywhere...
                        now = int(time.time())
                        sleep(((now // precise_val) * precise_val) +
                              interval_val - now)
                    elif interval:
                        sleep(interval_val)

            # Catch even system exit calls exceptions so we can pass them to
            # the finally function.
            except BaseException as e:
                exit_exc = e
            finally:
                if fin_func:
                    return fin_func(exit_exc=exit_exc, caller=f)
                elif exit_exc is not None:
                    raise exit_exc

        return wrapper
    return loop_deco


class Component(object):
    """ Abstract base class documenting the component architecture expectations
    """
    # Provides default configuration values. To make a configuration key required
    # simply make the value = REQUIRED
    defaults = dict()
    key = None
    # A list of class methods that are independent greenlets. These will
    # automatically get started and stopped at appropriate times.
    gl_methods = []
    one_min_stats = []
    one_sec_stats = []
    dependencies = {}

    @property
    def name(self):
        return "{}_{}".format(self.__class__.__name__, self.key)

    def _configure(self, config):
        """ Applies defaults and checks requirements of component configuration
        """
        # Apply defaults
        self.config = deepcopy(self.defaults)
        # Override defaults with provided config information
        recursive_update(self.config, config)
        for key, value in self.config.iteritems():
            if value == REQUIRED:
                raise ConfigurationError(
                    "Key {} is a required configuration value for "
                    "component {}".format(key, self.__class__.__name__))

        if ('log_level' in self.config and
                self.config['log_level'] not in ['DEBUG', 'INFO', 'WARN', 'ERROR']):
            raise ConfigurationError("Invalid logging level specified")
        self.key = self.config.get('key')

    def __getitem__(self, key):
        """ Easy access to configuration values! """
        return self.config[key]

    def start(self):
        """ Called when the application is starting. """
        log_level = self.config.get('log_level')
        if log_level:
            self.logger.setLevel(getattr(logging, log_level))
        self.logger.info("Component {} starting up".format(self.name))
        self.greenlets = {}
        for method in self.gl_methods:
            gl = spawn(getattr(self, method))
            self.logger.info("Starting greenlet {}".format(method))
            self.greenlets[method] = gl

    def stop(self):
        """ Called when the application is trying to exit. Should not block.
        """
        self.logger.info("Component {} stopping".format(self.name))
        for method, gl in self.greenlets.iteritems():
            self.logger.info("Stopping greenlet {}".format(method))
            gl.kill(block=False)

    @property
    def status(self):
        """ Should return a json convertable data structure to be shown in the
        web interface. """
        return dict()

    def update_config(self, updated_config):
        """ A call performed when the configuration file gets reloaded at
        runtime. self.raw_config will have bee pre-populated by the manager
        before call is made.

        Since configuration values of certain components can't be reloaded at
        runtime it's good practice to log a warning when a change is detected
        but can't be implemented. """
        pass

    def _incr(self, counter, amount=1):
        self.counters[counter].incr(amount)

    def _lookup(self, key):
        try:
            return self.manager.components[key]
        except KeyError:
            raise ConfigurationError("Cannot find component {}"
                                     .format(key))


class SecondStatManager(object):
    """ Monitors the last 60 minutes of a specific number at 1 minute precision
    and the last 1 minute of a number at 1 second precision. Essentially a
    counter gets incremented and rotated through a circular buffer.
    """
    def __init__(self):
        self._val = 0
        self.mins = deque([], 60)
        self.seconds = deque([], 60)
        self.total = 0

    def incr(self, amount=1):
        """ Increments the counter """
        self._val += amount

    def tick(self):
        """ should be called once every second """
        self.seconds.append(self._val)
        self.total += self._val
        self._val = 0

    def tock(self):
        # rotate the total into a minute slot
        last_min = sum(self.seconds)
        self.mins.append(last_min)
        return last_min

    @property
    def hour(self):
        return sum(self.mins)

    @property
    def minute(self):
        if len(self.mins):
            return self.mins[-1]
        return 0

    @property
    def second_avg(self):
        return sum(self.seconds) / 60.0

    @property
    def min_avg(self):
        return sum(self.mins) / 60.0

    def summary(self):
        return dict(name=self.key,
                    owner=str(self.owner),
                    total=self.total,
                    min_total=self.minute,
                    hour_total=self.hour,
                    min_avg=self.min_avg)


class MinuteStatManager(SecondStatManager):
    """ Monitors the last 60 minutes of a specific number at 1 minute precision
    """
    def __init__(self):
        SecondStatManager.__init__(self)
        self._val = 0
        self.mins = deque([], 60)
        self.total = 0

    def tock(self):
        """ should be called once every minute """
        self.mins.append(self._val)
        self.total += self._val
        self._val = 0
