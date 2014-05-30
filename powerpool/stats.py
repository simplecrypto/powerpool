from gevent import sleep, Greenlet
from collections import deque

import threading
import logging
import time


logger = logging.getLogger('stats')
cpu_times = (None, None)


class StatMonitor(Greenlet):
    def __init__(self, server):
        Greenlet.__init__(self)
        self.celery = server.celery
        self.server = server

    def _run(self):
        last_tick = int(time.time())
        last_send = (int(time.time()) // 60) * 60
        while True:
            now = time.time()
            # time to rotate minutes?
            if now > (last_send + 60):
                shares = self.server_state['shares'].tock()
                reject_low = self.server_state['reject_low'].tock()
                reject_dup = self.server_state['reject_dup'].tock()
                reject_stale = self.server_state['reject_stale'].tock()
                self.server.stratum_connects.tock()
                self.server.stratum_disconnects.tock()
                self.server.agent_connects.tock()
                self.server.agent_disconnects.tock()

                if shares or reject_dup or reject_low or reject_stale:
                    self.celery.send_task_pp(
                        'add_one_minute', 'pool', shares, now, '', reject_dup,
                        reject_low, reject_stale)
                last_send += 60

            # time to tick?
            if now > (last_tick + 1):
                self.server.shares.tick()
                self.server.reject_low.tick()
                self.server.reject_dup.tick()
                self.server.reject_stale.tick()
                self.server.stratum_connects.tick()
                self.server.stratum_disconnects.tick()
                self.server.agent_connects.tick()
                self.server.agent_disconnects.tick()
                last_tick += 1

            sleep(0.1)


class StatManager(object):
    def __init__(self):
        self._val = 0
        self.mins = deque([], 60)
        self.seconds = deque([], 60)
        self.lock = threading.Lock()
        self.total = 0

    def incr(self, amount=1):
        """ Increments the counter """
        with self.lock:
            self._val += amount
    __add__ = incr

    def tick(self):
        """ should be called once every second """
        val = self.reset()
        self.seconds.append(val)
        self.total += val

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
        return sum(self.seconds)

    @property
    def second_avg(self):
        return sum(self.seconds) / 60.0

    @property
    def min_avg(self):
        return sum(self.mins) / 60.0

    def summary(self):
        return dict(total=self.total,
                    min_total=self.minute,
                    hour_total=self.hour,
                    min_avg=self.min_avg)

    def reset(self):
        """ Locks the counter, resets the value, then returns the value """
        with self.lock:
            curr = self._val
            self._val = 0
            return curr
