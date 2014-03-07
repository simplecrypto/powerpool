from gevent import sleep

import logging
import time
import psutil


logger = logging.getLogger('stats')
cpu_times = (None, None)


def stat_rotater(server_state, celery):
    global cpu_times
    last_send = (int(time.time()) // 60) * 60
    while True:
        val = server_state['latest_shares'].reset()
        server_state['share_ticks'].append(val)
        if last_send < int(time.time()) - 60:
            last_send += 60
            celery.send_task_pp('add_one_minute', "pool",
                                sum(server_state['share_ticks']), last_send)
            cpu_times = cpu_times[1], psutil.cpu_times()

        sleep(1)
