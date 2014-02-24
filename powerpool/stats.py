from flask import Flask, jsonify, abort
from gevent import sleep

import logging
import time
import psutil

from simpledoge.tasks import add_one_minute

logger = logging.getLogger('stats')
stats_app = Flask('stats')
cpu_times = (None, None)


@stats_app.route('/')
def general():
    net_state = stats_app.config['net_state']
    client_states = stats_app.config['client_states']
    return jsonify(clients=len(client_states),
                   current_height=net_state['current_height'],
                   difficulty=net_state['difficulty'],
                   jobs=len(net_state['jobs']),
                   share_ticks=list(net_state['share_ticks']),
                   block_solve=net_state['block_solve'])


@stats_app.route('/client/<id>')
def client(id=None):
    try:
        client = stats_app.config['client_states'][id]
    except KeyError:
        abort(404)

    return jsonify(shares=client['shares'],
                   id=client['id'],
                   address=client['address'])


@stats_app.route('/server')
def server():
    global cpu_times

    def calculate(t1, t2):
        t1_all = sum(t1)
        t1_busy = t1_all - t1.idle

        t2_all = sum(t2)
        t2_busy = t2_all - t2.idle

        # this usually indicates a float precision issue
        if t2_busy <= t1_busy:
            return 0.0

        busy_delta = t2_busy - t1_busy
        all_delta = t2_all - t1_all
        busy_perc = (busy_delta / all_delta) * 100
        return round(busy_perc, 1)

    ret = {}
    ret.update({"mem_" + key: val for key, val
                in psutil.virtual_memory().__dict__.iteritems()})
    ret.update({"cpu_ptime_" + key: val for key, val
                in psutil.cpu_times_percent().__dict__.iteritems()})
    if None not in cpu_times:
        ret['cpu_percent'] = calculate(*cpu_times)
    else:
        ret['cpu_percent'] = 0
    ret.update({"diskio_" + key: val for key, val
                in psutil.disk_io_counters().__dict__.iteritems()})
    ret.update({"disk_" + key: val for key, val
                in psutil.disk_usage('/').__dict__.iteritems()})
    users = psutil.get_users()
    ret['user_count'] = len(users)
    ret['user_info'] = [(u.name, u.host) for u in users]
    return jsonify(**ret)


def stat_rotater(net_state):
    global cpu_times
    last_send = (int(time.time()) // 60) * 60
    while True:
        val = net_state['latest_shares'].reset()
        net_state['share_ticks'].append(val)
        if last_send < int(time.time()) - 60:
            last_send += 60
            add_one_minute.delay("pool", sum(net_state['share_ticks']), last_send)
            cpu_times = cpu_times[1], psutil.cpu_times()

        sleep(1)
