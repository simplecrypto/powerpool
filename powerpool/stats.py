from flask import Flask, jsonify, abort
from gevent import sleep

import logging
import time
import psutil

from simpledoge.tasks import add_one_minute

logger = logging.getLogger('stats')
stats_app = Flask('stats')


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
    ret = {}
    ret.update({"mem_" + key: val for key, val
                in psutil.virtual_memory().__dict__.iteritems()})
    ret.update({"cpu_ptime_" + key: val for key, val
                in psutil.cpu_times_percent().__dict__.iteritems()})
    ret['cpu_percent'] = psutil.cpu_percent()
    ret.update({"diskio_" + key: val for key, val
                in psutil.disk_io_counters().__dict__.iteritems()})
    ret.update({"disk_" + key: val for key, val
                in psutil.disk_usage('/').__dict__.iteritems()})
    users = psutil.get_users()
    ret['user_count'] = len(users)
    ret['user_info'] = [(u.name, u.host) for u in users]
    return jsonify(**ret)


def stat_rotater(net_state):
    last_send = (int(time.time()) // 60) * 60
    while True:
        val = net_state['latest_shares'].reset()
        net_state['share_ticks'].append(val)
        if last_send < int(time.time()) - 60:
            last_send += 60
            add_one_minute.delay("pool", sum(net_state['share_ticks']), last_send)

        sleep(1)
