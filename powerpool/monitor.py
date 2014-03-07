from flask import Flask, jsonify, abort

import logging
import psutil


logger = logging.getLogger('monitor')
monitor_app = Flask('monitor')
cpu_times = (None, None)


@monitor_app.route('/')
def general():
    net_state = monitor_app.config['net_state']
    stratum_clients = monitor_app.config['stratum_clients']
    server_state = monitor_app.config['server_state']
    shares = sum(server_state['share_ticks'])
    mhr = ((2 ** 16) * shares) / 1000000 / 60.0
    return jsonify(clients=len(stratum_clients),
                   current_height=net_state['current_height'],
                   difficulty=net_state['difficulty'],
                   jobs=len(net_state['jobs']),
                   megahashpersec=mhr,
                   oneminshares=shares,
                   block_solve=server_state['block_solve'])


@monitor_app.route('/client/<id>')
def client(id=None):
    try:
        client = monitor_app.config['stratum_clients'][id]
    except KeyError:
        abort(404)

    return jsonify(shares=client['shares'],
                   id=client['id'],
                   address=client['address'])


@monitor_app.route('/server')
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
