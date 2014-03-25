from flask import Flask, jsonify, abort
from itertools import chain
from collections import deque

import logging
import sys
import psutil
import datetime
import resource


logger = logging.getLogger('monitor')
monitor_app = Flask('monitor')
cpu_times = (None, None)


@monitor_app.route('/')
def general():
    net_state = monitor_app.config['net_state']
    stratum_clients = monitor_app.config['stratum_clients']
    agent_clients = monitor_app.config['agent_clients']
    server_state = monitor_app.config['server_state']
    print monitor_app.logger.handlers

    share_summary = server_state['shares'].summary()
    share_summary['megahashpersec'] = ((2 ** 16) * share_summary['min_total']) / 1000000 / 60.0
    return jsonify(stratum_clients=len(stratum_clients) - 2,
                   server_start=str(server_state['server_start']),
                   uptime=str(datetime.datetime.utcnow() - server_state['server_start']),
                   agent_clients=len(agent_clients),
                   current_height=net_state['current_height'],
                   difficulty=net_state['difficulty'],
                   jobs=len(net_state['jobs']),
                   shares=share_summary,
                   reject_dup=server_state['reject_dup'].summary(),
                   reject_low=server_state['reject_low'].summary(),
                   reject_stale=server_state['reject_stale'].summary(),
                   agent_disconnects=server_state['agent_disconnects'].summary(),
                   agent_connects=server_state['agent_connects'].summary(),
                   stratum_disconnects=server_state['stratum_disconnects'].summary(),
                   stratum_connects=server_state['stratum_connects'].summary(),
                   block_solve=server_state['block_solve'])


@monitor_app.route('/client/<address>')
def client(address=None):
    try:
        clients = monitor_app.config['stratum_clients']['address_lut'][address]
    except KeyError:
        abort(404)

    return jsonify(**{address: [client.summary for client in clients]})


@monitor_app.route('/clients')
def clients():
    lut = monitor_app.config['stratum_clients']['address_lut']
    clients = {key: [item.details for item in value]
               for key, value in lut.iteritems()}

    return jsonify(clients=clients)


@monitor_app.route('/agents')
def agents():
    agent_clients = monitor_app.config['agent_clients']
    agents = {key: value.summary for key, value in agent_clients.iteritems()}

    return jsonify(agents=agents)


@monitor_app.route('/memory')
def memory():
    def total_size(o, handlers={}):
        dict_handler = lambda d: chain.from_iterable(d.items())
        all_handlers = {tuple: iter,
                        list: iter,
                        deque: iter,
                        dict: dict_handler,
                        set: iter,
                        frozenset: iter,
                        }
        all_handlers.update(handlers)     # user handlers take precedence
        seen = set()                      # track which object id's have already been seen
        default_size = sys.getsizeof(0)       # estimate sizeof object without __sizeof__

        def sizeof(o):
            if id(o) in seen:       # do not double count the same object
                return 0
            seen.add(id(o))
            s = sys.getsizeof(o, default_size)

            for typ, handler in all_handlers.items():
                if isinstance(o, typ):
                    s += sum(map(sizeof, handler(o)))
                    break
            return s

        return sizeof(o)

    keys = ['net_state', 'stratum_clients', 'agent_clients', 'server_state']
    out = {key: sys.getsizeof(monitor_app.config[key]) for key in keys}
    out['total'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return jsonify(**out)


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
