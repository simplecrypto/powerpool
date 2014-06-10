from flask import Flask, jsonify, abort, Blueprint, current_app
from itertools import chain
from collections import deque
from cryptokit.block import BlockTemplate
from cryptokit.transaction import Transaction
from gevent.wsgi import WSGIServer
from gevent.coros import RLock
from collections import deque

import logging
import sys
import datetime
import resource


logger = logging.getLogger('monitor')
main = Blueprint('main', __name__)


class MonitorWSGI(WSGIServer):
    def __init__(self, DEBUG=False, address='127.0.0.1', port=3855, enabled=True, **kwargs):
        """ Handles implementing default configurations """
        if not enabled:
            logger.info("HTTP monitor not enabled, not starting up...")
            return
        else:
            logger.info("HTTP monitor not enabled, not starting up...")
        monitor_app = Flask('monitor')
        monitor_app.config.update(kwargs)
        monitor_app.config['DEBUG'] = debug
        monitor_app.register_blueprint(main)
        WSGIServer.__init__(self, (address, port), monitor_app)


def jsonize(item):
    if isinstance(item, dict):
        new = {}
        for k, v in item.iteritems():
            if isinstance(v, deque):
                new[k] = jsonize(list(v))
            else:
                new[k] = jsonize(v)
        return new
    elif isinstance(item, list) or isinstance(item, tuple):
        new = []
        for part in item:
            new.append(jsonize(part))
        return new
    else:
        if isinstance(item, StatManager):
            return item.summary()
        elif isinstance(item, BlockTemplate):
            return jsonize(item.__dict__)
        elif isinstance(item, Transaction):
            item.disassemble()
            return item.to_dict()
        elif isinstance(item, str):
            return item.encode('string_escape')
        elif isinstance(item, set):
            return list(item)
        elif (isinstance(item, float) or
                isinstance(item, int) or
                item is None or
                isinstance(item, bool)):
            return item
        else:
            return str(item)


@main.route('/debug')
def debug():
    if not current_app.config['DEBUG']:
        abort(403)
    server = current_app.config['server']
    return jsonify(server=jsonize(server.__dict__),
                   netmon=jsonize(server.netmon.__dict__),
                   stratum_clients=jsonize(server.stratum_clients),
                   stratum_clients_addr_lut=jsonize(server.stratum_clients.address_lut.items()),
                   stratum_clients_worker_lut=jsonize(server.stratum_clients.addr_worker_lut.items())
                   )


@main.route('/')
def general():
    net_state = current_app.config['net_state']
    stratum_clients = current_app.config['stratum_clients']
    agent_clients = current_app.config['agent_clients']
    server_state = current_app.config['server_state']

    share_summary = server_state['shares'].summary()
    share_summary['megahashpersec'] = ((2 ** 16) * share_summary['min_total']) / 1000000 / 60.0

    stale_tot = server_state['reject_stale'].total
    low_tot = server_state['reject_low'].total
    dup_tot = server_state['reject_dup'].total
    acc_tot = server_state['shares'].total or 1

    return jsonify(stratum_clients=len(stratum_clients),
                   server_start=str(server_state['server_start']),
                   uptime=str(datetime.datetime.utcnow() - server_state['server_start']),
                   agent_clients=len(agent_clients),
                   aux_state=jsonize(server_state['aux_state']),
                   main_state=jsonize(net_state['work']),
                   jobs=len(net_state['jobs']),
                   shares=share_summary,
                   share_percs=dict(
                       low_perc=low_tot / float(acc_tot + low_tot) * 100.0,
                       stale_perc=stale_tot / float(acc_tot + stale_tot) * 100.0,
                       dup_perc=dup_tot / float(acc_tot + dup_tot) * 100.0,
                   ),
                   reject_dup=server_state['reject_dup'].summary(),
                   reject_low=server_state['reject_low'].summary(),
                   reject_stale=server_state['reject_stale'].summary(),
                   agent_disconnects=server_state['agent_disconnects'].summary(),
                   agent_connects=server_state['agent_connects'].summary(),
                   stratum_disconnects=server_state['stratum_disconnects'].summary(),
                   stratum_connects=server_state['stratum_connects'].summary())


@main.route('/client/<address>')
def client(address=None):
    try:
        clients = current_app.config['stratum_clients']['address_lut'][address]
    except KeyError:
        abort(404)

    return jsonify(**{address: [client.details for client in clients]})


@main.route('/clients')
def clients():
    lut = current_app.config['stratum_clients']['address_lut']
    clients = {key: [item.summary for item in value]
               for key, value in lut.iteritems()}

    return jsonify(clients=clients)


@main.route('/agents')
def agents():
    agent_clients = current_app.config['agent_clients']
    agents = {key: value.summary for key, value in agent_clients.iteritems()}

    return jsonify(agents=agents)


@main.route('/memory')
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
    out = {key: sys.getsizeof(current_app.config[key]) for key in keys}
    out['total'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return jsonify(**out)


class StatManager(object):
    def __init__(self):
        self._val = 0
        self.mins = deque([], 60)
        self.seconds = deque([], 60)
        self.lock = RLock()
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
