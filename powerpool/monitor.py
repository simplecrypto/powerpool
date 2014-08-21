from flask import (Flask, jsonify, abort, Blueprint, current_app,
                   send_from_directory)
from werkzeug.local import LocalProxy
from collections import deque
from cryptokit.block import BlockTemplate
from cryptokit.transaction import Transaction
from gevent.wsgi import WSGIServer, WSGIHandler

from .utils import time_format

import os


main = Blueprint('main', __name__)
stratum_manager = LocalProxy(
    lambda: getattr(current_app, 'stratum_manager', None))
jobmanager = LocalProxy(
    lambda: getattr(current_app, 'jobmanager', None))
server = LocalProxy(
    lambda: getattr(current_app, 'server', None))
reporter = LocalProxy(
    lambda: getattr(current_app, 'reporter', None))
logger = LocalProxy(
    lambda: getattr(current_app, 'real_logger', None))


class Logger(object):
    """ A dummp file object to allow using a logger to log requests instead
    of sending to stderr like the default WSGI logger """
    logger = None

    def write(self, s):
        self.logger.info(s.strip())


class CustomWSGIHandler(WSGIHandler):
    """ A simple custom handler allows us to provide more helpful request
    logging format. Format designed for easy profiling """
    def format_request(self):
        length = self.response_length or '-'
        delta = time_format(self.time_finish - self.time_start)
        client_address = self.client_address[0] if isinstance(self.client_address, tuple) else self.client_address
        return '%s "%s" %s %s %s' % (
            client_address or '-',
            getattr(self, 'requestline', ''),
            (getattr(self, 'status', None) or '000').split()[0],
            length,
            delta)


class MonitorWSGI(WSGIServer):
    # Use our custom wsgi handler
    handler_class = CustomWSGIHandler

    def __init__(self, server, DEBUG=False, address='127.0.0.1', port=3855, enabled=True, **kwargs):
        """ Handles implementing default configurations """
        logger = server.register_logger('monitor')
        wsgi_logger = server.register_logger('monitor_wsgi')
        if not enabled:
            logger.info("HTTP monitor not enabled, not starting up...")
            return
        else:
            logger.info("HTTP monitor enabled, starting up...")
        app = Flask('monitor')
        app.config.update(kwargs)
        app.config['DEBUG'] = debug
        app.register_blueprint(main)

        # Monkey patch the wsgi logger
        Logger.logger = wsgi_logger
        app.real_logger = logger

        # setup localproxy refs
        app.manager = server
        app.jobmanager = server.jobmanager
        app.reporter = server.reporter
        app.stratum_manager = server.stratum
        WSGIServer.__init__(self, (address, port), app, log=Logger())

    def stop(self, *args, **kwargs):
        self.application.real_logger.info("Stopping monitoring server")
        WSGIServer.stop(self, *args, **kwargs)


def jsonize(item):
    """ Recursive function that converts a lot of non-serializable content
    to something json.dumps will like better """
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
        if isinstance(item, BlockTemplate):
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
    return jsonify(stratum_manager=stratum_manager.status,
                   reporter=reporter.status,
                   jobmanager=jobmanager.status,
                   server=server.status)


@main.route('/client/<address>')
def client(address=None):
    try:
        clients = stratum_manager.address_lut[address]
    except KeyError:
        abort(404)

    return jsonify(**{address: [getattr(reporter.addresses.get(address), 'status', None)] +
                      [client.details for client in clients]})


@main.route('/ip/<address>')
def ip_lookup(address=None):
    clients = [client.details for client in stratum_manager.clients.itervalues()
               if getattr(client, 'peer_name', [False])[0] == address]

    return jsonify(**{address: clients})


@main.route('/clients')
def clients():
    lut = stratum_manager.address_lut
    clients = {key: [item.summary for item in value]
               for key, value in lut.iteritems()}

    return jsonify(clients=clients)


@main.route('/agents')
def agents():
    agent_clients = current_app.config['agent_clients']
    agents = {key: value.summary for key, value in agent_clients.iteritems()}

    return jsonify(agents=agents)


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
        return dict(total=self.total,
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


viewer_dir = os.path.join(os.path.abspath(os.path.dirname(__file__) + '/../'), 'viewer')
@main.route('/viewer/')
@main.route('/viewer/<path:filename>')
def viewer(filename=None):
    if not filename:
        filename = "index.html"
    return send_from_directory(viewer_dir, filename)
