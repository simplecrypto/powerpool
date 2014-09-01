from flask import Flask, jsonify, abort, send_from_directory
from cryptokit.block import BlockTemplate
from cryptokit.transaction import Transaction
from gevent.wsgi import WSGIServer, WSGIHandler
from collections import deque

from .utils import time_format
from .lib import Component

import os


class Logger(object):
    """ A dummy file object to allow using a logger to log requests instead
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


class ServerMonitor(Component, WSGIServer):
    """ Provides a few useful json endpoints for viewing server health and
    performance. """
    # Use our custom wsgi handler
    handler_class = CustomWSGIHandler
    defaults = dict(address="127.0.0.1",
                    port=3855,
                    DEBUG=False)

    def __init__(self, config):
        self._configure(config)
        app = Flask(__name__)
        app.config.update(self.config)
        app.add_url_rule('/', 'general', self.general)
        app.add_url_rule('/debug', 'debug', self.debug)
        app.add_url_rule('/counters', 'counters', self.counters)
        app.add_url_rule('/client/<address>', 'client', self.client)
        app.add_url_rule('/<comp_key>/clients', 'serv_clients_comp', self.clients_comp)
        app.add_url_rule('/<comp_key>/', 'component', self.comp)
        app.add_url_rule('/viewer/', 'viewer', self.viewer)
        app.add_url_rule('/viewer/<path:filename>', 'viewer_sub', self.viewer)
        # Legacy
        app.add_url_rule('/05/clients/', 'clients', self.clients_0_5)
        app.add_url_rule('/05/', 'general_0_5', self.general_0_5)

        self.viewer_dir = os.path.join(os.path.abspath(
            os.path.dirname(__file__) + '/../'), 'viewer')
        self.app = app
        WSGIServer.__init__(self, (self.config['address'],
                                   self.config['port']),
                            self.app,
                            spawn=100,
                            log=Logger())

    def start(self, *args, **kwargs):
        self.logger.info("Stratum server starting up on {address}:{port}"
                         .format(**self.config))

        # Monkey patch the wsgi logger
        Logger.logger = self.logger

        WSGIServer.start(self, *args, **kwargs)
        Component.start(self)

    def stop(self, *args, **kwargs):
        self.close()
        self.pool.kill(block=False)
        Component.stop(self)

    def debug(self):
        if not self.app.config['DEBUG']:
            abort(403)
        data = {}
        for i, comp in enumerate(self.manager.components):
            data[i] = jsonize(comp.__dict__)
        return jsonify(data)

    def general(self):
        data = {}
        for comp in self.manager.components:
            key = "{}_{}".format(comp.__class__.__name__, id(comp))
            try:
                data[key] = comp.status
            except Exception:
                data[key] = "Component Error"
                self.logger.error("Component {} raised invalid status"
                                  .format(comp), exc_info=True)
        return jsonify(data)

    def client(self, address):
        clients = []
        for server in self.manager.component_types['StratumServer']:
            clients.extend(server.address_lut.get(address, []))

        return jsonify(**{address: [client.details for client in clients]})

    def comp(self, comp_key):
        return jsonify(**self.manager.components[comp_key].status)

    def clients_comp(self, comp_key):
        try:
            lut = self.manager.components[comp_key].address_lut
        except KeyError:
            abort(404)

        clients = {key: [item.summary for item in value]
                   for key, value in lut.iteritems()}

        return jsonify(clients=clients)

    def viewer(self, filename=None):
        if not filename:
            filename = "index.html"
        return send_from_directory(self.viewer_dir, filename)

    def counters(self):
        counters = []
        counters.extend(c.summary() for c in self.manager._min_stat_counters)
        counters.extend(c.summary() for c in self.manager._sec_stat_counters)
        return jsonify(counters=counters)

    def clients_0_5(self):
        """ Legacy client view emulating version 0.5 support """
        lut = self.manager.component_types['StratumServer'][0].address_lut
        clients = {key: [item.summary for item in value]
                   for key, value in lut.iteritems()}

        return jsonify(clients=clients)

    def general_0_5(self):
        """ Legacy 0.5 emulating view """
        return jsonify(server={},
                       stratum_manager=self.manager.component_types['StratumServer'][0].status)


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
