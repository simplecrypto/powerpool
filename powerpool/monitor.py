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
        app.add_url_rule('/client/<address>', 'client', self.client)
        app.add_url_rule('/ip/<address>', 'ip_lookup', self.ip_lookup)
        app.add_url_rule('/clients', 'clients', self.clients)
        app.add_url_rule('/viewer/', 'viewer', self.viewer)
        app.add_url_rule('/viewer/<path:filename>', 'viewer_sub', self.viewer)

        self.viewer_dir = os.path.join(os.path.abspath(
            os.path.dirname(__file__) + '/../'), 'viewer')
        self.app = app

    def start(self, *args, **kwargs):
        self.logger.info("Stratum server starting up on {address}:{port}"
                         .format(**self.config))

        # Monkey patch the wsgi logger
        Logger.logger = self.logger
        WSGIServer.__init__(
            self, (self.config['address'], self.config['port']), self.app, log=Logger())

        WSGIServer.start(self, *args, **kwargs)
        Component.start(self)

    def stop(self, *args, **kwargs):
        WSGIServer.close(self)
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
            try:
                data[comp.__class__.__name__] = comp.status
            except Exception:
                data[comp.__class__.__name__] = "Component Error"
                self.logger.error("Component {} raised invalid status"
                                  .format(comp), exc_info=True)
        return jsonify(data)

    def client(self, address):
        try:
            clients = self.manager.component_types['StratumServer'].address_lut[address]
        except KeyError:
            abort(404)

        reporter = self.manager.component_types['Reporter'][0]
        return jsonify(**{address: [getattr(reporter.addresses.get(address), 'status', None)] +
                          [client.details for client in clients]})

    def ip_lookup(self, address):
        clients = self.manager.component_types['StratumServer'][0].clients
        clients = [client.details for client in clients.itervalues()
                   if getattr(client, 'peer_name', [False])[0] == address]

        return jsonify(**{address: clients})

    def clients(self):
        lut = self.manager.component_types['StratumServer'][0].address_lut
        clients = {key: [item.summary for item in value]
                   for key, value in lut.iteritems()}

        return jsonify(clients=clients)

    def viewer(self, filename=None):
        if not filename:
            filename = "index.html"
        return send_from_directory(self.viewer_dir, filename)


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
