from flask import Flask, jsonify, abort, send_from_directory, url_for
from cryptokit.block import BlockTemplate
from cryptokit.transaction import Transaction
from gevent.wsgi import WSGIServer, WSGIHandler
from collections import deque

from .utils import time_format
from .lib import Component

import decimal
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


class ReverseProxied(object):
    '''Wrap the application in this middleware and configure the
    front-end server to add these headers, to let you quietly bind
    this to a URL other than / and to an HTTP scheme that is
    different than what is used locally.

    In nginx:
    location /myprefix {
        proxy_pass http://192.168.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Scheme $scheme;
        proxy_set_header X-Script-Name /myprefix;
        }

    :param app: the WSGI application
    '''
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)


class ServerMonitor(Component, WSGIServer):
    """ Provides a few useful json endpoints for viewing server health and
    performance. """
    # Use our custom wsgi handler
    handler_class = CustomWSGIHandler
    defaults = dict(address="127.0.0.1",
                    port=3855,
                    JSON_SORT_KEYS=False,
                    JSONIFY_PRETTYPRINT_REGULAR=False,
                    DEBUG=False)

    def __init__(self, config):
        self._configure(config)
        app = Flask(__name__)
        app.wsgi_app = ReverseProxied(app.wsgi_app)
        app.config.update(self.config)
        app.add_url_rule('/', 'general', self.general)
        app.add_url_rule('/debug/', 'debug', self.debug)
        app.add_url_rule('/counters/', 'counters', self.counters)
        app.add_url_rule('/<comp_key>/clients/', 'clients_comp', self.clients_comp)
        app.add_url_rule('/<comp_key>/client/<username>', 'client', self.client)
        app.add_url_rule('/<comp_key>/', 'comp', self.comp)
        app.add_url_rule('/<comp_key>/config', 'comp_config', self.comp_config)
        # Legacy
        app.add_url_rule('/05/clients/', 'clients', self.clients_0_5)
        app.add_url_rule('/05/', 'general_0_5', self.general_0_5)

        self.viewer_dir = os.path.join(os.path.abspath(
            os.path.dirname(__file__) + '/../'), 'viewer')
        self.app = app

    def start(self, *args, **kwargs):
        listener = (self.config['address'],
                    self.config['port'] +
                    self.manager.config['server_number'])
        WSGIServer.__init__(self, listener, self.app, spawn=100, log=Logger())

        self.logger.info("Monitoring port listening on {}".format(listener))

        # Monkey patch the wsgi logger
        Logger.logger = self.logger

        WSGIServer.start(self, *args, **kwargs)
        Component.start(self)

    def stop(self, *args, **kwargs):
        WSGIServer.stop(self)
        Component.stop(self)
        self.logger.info("Exit")

    def debug(self):
        data = {}
        for key, comp in self.manager.components.iteritems():
            data[key] = jsonize(comp.__dict__)
        return jsonify(data)

    def general(self):
        from .stratum_server import StratumServer
        data = {}
        for key, comp in self.manager.components.iteritems():
            dict_key = "{}_{}".format(comp.__class__.__name__, key)
            try:
                data[dict_key] = comp.status
                data[dict_key]['config_view'] = url_for(
                    'comp_config', comp_key=key, _external=True)
                if isinstance(comp, StratumServer):
                    data[dict_key]['clients'] = url_for(
                        'clients_comp', comp_key=key, _external=True)
            except Exception as e:
                err = "Component {} status call raised {}".format(key, e)
                data[dict_key] = err
                self.logger.error(err, exc_info=True)
        data['debug_view'] = url_for('debug', _external=True)
        data['counter_view'] = url_for('counters', _external=True)
        return jsonify(jsonize(data))

    def client(self, comp_key, username):
        try:
            component = self.manager.components[comp_key]
        except KeyError:
            abort(404)
        return jsonify(username=[client.details for client in
                                 component.address_lut.get(username, [])])

    def comp_config(self, comp_key):
        try:
            return jsonify(**jsonize(self.manager.components[comp_key].config))
        except KeyError:
            abort(404)

    def comp(self, comp_key):
        try:
            return jsonify(**jsonize(self.manager.components[comp_key].status))
        except KeyError:
            abort(404)

    def clients_comp(self, comp_key):
        try:
            lut = self.manager.components[comp_key].address_lut
        except KeyError:
            abort(404)

        clients = {}
        for username, client_list in lut.iteritems():
            clients[username] = {client._id: client.summary
                                 for client in client_list}
            clients[username]['details_view'] = url_for(
                'client', comp_key=comp_key, username=username, _external=True)

        return jsonify(clients=clients)

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
            k = str(k)
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
        if isinstance(item, Transaction):
            item.disassemble()
            return item.to_dict()
        elif isinstance(item, str):
            return item.encode('string_escape')
        elif isinstance(item, set):
            return list(item)
        elif isinstance(item, decimal.Decimal):
            return float(item)
        elif isinstance(item, (int, long, bool, float)) or item is None:
            return item
        elif hasattr(item, "__dict__"):
            return {str(k).encode('string_escape'): str(v).encode('string_escape')
                    for k, v in item.__dict__.iteritems()}
        else:
            return str(item)
