from flask import Flask, jsonify, abort
from gevent import sleep

import logging

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


def stat_rotater(net_state):
    while True:
        val = net_state['latest_shares'].reset()
        net_state['share_ticks'].append(val)
        sleep(1)
