import json
import socket

from binascii import hexlify
from struct import pack
from cryptokit.base58 import get_bcaddress_version
from gevent.event import Event
from gevent.server import StreamServer


class StratumServer(StreamServer):
    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}

    def __init__(self, listener, logger, client_states, difficulty, config, **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.logger = logger
        self.client_states = client_states
        self.config = config
        self.difficulty = 0
        self.id_count = 0

    def send_error(self, fp, num=20, id_val=1):
        fp.write(json.dumps({'id': id_val,
                             'result': None,
                             'error': (num, self.errors[num], None)}))
        fp.flush()

    def handle(self, sock, address):
        fp = sock.makefile()
        # create a dictionary to track our
        state = {'authenticated': False,
                 'address': None,
                 'id': hexlify(pack('Q', self.id_count))}
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 1

        # do a finally call to cleanup when we exit
        try:
            self.client_states.add(state)
            # watch for new block announcements and push accordingly
            def new_block_call(event):
                fp.write("new block announced!\n")
                fp.flush()
            state['new_block_event'] = Event()
            state['new_block_event'].rawlink(new_block_call)
            while True:
                try:
                    data = json.loads(fp.readline())
                except ValueError:
                    self.send_error(fp)
                    break

                self.logger.debug("Data {} recieved on client {}"
                                  .format(data, state['id']))

                ret = None
                if 'method' in data:
                    meth = data['method'].lower()
                    if meth == 'mining.subscribe':
                        ret = {'result': ((("mining.set_difficulty", "something"),
                                          ("mining.notify", state['id'])),
                                          state['id'],
                                          self.config['extranonce_size'])}
                    elif meth == "mining.authorize":
                        username = data.get('params', [None])[0]
                        if get_bcaddress_version(username):
                            address = username
                        else:
                            address = self.config['pool_address']
                        ret = {'result': True}
                        state['authorized'] = True
                    elif meth == "mining.submit":
                        pass

                msg_id = data.get('id', 1)
                if not ret:
                    self.send_error()
                else:
                    ret.update(dict(id=msg_id, re))

                fp.write(json.dumps(ret))
                fp.flush()
            sock.shutdown(socket.SHUT_WR)
            sock.close()
        finally:
            self.client_states.remove(state)
