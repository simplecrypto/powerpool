import json
import socket

from binascii import hexlify
from struct import pack
from cryptokit.base58 import get_bcaddress_version
from gevent import sleep
from gevent.event import Event
from gevent.server import StreamServer
from hashlib import sha1
from os import urandom
from pprint import pformat


class StratumServer(StreamServer):
    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}

    def __init__(self, listener, logger, client_states, config, net_state,
                 **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.logger = logger
        self.client_states = client_states
        self.config = config
        self.net_state = net_state
        self.id_count = 0

    def handle(self, sock, address):
        fp = sock.makefile()
        # create a dictionary to track our
        state = {'authenticated': False,
                 'subscribed': False,
                 'address': None,
                 'id': hexlify(pack('Q', self.id_count)),
                 'subscr_difficulty': None,
                 'subscr_notify': None}
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 2
        # track the id of the last message recieved
        msg_id = None

        # watch for new block announcements and push accordingly
        def new_block_call(event):
            self.logger.info("new block announced!\n")
            #fp.flush()
        state['new_block_event'] = Event()
        state['new_block_event'].rawlink(new_block_call)

        def send_error(num=20, id_val=1):
            err = {'id': id_val,
                   'result': None,
                   'error': (num, self.errors[num], None)}
            self.logger.debug("Sending error response: {}"
                              .format(pformat(err)))
            fp.write(json.dumps(err, separators=(',', ':')) + "\n")
            fp.flush()

        def send_success(id_val=1):
            succ = {'id': id_val, 'result': True, 'error': None}
            self.logger.debug("Sending error response: {}"
                              .format(pformat(succ)))
            fp.write(json.dumps(succ, separators=(',', ':')) + "\n")
            fp.flush()

        def push_difficulty():
            send = {'params': [self.net_state['difficulty']],
                    'id': None,
                    'method': 'mining.set_difficulty'}
            fp.write(json.dumps(send, separators=(',', ':')) + "\n")
            fp.flush()

        def push_job(flush=False):
            """ Pushes the latest job down to the client. Flush is whether
            or not he should dump his previous jobs or not. Dump will occur
            when a new block is found since work on the old block is
            invalid."""
            while True:
                jobid = self.net_state['latest_job']
                try:
                    job = self.net_state['jobs'][jobid]
                    break
                except KeyError:
                    self.logger.debug("No jobs available for worker!")
                    sleep(1)

            send = {'params':
                    [jobid, job.hash_prev, job.coinbase1, job.coinbase2,
                        job.merklebranch_hex, job.version_packed, job.target,
                        job.ntime, flush],
                    'id': None,
                    'method': 'mining.notify'}
            fp.write(json.dumps(send, separators=(',', ':')) + "\n")
            fp.flush()

        # do a finally call to cleanup when we exit
        try:
            self.client_states[state['id']] = state

            while True:
                line = fp.readline()
                try:
                    data = json.loads(line)
                except ValueError:
                    self.logger.debug("Data {} not JSON"
                                      .format(line))
                    send_error()
                    continue
                msg_id = data.get('id', 1)
                self.logger.debug("Data {} recieved on client {}"
                                  .format(data, state['id']))

                ret = None
                if 'method' in data:
                    meth = data['method'].lower()
                    if meth == 'mining.subscribe':
                        state['subscr_notify'] = sha1(urandom(4)).hexdigest()
                        state['subscr_difficulty'] = sha1(urandom(4)).hexdigest()
                        ret = {'result':
                               ((("mining.set_difficulty",
                                  state['subscr_difficulty']),
                                 ("mining.notify",
                                  state['subscr_notify'])),
                                state['id'],
                                self.config['extranonce_size']),
                               'error': None,
                               'id': msg_id}
                        state['subscribed'] = True
                        self.logger.debug("Sending subscribe response: {}"
                                          .format(pformat(ret)))
                        fp.write(json.dumps(ret) + "\n")
                        fp.flush()
                        continue
                    elif meth == "mining.authorize":
                        if state['subscribed'] is False:
                            send_error(25)
                            continue

                        # if the address they passed is a valid address,
                        # use it. Otherwise use the pool address
                        username = data.get('params', [None])[0]
                        if get_bcaddress_version(username):
                            state['address'] = username
                        else:
                            state['address'] = self.config['pool_address']
                        state['authorized'] = True
                        send_success(msg_id)
                        push_difficulty()
                        push_job()
                        continue
                    elif meth == "mining.submit":
                        if state['authorized'] is False:
                            send_error(24)
                            continue
                        send_success(msg_id)

            sock.shutdown(socket.SHUT_WR)
            sock.close()
        except socket.error:
            pass
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            del self.client_states[state['id']]
