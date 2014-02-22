import json
import socket
import logging

from time import time
from binascii import hexlify, unhexlify
from struct import pack, unpack
from bitcoinrpc.authproxy import JSONRPCException
from cryptokit.base58 import get_bcaddress_version
from cryptokit.block import BlockTemplate
from cryptokit import target_from_diff
from gevent import sleep
from gevent.event import Event
from gevent.server import StreamServer
from hashlib import sha1
from os import urandom
from pprint import pformat
from simpledoge.tasks import add_share, add_block, add_one_minute, update_block_state


class StratumServer(StreamServer):
    logger = logging.getLogger('stratum_server')
    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}

    def __init__(self, listener, client_states, config, net_state,
                 **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.client_states = client_states
        self.config = config
        self.net_state = net_state
        self.id_count = 0

    def handle(self, sock, address):
        self.logger.info("Recieving connection from addr {} on sock {}"
                         .format(address, sock))
        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)
        fp = sock.makefile()
        # create a dictionary to track our
        state = {'authenticated': False,
                 'subscribed': False,
                 'address': None,
                 'id': hexlify(pack('Q', self.id_count)),
                 'subscr_difficulty': None,
                 'subscr_notify': None,
                 'shares': {},
                 'last_graph_transmit': 0}
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 1
        # track the id of the last message recieved
        msg_id = None

        # watch for new block announcements and push accordingly
        def new_block_call(event):
            """ An event triggered by the network monitor when it learns of a
            new block on the network. All old work is now useless so must be
            flushed. """
            push_job(flush=True)
            fp.flush()
        state['new_block_event'] = Event()
        state['new_block_event'].rawlink(new_block_call)

        def send_error(num=20, id_val=1):
            err = {'id': id_val,
                   'result': None,
                   'error': (num, self.errors[num], None)}
            self.logger.debug("error response: {}".format(pformat(err)))
            fp.write(json.dumps(err, separators=(',', ':')) + "\n")
            fp.flush()

        def send_success(id_val=1):
            succ = {'id': id_val, 'result': True, 'error': None}
            self.logger.debug("success response: {}".format(pformat(succ)))
            fp.write(json.dumps(succ, separators=(',', ':')) + "\n")
            fp.flush()

        def push_difficulty():
            """ Pushes the current difficulty to the client. Currently this
            only happens uppon initial connect, but would be used for vardiff
            """
            send = {'params': [self.net_state['difficulty']],
                    'id': None,
                    'method': 'mining.set_difficulty'}
            fp.write(json.dumps(send, separators=(',', ':')) + "\n")

        def push_job(flush=False):
            """ Pushes the latest job down to the client. Flush is whether
            or not he should dump his previous jobs or not. Dump will occur
            when a new block is found since work on the old block is
            invalid."""
            job = None
            while True:
                jobid = self.net_state['latest_job']
                try:
                    job = self.net_state['jobs'][jobid]
                    break
                except KeyError:
                    self.logger.debug("No jobs available for worker!")
                    sleep(0.5)

            send_params = job.stratum_params() + [flush]
            # 0: job_id 1: prevhash 2: coinbase1 3: coinbase2 4: merkle_branch
            # 5: version 6: nbits 7: ntime 8: clean_jobs
            self.logger.debug(
                "Sending new job to worker\n\tjob_id: {0}\n\tprevhash: {1}"
                "\n\tcoinbase1: {2}\n\tcoinbase2: {3}\n\tmerkle_branch: {4}"
                "\n\tversion: {5}\n\tnbits: {6} ({bt:064x})\n\tntime: {7}"
                "\n\tclean_jobs: {8}\n"
                .format(*send_params, bt=job.bits_target))
            send = {'params': send_params,
                    'id': None,
                    'method': 'mining.notify'}
            fp.write(json.dumps(send, separators=(',', ':')) + "\n")
            fp.flush()

        def submit_job(data):
            params = data['params']
            # [worker_name, job_id, extranonce2, ntime, nonce]
            # ["slush.miner1", "bf", "00000001", "504e86ed", "b2957c02"]
            self.logger.debug(
                "Recieved work submit:\n\tworker_name: {0}\n\t"
                "job_id: {1}\n\textranonce2: {2}\n\t"
                "ntime: {3}\n\tnonce: {4} ({int_nonce})"
                .format(
                    *params,
                    int_nonce=unpack(str("<L"), unhexlify(params[4]))))

            job = self.net_state['jobs'].get(data['params'][1])
            if not job:
                # stale job
                send_error(21)
                return False

            header = job.block_header(
                nonce=params[4],
                extra1=state['id'],
                extra2=params[2],
                ntime=params[3])

            # Check a submitted share against previous shares to eliminate duplicates
            share = (state['id'], params[2], params[4])
            if share in job.acc_shares:
                self.logger.warn("Duplicate share!")
                send_error(22)
                return False


            job_target = target_from_diff(self.net_state['difficulty'],
                                          self.config['diff1'])
            valid_job = job.validate_scrypt(header, target=job_target)
            if not valid_job:
                self.logger.debug("Low diff share!")
                send_error(23)
                return False

            send_success(msg_id)
            self.logger.debug("Valid job accepted!")
            # Add the share to the accepted set
            job.acc_shares.add(share)
            valid_net = BlockTemplate.validate_scrypt(header, job.bits_target)
            self.net_state['latest_shares'].incr(self.net_state['difficulty'])
            add_share.delay(state['address'], self.net_state['difficulty'])
            update_block_state.delay()
            self.logger.info("Updating block states...")

            # valid network?
            if not valid_net:
                return True

            self.logger.log(35, "Valid network block identified!")
            block = job.submit_serial(header)
            for conn in self.net_state['live_connections']:
                try:
                    res = conn.submitblock(hexlify(block))
                except (JSONRPCException, socket.error, ValueError) as e:
                    self.logger.error("Block failed to submit to the server!",
                                      exc_info=True)
                    self.logger.warn(getattr(e, 'error'))
                else:
                    if res is None:
                        add_block.delay(
                            state['address'],
                            self.net_state['current_height'],
                            job.total_value,
                            job.fee_total,
                            hexlify(job.bits))
                        self.logger.info("NEW BLOCK ACCEPTED!!!")
                        self.logger.info("New block at height %i"
                                         % self.net_state['current_height'])
                        self.logger.info("Block coinbase hash %s"
                                         % job.coinbase.lehexhash)
                        self.net_state['block_solve'] = int(time())
                    else:
                        self.logger.error("Block failed to submit to the server!",
                                          exc_info=True)

            return True

        def report_shares():
            """ Goes through the list of recorded shares and aggregates them
            into one minute chunck for graph. Sends a task to record the minute
            chunks when one it found greater than zero. """
            now = int(time())
            if (now - state['last_graph_transmit']) > 90:
                # bounds for a share to be grouped for transmission in minute
                # chunks
                upper = (now // 60) * 60
                lower = state['last_graph_transmit']
                # share records that are to be discarded
                expire = now - self.config['keep_share']
                # for transmission
                chunks = {}
                # a list of indexes that have expired and are ready for removal
                rem = []
                for stamp, shares in state['shares'].iteritems():
                    if stamp >= lower and stamp < upper:
                        minute = (stamp // 60) * 60
                        chunks.setdefault(minute, 0)
                        chunks[minute] += shares
                    if stamp < expire:
                        rem.append(stamp)

                for key in rem:
                    del state['shares'][key]

                for stamp, shares in chunks.iteritems():
                    add_one_minute.delay(state['address'], shares, stamp)
                state['last_graph_transmit'] = upper

            state['shares'].setdefault(now, 0)
            state['shares'][now] += self.net_state['difficulty']

        def authenticate(data):
            # if the address they passed is a valid address,
            # use it. Otherwise use the pool address
            username = data.get('params', [None])[0]
            if get_bcaddress_version(username):
                state['address'] = username
            else:
                self.logger.debug(
                    "Invalid address provided, falling back to "
                    "donation address")
                state['address'] = self.config['donate_address']
            state['authenticated'] = True
            send_success(msg_id)
            push_difficulty()
            push_job()

        def subscribe(data):
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

        self.client_states[state['id']] = state

        # do a finally call to cleanup when we exit
        try:

            while True:
                line = fp.readline().strip()
                # if there's data to read, parse it as json
                if line:
                    try:
                        data = json.loads(line)
                    except ValueError:
                        self.logger.debug("Data {} not JSON".format(line))
                        send_error()
                        continue
                # ignore empty strings sent
                else:
                    send_error()
                    sleep(1)
                    continue

                # set the msgid
                msg_id = data.get('id', 1)
                self.logger.debug("Data {} recieved on client {}"
                                  .format(data, state['id']))

                if 'method' in data:
                    meth = data['method'].lower()
                    if meth == 'mining.subscribe':
                        if state['subscribed'] is True:
                            send_error()
                            continue

                        subscribe(data)
                    elif meth == "mining.authorize":
                        if state['subscribed'] is False:
                            send_error(25)
                            continue
                        if state['authenticated'] is True:
                            send_error()
                            continue

                        authenticate(data)
                    elif meth == "mining.submit":
                        if state['authenticated'] is False:
                            send_error(24)
                            continue

                        if submit_job(data):
                            report_shares()
                else:
                    self.logger.info("Unkown action for command {}"
                                     .format(data))
                    send_error()

            sock.shutdown(socket.SHUT_WR)
            sock.close()
        except socket.error:
            pass
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            del self.client_states[state['id']]

        self.logger.info("Closing connection for client {}".format(state['id']))
