import json
import socket
import logging
import re

from time import time
from binascii import hexlify, unhexlify
from struct import pack, unpack
from bitcoinrpc import CoinRPCException
from cryptokit.base58 import get_bcaddress_version
from cryptokit.block import scrypt_int
from cryptokit import target_from_diff
from hashlib import sha256
from gevent import sleep, with_timeout
from gevent.event import Event
from gevent.server import StreamServer
from hashlib import sha1
from os import urandom
from pprint import pformat
from simpledoge.tasks import add_share, add_block, add_one_minute


class StratumServer(StreamServer):
    logger = logging.getLogger('stratum_server')
    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}

    # constansts for share submission outcomes. returned by the share checker
    STALE_SHARE = 21
    LOW_DIFF = 23
    DUP_SHARE = 22
    BLOCK_FOUND = 2
    VALID_SHARE = 1

    # a list of different share types to be used when collecting share
    # statistics (faster than remaking the list repeatedly)
    share_types = ['valid_shares', 'dup_shares', 'low_diff_shares',
                   'stale_shares']

    # a simple mapping of outcomes to indexes for how they're stored in local
    # memory until they get shipped of for graphing
    outcome_to_idx = {
        STALE_SHARE: 'stale_shares',
        LOW_DIFF: 'low_diff_shares',
        DUP_SHARE: 'dup_shares',
        BLOCK_FOUND: 'valid_shares',
        VALID_SHARE: 'valid_shares'
    }

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
        state = {
            # flags for current connection state
            'authenticated': False,
            'subscribed': False,
            'address': None,
            # the worker id. this is also extranonce 1
            'id': hexlify(pack('Q', self.id_count)),
            # subscription id for difficulty on stratum
            'subscr_difficulty': None,
            # subscription id for work notif on stratum
            'subscr_notify': None,
            # all shares keyed by timestamp. will get flushed after a
            # period specified in config
            'valid_shares': {},
            'dup_shares': {},
            'stale_shares': {},
            'low_diff_shares': {},
            # the name of this worker. empty string by default
            'worker': '',
            # last time we sent graphing data to the server
            'last_graph_transmit': 0,
            'new_block_event': None
        }
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 1
        # track the id of the last message recieved for replying
        msg_id = None

        # watch for new block announcements and push accordingly
        def new_block_call(event):
            """ An event triggered by the network monitor when it learns of a
            new block on the network. All old work is now useless so must be
            flushed. """
            push_job(flush=True)
        state['new_block_event'] = Event()
        state['new_block_event'].rawlink(new_block_call)

        def send_error(num=20, id_val=1):
            """ Utility for transmitting an error to the client """
            err = {'id': id_val,
                   'result': None,
                   'error': (num, self.errors[num], None)}
            self.logger.debug("error response: {}".format(pformat(err)))
            fp.write(json.dumps(err, separators=(',', ':')) + "\n")
            fp.flush()

        def send_success(id_val=1):
            """ Utility for transmitting success to the client """
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
            fp.flush()

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
            """ Handles recieving work submission and checking that it is valid
            , if it meets network diff, etc. Sends reply to stratum client. """
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
                send_error(self.STALE_SHARE)
                return self.STALE_SHARE

            header = job.block_header(
                nonce=params[4],
                extra1=state['id'],
                extra2=params[2],
                ntime=params[3])

            # Check a submitted share against previous shares to eliminate
            # duplicates
            share = (state['id'], params[2], params[4])
            if share in job.acc_shares:
                self.logger.info("Duplicate share!")
                send_error(self.DUP_SHARE)
                return self.DUP_SHARE

            job_target = target_from_diff(self.net_state['difficulty'],
                                          self.config['diff1'])
            hash_int = scrypt_int(header)
            if hash_int >= job_target:
                self.logger.debug("Low diff share!")
                send_error(self.LOW_DIFF)
                return self.LOW_DIFF

            # we want to send an ack ASAP, so do it here
            send_success(msg_id)
            self.logger.debug("Valid job accepted!")
            # Add the share to the accepted set to check for dups
            job.acc_shares.add(share)
            self.net_state['latest_shares'].incr(self.net_state['difficulty'])
            add_share.delay(state['address'], self.net_state['difficulty'])

            # valid network hash?
            if hash_int >= job.bits_target:
                return self.VALID_SHARE

            self.logger.log(35, "Valid network block identified!")
            block = job.submit_serial(header)
            for conn in self.net_state['live_connections']:
                try:
                    res = conn.submitblock(hexlify(block))
                except (CoinRPCException, socket.error, ValueError) as e:
                    self.logger.error("Block failed to submit to the server!",
                                      exc_info=True)
                    self.logger.warn(getattr(e, 'error'))
                else:
                    if res is None:
                        hash_hex = hexlify(
                            sha256(sha256(header).digest()).digest()[::-1])
                        add_block.delay(
                            state['address'],
                            self.net_state['current_height'] + 1,
                            job.total_value,
                            job.fee_total,
                            hexlify(job.bits),
                            hash_hex)
                        self.logger.info("NEW BLOCK ACCEPTED!!!")
                        self.logger.info("New block at height %i"
                                         % self.net_state['current_height'])
                        self.logger.info("Block coinbase hash %s"
                                         % job.coinbase.lehexhash)
                        self.net_state['block_solve'] = int(time())
                    else:
                        self.logger.error("Block failed to submit to the server!",
                                          exc_info=True)

            return self.BLOCK_FOUND

        def report_shares(outcome):
            """ Goes through the list of recorded shares and aggregates them
            into one minute chunck for graph. Sends a task to record the minute
            chunks when one it found greater than zero. Also records a share
            into internal records. """
            now = int(time())
            if (now - state['last_graph_transmit']) > 90:
                # bounds for a share to be grouped for transmission in minute
                # chunks. the upper bound is the last minute that has
                # completely passed, while the lower bound is the last time we
                # sent graph data (which should also be an exact round minute,
                # ensuring that we don't submit info for the same minute twice)
                upper = (now // 60) * 60
                lower = state['last_graph_transmit']
                # share records that are to be discarded
                expire = now - self.config['keep_share']
                # for transmission
                chunks = {}
                # a list of indexes that have expired and are ready for removal
                rem = []
                for i, key in enumerate(self.share_types):
                    for stamp, shares in state[key].iteritems():
                        if stamp >= lower and stamp < upper:
                            minute = (stamp // 60) * 60
                            chunks.setdefault(minute, [0, 0, 0, 0])
                            chunks[minute][i] += shares
                        if stamp < expire:
                            rem.append((key, stamp))

                for key, stamp in rem:
                    del state[key][stamp]

                for stamp, shares in chunks.iteritems():
                    add_one_minute.delay(state['address'], shares[0], stamp,
                                         state['worker'], *shares[1:])
                state['last_graph_transmit'] = upper

            key = self.outcome_to_idx[outcome]
            state[key].setdefault(now, 0)
            state[key][now] += self.net_state['difficulty']

        def authenticate(data):
            # if the address they passed is a valid address,
            # use it. Otherwise use the pool address
            bits = data.get('params', [None])[0].split('.', 1)
            username = bits[0]
            if len(bits) > 1:
                self.logger.debug("Registering worker name {}".format(bits[1]))
                state['worker'] = bits[1]
            try:
                version = get_bcaddress_version(username)
            except Exception:
                version = False

            if version:
                state['address'] = username
            else:
                filtered = re.sub('[\W_]+', '', username).lower()
                self.logger.debug(
                    "Invalid address passed in, checking aliases against {}"
                    .format(filtered))
                if filtered in self.config['aliases']:
                    state['address'] = self.config['aliases'][filtered]
                    self.logger.debug("Setting address alias to {}"
                                      .format(state['address']))
                else:
                    state['address'] = self.config['donate_address']
                    self.logger.debug("Falling back to donate address {}"
                                      .format(state['address']))
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
                line = with_timeout(self.config['push_job_interval'],
                                    fp.readline,
                                    timeout_value='timeout')

                # push a new job every timeout seconds if requested
                if line == 'timeout':
                    self.logger.debug(
                        "Pushing new job to client {} after timeout"
                        .format(state['id']))
                    if state['authenticated'] is True:
                        push_job(flush=False)
                    continue

                line = line.strip()

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

                        res = submit_job(data)
                        if res:
                            report_shares(res)
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
