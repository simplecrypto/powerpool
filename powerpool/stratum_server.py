import json
import socket
import datetime
import argparse
import struct
import random
import time

from binascii import hexlify, unhexlify
from cryptokit import target_from_diff, uint256_from_str
from gevent import sleep, with_timeout, spawn
from gevent.queue import Queue
from gevent.pool import Pool
from gevent.server import StreamServer
from hashlib import sha1
from os import urandom
from pprint import pformat

from .server import GenericClient
from .lib import Component, manager


class ArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


password_arg_parser = ThrowingArgumentParser()
password_arg_parser.add_argument('-d', '--diff', type=int)


class StratumServer(Component, StreamServer):
    """ A single port binding of our stratum server. """
    defaults = dict(address="127.0.0.1", start_difficulty=128, vardiff=True, port=3333)

    def __init__(self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        listener = (self.config['address'], self.config['port'])
        StreamServer.__init__(self, listener, spawn=Pool())

    def start(self, *args, **kwargs):
        self.logger.info("Stratum server starting up on {address}:{port}"
                         .format(**self.config))
        StreamServer.start(self, *args, **kwargs)
        Component.start(self)

    def stop(self, *args, **kwargs):
        self.logger.info("Stratum server {address}:{port} stopping"
                         .format(**self.config))
        StreamServer.stop(self, *args, **kwargs)
        Component.stop(self)

    def handle(self, sock, address):
        """ A new connection appears on the server, so setup a new StratumClient
        object to manage it. """
        manager.stratum._incr('stratum_connects')
        manager.stratum.stratum_id_count += 1
        StratumClient(sock, address, manager, self)

    def _name(self):
        return "stratum_{}".format(self.config['port'])


class StratumClient(GenericClient):
    """ Object representation of a single stratum connection to the server. """

    # Stratum error codes
    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}
    # enhance readability by reducing magic number use...
    STALE_SHARE_ERR = 21
    LOW_DIFF_ERR = 23
    DUP_SHARE_ERR = 22

    # constansts for share submission outcomes. returned by the share checker
    VALID_SHARE = 0
    DUP_SHARE = 1
    LOW_DIFF_SHARE = 2
    STALE_SHARE = 3
    share_type_strings = {0: "acc", 1: "dup", 2: "low", 3: "stale"}

    def __init__(self, sock, address, manager, stratum_server):
        self.logger = stratum_server.logger
        self.logger.info("Recieving stratum connection from addr {} on sock {}"
                         .format(address, sock))

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        # global items, allows access to other modules in PowerPool
        manager = manager
        self.config = stratum_server.config
        self.manager_config = manager.stratum.config
        self.algo = manager.stratum.algos[stratum_server.config['algo']]

        # register client into the client dictionary
        self.sock = sock

        # flags for current connection state
        self._disconnected = False
        self.authenticated = False
        self.subscribed = False
        self.idle = False
        self.address = None
        self.worker = None
        # the worker id. this is also extranonce 1
        self.id = hexlify(struct.pack('Q', manager.stratum.stratum_id_count))
        # subscription id for difficulty on stratum
        self.subscr_difficulty = None
        # subscription id for work notif on stratum
        self.subscr_notify = None

        # running total for vardiff
        self.accepted_shares = 0
        # an index of jobs and their difficulty
        self.job_mapper = {}
        self.job_counter = random.randint(0, 100000)
        # last time we sent graphing data to the server
        self.time_seed = random.uniform(0, 10)  # a random value to jitter timings by
        self.last_share_submit = time.time()
        self.last_job_push = time.time()
        self.last_job_id = None
        self.last_diff_adj = time.time() - self.time_seed
        self.difficulty = self.config['start_difficulty']
        # the next diff to be used by push job
        self.next_diff = self.config['start_difficulty']
        self.connection_time = int(time.time())
        self.msg_id = None

        # where we put all the messages that need to go out
        self.write_queue = Queue()
        write_greenlet = None
        self.fp = None

        try:
            manager.stratum.set_conn(self)
            self.peer_name = sock.getpeername()
            self.fp = sock.makefile()
            write_greenlet = spawn(self.write_loop)
            self.read_loop()
        except socket.error:
            self.logger.debug("Socket error closing connection", exc_info=True)
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            if self.authenticated:
                manager.stratum.authed_clients -= 1

            if self.idle:
                manager.stratum.idle_clients -= 1

            if write_greenlet:
                write_greenlet.kill()

            # handle clean disconnection from client
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass
            try:
                if self.fp:
                    self.fp.close()
                self.sock.close()
            except (socket.error, AttributeError):
                pass

            self._incr('stratum_disconnects')
            manager.stratum.remove_client(self)

            self.logger.info("Closing connection for client {}".format(self.id))

    def _incr(self, *args):
        manager.stratum._incr(*args)

    @property
    def summary(self):
        """ Displayed on the all client view in the http status monitor """
        return dict(worker=self.worker,
                    idle=self.idle)

    @property
    def last_share_submit_delta(self):
        return datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(self.last_share_submit)

    @property
    def details(self):
        """ Displayed on the single client view in the http status monitor """
        return dict(alltime_accepted_shares=self.accepted_shares,
                    difficulty=self.difficulty,
                    worker=self.worker,
                    id=self.id,
                    last_share_submit=str(self.last_share_submit_delta),
                    idle=self.idle,
                    address=self.address,
                    ip_address=self.peer_name[0],
                    connection_time=str(self.connection_duration))

    def send_error(self, num=20, id_val=1):
        """ Utility for transmitting an error to the client """
        err = {'id': id_val,
               'result': None,
               'error': (num, self.errors[num], None)}
        self.logger.warn("Error number {} on ip {}".format(num, self.peer_name[0]))
        self.write_queue.put(json.dumps(err, separators=(',', ':')) + "\n")

    def send_success(self, id_val=1):
        """ Utility for transmitting success to the client """
        succ = {'id': id_val, 'result': True, 'error': None}
        self.logger.debug("success response: {}".format(pformat(succ)))
        self.write_queue.put(json.dumps(succ, separators=(',', ':')) + "\n")

    def push_difficulty(self):
        """ Pushes the current difficulty to the client. Currently this
        only happens uppon initial connect, but would be used for vardiff
        """
        send = {'params': [self.difficulty],
                'id': None,
                'method': 'mining.set_difficulty'}
        self.write_queue.put(json.dumps(send, separators=(',', ':')) + "\n")

    def push_job(self, flush=False, timeout=False):
        """ Pushes the latest job down to the client. Flush is whether
        or not he should dump his previous jobs or not. Dump will occur
        when a new block is found since work on the old block is
        invalid."""
        job = None
        while True:
            jobid = manager.jobmanager.latest_job
            try:
                job = manager.jobmanager.jobs[jobid]
                break
            except KeyError:
                self.logger.warn("No jobs available for worker!")
                sleep(0.1)

        if self.last_job_id == job.job_id and not timeout:
            self.logger.info("Ignoring non timeout resend of job id {} to worker {}.{}"
                             .format(job.job_id, self.address, self.worker))
            return

        # we push the next difficulty here instead of in the vardiff block to
        # prevent a potential mismatch between client and server
        if self.next_diff != self.difficulty:
            self.logger.info("Pushing diff updae {} -> {} before job for {}.{}"
                             .format(self.difficulty, self.next_diff, self.address, self.worker))
            self.difficulty = self.next_diff
            self.push_difficulty()

        self.logger.info("Sending job id {} to worker {}.{}{}"
                         .format(job.job_id, self.address, self.worker,
                                 " after timeout" if timeout else ''))

        self._push(job)

    def _push(self, job, flush=False):
        """ Abbreviated push update that will occur when pushing new block
        notifications. Mico-optimized to try and cut stale share rates as much
        as possible. """
        self.last_job_id = job.job_id
        self.last_job_push = time.time()
        # get client local job id to map current difficulty
        self.job_counter += 1
        job_id = str(self.job_counter)
        self.job_mapper[job_id] = (self.difficulty, job.job_id)
        self.write_queue.put(job.stratum_string() % (job_id, "true" if flush else "false"))

    def submit_job(self, data):
        """ Handles recieving work submission and checking that it is valid
        , if it meets network diff, etc. Sends reply to stratum client. """
        params = data['params']
        # [worker_name, job_id, extranonce2, ntime, nonce]
        # ["slush.miner1", "bf", "00000001", "504e86ed", "b2957c02"]
        if __debug__:
            self.logger.debug(
                "Recieved work submit:\n\tworker_name: {0}\n\t"
                "job_id: {1}\n\textranonce2: {2}\n\t"
                "ntime: {3}\n\tnonce: {4} ({int_nonce})"
                .format(
                    *params,
                    int_nonce=struct.unpack(str("<L"), unhexlify(params[4]))))

        if self.idle:
            self.idle = False
            manager.stratum.idle_clients -= 1

        self.last_share_submit = time.time()

        try:
            difficulty, jobid = self.job_mapper[data['params'][1]]
        except KeyError:
            # since we can't identify the diff we just have to assume it's
            # current diff
            self.send_error(self.STALE_SHARE_ERR, id_val=self.msg_id)
            manager.reporter.log_share(self, self.difficulty, self.STALE_SHARE, params)
            return

        # lookup the job in the global job dictionary. If it's gone from here
        # then a new block was announced which wiped it
        try:
            job = manager.jobmanager.jobs[jobid]
        except KeyError:
            self.send_error(self.STALE_SHARE_ERR, id_val=self.msg_id)
            manager.reporter.log_share(self, difficulty, self.STALE_SHARE, params)
            return

        # assemble a complete block header bytestring
        header = job.block_header(
            nonce=params[4],
            extra1=self.id,
            extra2=params[2],
            ntime=params[3])

        # Check a submitted share against previous shares to eliminate
        # duplicates
        share = (self.id, params[2], params[4], params[3])
        if share in job.acc_shares:
            self.logger.info("Duplicate share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.DUP_SHARE_ERR, id_val=self.msg_id)
            manager.reporter.log_share(self, difficulty, self.DUP_SHARE, params, job=job)
            return

        job_target = target_from_diff(difficulty, job.diff1)
        hash_int = uint256_from_str(self.algo(header))
        if hash_int >= job_target:
            self.logger.info("Low diff share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.LOW_DIFF_ERR, id_val=self.msg_id)
            manager.reporter.log_share(self, difficulty, self.LOW_DIFF_SHARE, params, job=job)
            return

        # we want to send an ack ASAP, so do it here
        self.send_success(id_val=self.msg_id)
        # Add the share to the accepted set to check for dups
        job.acc_shares.add(share)
        self.accepted_shares += difficulty
        manager.reporter.log_share(self, difficulty, self.VALID_SHARE, params, job=job,
                                   header_hash=hash_int, header=header)

    def authenticate(self, data):
        try:
            password = data.get('params', [None])[1]
        except IndexError:
            password = ""

        # allow the user to use the password field as an argument field
        try:
            args = password_arg_parser.parse_args(password.split())
        except ArgumentParserError:
            pass
        else:
            if args.diff and args.diff in self.manager_config['vardiff']['tiers']:
                self.difficulty = args.diff
                self.next_diff = args.diff

        username = data.get('params', [None])[0]
        self.logger.info("Authentication request from {} for username {}"
                         .format(self.peer_name[0], username))
        user_worker = self.convert_username(username)

        # unpack into state dictionary
        self.address, self.worker = user_worker
        manager.stratum.set_user(self)
        self.authenticated = True
        manager.stratum.authed_clients += 1

        # notify of success authing and send him current diff and latest job
        self.send_success(self.msg_id)
        self.push_difficulty()
        self.push_job()

    def recalc_vardiff(self):
        # ideal difficulty is the n1 shares they solved divided by target
        # shares per minute
        spm_tar = self.manager_config['vardiff']['spm_target']
        ideal_diff = manager.reporter.spm(self.address) / spm_tar
        self.logger.debug("VARDIFF: Calculated client {} ideal diff {}"
                          .format(self.id, ideal_diff))
        # find the closest tier for them
        new_diff = min(self.manager_config['vardiff']['tiers'], key=lambda x: abs(x - ideal_diff))

        if new_diff != self.difficulty:
            self.logger.info(
                "VARDIFF: Moving to D{} from D{} on {}.{}"
                .format(new_diff, self.difficulty, self.address, self.worker))
            self.next_diff = new_diff
        else:
            self.logger.debug("VARDIFF: Not adjusting difficulty, already "
                              "close enough")

        self.last_diff_adj = time.time()
        self.push_job(timeout=True)

    def subscribe(self, data):
        """ Performs stratum subscription logic """
        self.subscr_notify = sha1(urandom(4)).hexdigest()
        self.subscr_difficulty = sha1(urandom(4)).hexdigest()
        ret = {'result':
               ((("mining.set_difficulty",
                  self.subscr_difficulty),
                 ("mining.notify",
                  self.subscr_notify)),
                self.id,
                manager.jobmanager.config['extranonce_size']),
               'error': None,
               'id': self.msg_id}
        self.subscribed = True
        self.logger.debug("Sending subscribe response: {}".format(pformat(ret)))
        self.write_queue.put(json.dumps(ret) + "\n")

    def read_loop(self):
        while True:
            if self._disconnected:
                self.logger.debug("Read loop encountered disconnect flag from "
                                  "write, exiting")
                break

            # designed to time out approximately "push_job_interval" after
            # the user last recieved a job. Some miners will consider the
            # mining server dead if they don't recieve something at least once
            # a minute, regardless of whether a new job is _needed_. This
            # aims to send a job _only_ as often as needed
            line = with_timeout(time.time() - self.last_job_push + self.manager_config['push_job_interval'] - self.time_seed,
                                self.fp.readline,
                                timeout_value='timeout')

            # push a new job if we timed out
            if line == 'timeout':
                if not self.idle and (time.time() - self.last_share_submit) > self.manager_config['idle_worker_threshold']:
                    self.idle = True
                    manager.stratum.idle_clients += 1

                if (time.time() - self.last_share_submit) > self.manager_config['idle_worker_disconnect_threshold']:
                    self.logger.info("Disconnecting worker {}.{} at ip {} for inactivity"
                                     .format(self.address, self.worker, self.peer_name[0]))
                    break

                if (self.authenticated is True and  # don't send to non-authed
                    # force send if we need to push a new difficulty
                    (self.next_diff != self.difficulty or
                     # send if we're past the push interval
                     time.time() > (self.last_job_push +
                                    self.manager_config['push_job_interval'] -
                                    self.time_seed))):
                    if self.config['vardiff']:
                        self.recalc_vardiff()
                    self.push_job(timeout=True)
                continue

            line = line.strip()

            # if there's data to read, parse it as json
            if len(line):
                try:
                    data = json.loads(line)
                except ValueError:
                    self.logger.warn("Data {}.. not JSON".format(line[:15]))
                    self.send_error()
                    continue
            else:
                # otherwise disconnect. Reading from a defunct connection yeilds
                # an EOF character which gets stripped off
                break

            # set the msgid
            self.msg_id = data.get('id', 1)
            if __debug__:
                self.logger.debug("Data {} recieved on client {}"
                                  .format(data, self.id))

            # run a different function depending on the action requested from
            # user
            if 'method' in data:
                meth = data['method'].lower()
                if meth == 'mining.subscribe':
                    if self.subscribed is True:
                        self.send_error(id_val=self.msg_id)
                        continue

                    self.subscribe(data)
                elif meth == "mining.authorize":
                    if self.subscribed is False:
                        self._incr('not_subbed_err')
                        self.send_error(25, id_val=self.msg_id)
                        continue
                    if self.authenticated is True:
                        self._incr('not_authed_err')
                        self.send_error(24, id_val=self.msg_id)
                        continue

                    self.authenticate(data)
                elif meth == "mining.extranonce.subscribe":
                    self.send_success(id_val=self.msg_id)
                elif meth == "mining.submit":
                    if self.authenticated is False:
                        self._incr('not_authed_err')
                        self.send_error(24, id_val=self.msg_id)
                        continue

                    self.submit_job(data)

                    # don't recalc their diff more often than interval
                    if (self.config['vardiff'] and
                            (time.time() - self.last_diff_adj) > self.manager_config['vardiff']['interval']):
                        self.recalc_vardiff()
                else:
                    self.logger.warn("Unkown action {} for command {}"
                                     .format(data['method'][:20], self.peer_name[0]))
                    self._incr('unk_err')
                    self.send_error(id_val=self.msg_id)
            else:
                self.logger.warn("Empty action in JSON {}"
                                 .format(self.peer_name[0]))
                self._incr('unk_err')
                self.send_error(id_val=self.msg_id)
