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
from pprint import pformat

from .agent_server import AgentServer
from .exceptions import LoopExit
from .server import GenericClient
from .utils import time_format
from .lib import Component, loop, REQUIRED


class ArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


password_arg_parser = ThrowingArgumentParser()
password_arg_parser.add_argument('-d', '--diff', type=int)


class StratumServer(Component, StreamServer):
    """ A single port binding of our stratum server. """

    one_min_stats = ['stratum_connects', 'stratum_disconnects',
                     'agent_connects', 'agent_disconnects']
    defaults = dict(address="0.0.0.0",
                    port=3333,
                    start_difficulty=128,
                    algo=REQUIRED,
                    idle_worker_threshold=300,
                    aliases={},
                    vardiff=dict(enabled=False,
                                 spm_target=20,
                                 interval=30,
                                 tiers=[8, 16, 32, 64, 96, 128, 192, 256, 512]),
                    push_job_interval=30,
                    idle_worker_disconnect_threshold=3600,
                    agent=dict(enabled=False,
                               port_diff=1111,
                               timeout=120,
                               accepted_types=['temp', 'status', 'hashrate',
                                               'thresholds']))
    # Don't spawn a greenlet to handle creation of clients, we start one for
    # reading and one for writing in their own class...
    _spawn = None

    def __init__(self, config):
        self._configure(config)
        listener = (self.config['address'], self.config['port'])

        # Start a corresponding agent server
        if self.config['agent']['enabled']:
            serv = AgentServer()
            self.components[serv.name] = serv

        StreamServer.__init__(self, listener, spawn=Pool())

        # A dictionary of all connected clients indexed by id
        self.clients = {}
        self.agent_clients = {}
        # A dictionary of lists of connected clients indexed by address
        self.address_lut = {}
        # A dictionary of lists of connected clients indexed by address and
        # worker tuple
        self.address_worker_lut = {}
        # counters that allow quick display of these numbers. stratum only
        self.authed_clients = 0
        self.idle_clients = 0
        # Unique client ID counters for stratum and agents
        self.stratum_id_count = 0
        self.agent_id_count = 0

    def start(self, *args, **kwargs):
        self.algo_func = self.manager.algos[self.config['algo']]
        self.reporter = self.manager.component_types['Reporter'][0]
        self.jobmanager = self.manager.component_types['Jobmanager'][0]
        self.jobmanager.new_job.rawlink(self.new_job)

        self.logger.info("Stratum server starting up on {address}:{port}"
                         .format(**self.config))
        StreamServer.start(self, *args, **kwargs)
        Component.start(self)

    def stop(self, *args, **kwargs):
        self.logger.info("Stratum server {address}:{port} stopping"
                         .format(**self.config))
        StreamServer.close(self)
        for client in self.clients.values():
            client.stop()
        Component.stop(self)

    def handle(self, sock, address):
        """ A new connection appears on the server, so setup a new StratumClient
        object to manage it. """
        self.logger.info("Recieving stratum connection from addr {} on sock {}"
                         .format(address, sock))
        self.stratum_id_count += 1
        client = StratumClient(
            sock,
            address,
            config=self.config,
            logger=self.logger,
            jobmanager=self.jobmanager,
            manager=self.manager,
            algo_func=self.algo_func,
            server=self,
            reporter=self.reporter)
        self._incr('stratum_connects')
        self.clients[client.id] = client
        client.start()

    def new_job(self, event):
        jm = self.jobmanager
        job = jm.jobs[jm.latest_job]

        t = time.time()
        job.stratum_string()
        flush = job.flush
        for client in self.clients.itervalues():
            if client.authenticated:
                client._push(job, flush=flush, block=False)
        self.logger.info("New job enqueued for transmission to {} users in {}"
                         .format(len(self.clients), time_format(time.time() - t)))

    @property
    def share_percs(self):
        """ Pretty display of what percentage each reject rate is. Counts
        from beginning of server connection """
        acc_tot = self.manager['valid'].total or 1
        low_tot = self.manager['reject_low'].total
        dup_tot = self.manager['reject_dup'].total
        stale_tot = self.manager['reject_stale'].total
        return dict(
            low_perc=low_tot / float(acc_tot + low_tot) * 100.0,
            stale_perc=stale_tot / float(acc_tot + stale_tot) * 100.0,
            dup_perc=dup_tot / float(acc_tot + dup_tot) * 100.0,
        )

    @property
    def status(self):
        """ For display in the http monitor """
        dct = dict(share_percs=self.share_percs,
                   mhps=(self.manager.jobmanager.config['hashes_per_share'] *
                         self.manager['valid'].minute / 1000000 / 60.0),
                   agent_client_count=len(self.agent_clients),
                   client_count=len(self.clients),
                   address_count=len(self.address_lut),
                   address_worker_count=len(self.address_lut),
                   client_count_authed=self.authed_clients,
                   client_count_active=len(self.clients) - self.idle_clients,
                   client_count_idle=self.idle_clients)
        dct.update({key: val.summary() for key, val in self.counters.iteritems()})
        return dct

    def set_user(self, client):
        """ Add the client (or create) appropriate worker and address trackers
        """
        user_worker = (client.address, client.worker)
        self.address_worker_lut.setdefault(user_worker, [])
        self.address_worker_lut[user_worker].append(client)
        self.authed_clients += 1

        self.address_lut.setdefault(user_worker[0], [])
        self.address_lut[user_worker[0]].append(client)

    def remove_client(self, client):
        """ Manages removing the StratumClient from the luts """
        del self.clients[client.id]
        address, worker = client.address, client.worker
        self._incr('stratum_disconnects')

        if client.authenticated:
            self.authed_clients -= 1
        if client.idle:
            self.idle_clients -= 1

        # it won't appear in the luts if these values were never set
        if address is None and worker is None:
            return

        # wipe the client from the address tracker
        if address in self.address_lut:
            # remove from lut for address
            self.address_lut[address].remove(client)
            # if it's the last client in the object, delete the entry
            if not len(self.address_lut[address]):
                del self.address_lut[address]

        # wipe the client from the address/worker tracker
        key = (address, worker)
        if key in self.address_worker_lut:
            self.address_worker_lut[key].remove(client)
            # if it's the last client in the object, delete the entry
            if not len(self.address_worker_lut[key]):
                del self.address_worker_lut[key]

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
    error_counter = {20: 'unk_err',
                     24: 'not_authed_err',
                     25: 'not_subbed_err'}
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

    def __init__(self, sock, address, logger, manager, jobmanager, server,
                 reporter, algo_func, config):
        self.config = config
        self.jobmanager = jobmanager
        self.manager = manager
        self.algo_func = algo_func
        self.server = server
        self.reporter = reporter
        self.logger = logger
        self.sock = sock
        self.address = address

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        self.authenticated = False
        self.subscribed = False
        # flags for current connection state
        self.idle = False
        self.address = None
        self.worker = None
        # the worker id. this is also extranonce 1
        self.id = hexlify(struct.pack('Q', self.server.stratum_id_count))

        t = time.time()
        # running total for vardiff
        self.accepted_shares = 0
        # an index of jobs and their difficulty
        self.job_mapper = {}
        self.job_counter = random.randint(0, 100000)
        # Allows us to avoid a bunch of clients getting scheduled at the same
        # time by offsetting most timing values by this
        self.time_seed = random.uniform(0, 10)
        # Used to determine if they're idle
        self.last_share_submit = t
        # Used to determine if we should send another job on read loop timeout
        self.last_job_push = t
        # Avoids repeat pushing jobs that the client already knows about
        self.last_job_id = None
        # Last time vardiff happened
        self.last_diff_adj = t - self.time_seed
        # Current difficulty setting
        self.difficulty = self.config['start_difficulty']
        # the next diff to be used by push job
        self.next_diff = self.config['start_difficulty']
        # What time the user connected...
        self.connection_time = int(t)
        # What time the user connected...
        self.msg_id = None

        # where we put all the messages that need to go out
        self.write_queue = Queue()
        self.fp = None
        self._stopped = False

    def start(self):
        self.peer_name = self.sock.getpeername()
        self.fp = self.sock.makefile()

        self._rloop = spawn(self.read)
        self._wloop = spawn(self.write)

    def stop(self, exit_exc=None, caller=None):
        if self._stopped:
            return

        self._stopped = True
        self._rloop.kill()
        self._wloop.kill()

        # handle clean disconnection from client
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass
        try:
            self.fp.close()
        except (socket.error, AttributeError):
            pass
        try:
            self.sock.close()
        except (socket.error, AttributeError):
            pass

        self.server.remove_client(self)
        self.logger.info("Closing connection for client {}".format(self.id))

    def _incr(self, *args):
        self.server._incr(*args)

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
            jobid = self.jobmanager.latest_job
            try:
                job = self.jobmanager.jobs[jobid]
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

    def _push(self, job, flush=False, block=True):
        """ Abbreviated push update that will occur when pushing new block
        notifications. Mico-optimized to try and cut stale share rates as much
        as possible. """
        self.last_job_id = job.job_id
        self.last_job_push = time.time()
        # get client local job id to map current difficulty
        self.job_counter += 1
        job_id = str(self.job_counter)
        self.job_mapper[job_id] = (self.difficulty, job.job_id)
        self.write_queue.put(job.stratum_string() % (job_id, "true" if flush else "false"), block=block)

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
            self.server.idle_clients -= 1

        self.last_share_submit = time.time()

        try:
            difficulty, jobid = self.job_mapper[data['params'][1]]
        except KeyError:
            # since we can't identify the diff we just have to assume it's
            # current diff
            self.send_error(self.STALE_SHARE_ERR, id_val=data['id'])
            self.reporter.log_share(client=self,
                                    diff=self.difficulty,
                                    typ=self.STALE_SHARE,
                                    params=params)
            return

        # lookup the job in the global job dictionary. If it's gone from here
        # then a new block was announced which wiped it
        try:
            job = self.jobmanager.jobs[jobid]
        except KeyError:
            self.send_error(self.STALE_SHARE_ERR, id_val=data['id'])
            self.reporter.log_share(client=self,
                                    diff=difficulty,
                                    typ=self.STALE_SHARE,
                                    params=params)
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
            self.send_error(self.DUP_SHARE_ERR, id_val=data['id'])
            self.reporter.log_share(client=self,
                                    diff=difficulty,
                                    typ=self.DUP_SHARE,
                                    params=params,
                                    job=job)
            return

        job_target = target_from_diff(difficulty, job.diff1)
        hash_int = uint256_from_str(self.algo_func(header))
        if hash_int >= job_target:
            self.logger.info("Low diff share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.LOW_DIFF_ERR, id_val=data['id'])
            self.reporter.log_share(client=self,
                                    diff=difficulty,
                                    typ=self.LOW_DIFF_SHARE,
                                    params=params,
                                    job=job)
            return

        # we want to send an ack ASAP, so do it here
        self.send_success(id_val=data['id'])
        # Add the share to the accepted set to check for dups
        job.acc_shares.add(share)
        self.accepted_shares += difficulty
        self.reporter.log_share(client=self,
                                diff=difficulty,
                                typ=self.VALID_SHARE,
                                params=params,
                                job=job,
                                header_hash=hash_int,
                                header=header)

    def recalc_vardiff(self):
        # ideal difficulty is the n1 shares they solved divided by target
        # shares per minute
        spm_tar = self.config['vardiff']['spm_target']
        ideal_diff = self.reporter.spm(self.address) / spm_tar
        self.logger.debug("VARDIFF: Calculated client {} ideal diff {}"
                          .format(self.id, ideal_diff))
        # find the closest tier for them
        new_diff = min(self.config['vardiff']['tiers'], key=lambda x: abs(x - ideal_diff))

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

    @loop(fin='stop', exit_exceptions=(socket.error, ))
    def read(self):
        # designed to time out approximately "push_job_interval" after the user
        # last recieved a job. Some miners will consider the mining server dead
        # if they don't recieve something at least once a minute, regardless of
        # whether a new job is _needed_. This aims to send a job _only_ as
        # often as needed
        line = with_timeout(time.time() - self.last_job_push + self.config['push_job_interval'] - self.time_seed,
                            self.fp.readline,
                            timeout_value='timeout')

        if line == 'timeout':
            t = time.time()
            if not self.idle and (t - self.last_share_submit) > self.config['idle_worker_threshold']:
                self.idle = True
                self.server.idle_clients += 1

            # push a new job if
            if (t - self.last_share_submit) > self.config['idle_worker_disconnect_threshold']:
                self.logger.info("Disconnecting worker {}.{} at ip {} for inactivity"
                                 .format(self.address, self.worker, self.peer_name[0]))
                self.stop()

            if (self.authenticated is True and  # don't send to non-authed
                # force send if we need to push a new difficulty
                (self.next_diff != self.difficulty or
                    # send if we're past the push interval
                    t > (self.last_job_push +
                         self.config['push_job_interval'] -
                         self.time_seed))):
                if self.config['vardiff']['enabled'] is True:
                    self.recalc_vardiff()
                self.push_job(timeout=True)
            return

        line = line.strip()

        # Reading from a defunct connection yeilds an EOF character which gets
        # stripped off
        if not line:
            raise LoopExit("Closed file descriptor encountered")

        try:
            data = json.loads(line)
        except ValueError:
            self.logger.warn("Data {}.. not JSON".format(line[:15]))
            self.send_error()
            return

        # handle malformed data
        data.setdefault('id', 1)
        data.setdefault('params', [])

        if __debug__:
            self.logger.debug("Data {} recieved on client {}".format(data, self.id))

        # run a different function depending on the action requested from
        # user
        if 'method' not in data:
            self.logger.warn("Empty action in JSON {}".format(self.peer_name[0]))
            self._incr('unk_err')
            self.send_error(id_val=data['id'])
            return

        meth = data['method'].lower()
        if meth == 'mining.subscribe':
            if self.subscribed is True:
                self.send_error(id_val=data['id'])
                return

            ret = {
                'result': (
                    (
                        # These values aren't used for anything, although
                        # perhaps they should be
                        ("mining.set_difficulty", self.id),
                        ("mining.notify", self.id)
                    ),
                    self.id,
                    self.manager.config['extranonce_size']
                ),
                'error': None,
                'id': data['id']
            }
            self.subscribed = True
            self.logger.debug("Sending subscribe response: {}".format(pformat(ret)))
            self.write_queue.put(json.dumps(ret) + "\n")

        elif meth == "mining.authorize":
            if self.subscribed is False:
                self._incr('not_subbed_err')
                self.send_error(25, id_val=data['id'])
                return

            if self.authenticated is True:
                self._incr('not_authed_err')
                self.send_error(24, id_val=data['id'])
                return

            try:
                password = data['params'][1]
                username = data['params'][0]
                # allow the user to use the password field as an argument field
                try:
                    args = password_arg_parser.parse_args(password.split())
                except ArgumentParserError:
                    # Ignore malformed parser data
                    pass
                else:
                    if args.diff:
                        self.difficulty = args.diff
                        self.next_diff = args.diff
            except IndexError:
                password = ""
                username = ""

            self.logger.info("Authentication request from {} for username {}"
                             .format(self.peer_name[0], username))
            user_worker = self.convert_username(username)

            # unpack into state dictionary
            self.address, self.worker = user_worker
            self.authenticated = True
            self.server.set_user(self)

            # notify of success authing and send him current diff and latest job
            self.send_success(data['id'])
            self.push_difficulty()
            self.push_job()

        elif meth == "mining.submit":
            if self.authenticated is False:
                self._incr('not_authed_err')
                self.send_error(24, id_val=data['id'])
                return

            self.submit_job(data)

            # don't recalc their diff more often than interval
            if (self.config['vardiff']['enabled'] is True and
                    (time.time() - self.last_diff_adj) > self.config['vardiff']['interval']):
                self.recalc_vardiff()

        elif meth == "mining.extranonce.subscribe":
            self.send_success(id_val=data['id'])

        else:
            self.logger.warn("Unkown action {} for command {}"
                             .format(data['method'][:20], self.peer_name[0]))
            self._incr('unk_err')
            self.send_error(id_val=data['id'])

    @loop(fin='stop', exit_exceptions=(socket.error, ))
    def write(self):
        for item in self.write_queue:
            self.fp.write(item)
            self.fp.flush()

    @property
    def summary(self):
        """ Displayed on the all client view in the http status monitor """
        return dict(worker=self.worker, idle=self.idle)

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
