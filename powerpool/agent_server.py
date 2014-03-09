import json
import socket
import logging

from gevent import sleep, with_timeout

from .server import GenericServer


class AgentServer(GenericServer):
    logger = logging.getLogger('agent')

    errors = {
        20: 'Other/Unknown',
        25: 'Not subscribed',
        30: 'Unkown command',
        31: 'Worker not connected',
        32: 'Already associated',
        33: 'No hello exchanged',
        34: 'Worker not authed',
        35: 'Type not accepted',
        36: 'Invalid format for method',
    }

    def __init__(self, listener, stratum_clients, config, agent_clients,
                 server_state, celery, **kwargs):
        super(GenericServer, self).__init__(listener, **kwargs)
        self.stratum_clients = stratum_clients
        self.agent_clients = agent_clients
        self.config = config
        self.server_state = server_state
        self.celery = celery
        self.id_count = 0

    def task_exc(self, name, *args, **kwargs):
        self.net_state['celery'].send_task(
            self.confg['celery_task_prefix'] + '.' + name, *args, **kwargs)

    def handle(self, sock, address):
        self.logger.info("Recieving agent connection from addr {} on sock {}"
                         .format(address, sock))
        self.server_state['agent_connects'].incr()
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
            'client_state': None,
            'authed': {},
            'client_version': None,
            'id': self.id_count,
        }
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 1

        def send_error(num=20):
            """ Utility for transmitting an error to the client """
            err = {'result': None, 'error': (num, self.errors[num], None)}
            self.logger.debug("error response: {}".format(err))
            fp.write(json.dumps(err, separators=(',', ':')) + "\n")
            fp.flush()

        def send_success():
            """ Utility for transmitting success to the client """
            succ = {'result': True, 'error': None}
            self.logger.debug("success response: {}".format(succ))
            fp.write(json.dumps(succ, separators=(',', ':')) + "\n")
            fp.flush()

        self.agent_clients[state['id']] = state

        # do a finally call to cleanup when we exit
        try:
            while True:
                line = with_timeout(self.config['push_job_interval'],
                                    fp.readline,
                                    timeout_value='timeout')

                # push a new job every timeout seconds if requested
                if line == 'timeout':
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
                else:
                    send_error()
                    sleep(1)
                    continue

                self.logger.debug("Data {} recieved on client {}"
                                  .format(data, state['id']))

                if 'method' in data:
                    meth = data['method'].lower()
                    if meth == 'hello':
                        if state['client_version'] is not None:
                            send_error(32)
                            continue

                        state['client_version'] = data.get('params', [0.1])[0]
                    elif meth == 'worker.authenticate':
                        if state['client_version'] is None:
                            send_error(33)
                            continue
                        username = data.get('params', [""])[0]
                        user_worker = self.convert_username(username)
                        # setup lookup table for easier access from other read sources
                        state['client_state'] = self.stratum_clients['addr_worker_lut'].get(user_worker)
                        if not state['client_state']:
                            send_error(31)

                        # here's where we do some top security checking...
                        state['authed'][username] = user_worker
                        send_success()
                    elif meth == "stats.submit":
                        if state['client_version'] is None:
                            send_error(33)
                            continue

                        if data.get('params', [''])[0] not in state['authed']:
                            send_error(34)
                            continue

                        if 'params' not in data or len(data['params']) != 3:
                            send_error(36)
                            continue

                        worker_addr, typ, data = data['params']
                        user, worker = state['authed'][worker_addr]
                        if typ == "status":
                            self.celery.send_task_pp(
                                'update_status', user, worker, data)
                            send_success()
                        else:
                            send_error(35)
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
            self.server_state['agent_disconnects'].incr()
            if state['client_state']:
                try:
                    del self.agent_clients[state['id']]
                except KeyError:
                    pass

        self.logger.info("Closing agent connection for client {}"
                         .format(state['id']))
