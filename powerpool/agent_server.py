import json
import socket
import logging

from gevent import sleep, with_timeout

from .server import GenericServer


class AgentServer(GenericServer):
    logger = logging.getLogger('agent_server')

    def __init__(self, *args, **kwargs):
        GenericServer.__init__(self, **kwargs)
        self.agent_states = {}

    def task_exc(self, name, *args, **kwargs):
        self.net_state['celery'].send_task(
            self.confg['celery_task_prefix'] + '.' + name, *args, **kwargs)

    def handle(self, sock, address):
        self.logger.info("Recieving agent connection from addr {} on sock {}"
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
        }
        # track the id of the last message recieved for replying
        msg_id = None

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
                    self.logger.debug(line)
                    send_error()

            sock.shutdown(socket.SHUT_WR)
            sock.close()
        except socket.error:
            pass
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            del self.agent_clients[state['id']]

        self.logger.info("Closing connection for client {}".format(state['id']))
