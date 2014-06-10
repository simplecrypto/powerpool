import zmq

from time import sleep
from gevent import Greenlet, spawn
from cryptokit.block import BlockTemplate
from future.utils import viewitems


class ZeroMQJobBridge(Greenlet):
    """ Allows you to bridge a jobmanager interface over zeromq to allow
    running it in a separate process. This component replaces the embedded job
    manager. """
    def _set_config(self, **kwargs):
        # A fast way to set defaults for the kwargs then set them as attributes
        self.config = dict()
        self.config.update(kwargs)

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self._set_config(**config)
        self.logger = server.register_logger('jobbridge')
        self.stratum_manager = server.stratum_manager
        self.jobs = {}

        self.context = zmq.Context()
        self.solve_socket = self.context.socket(zmq.SUB)
        self.sub_socket = self.context.socket(zmq.REQ)

    def solve_block(self, *args, **kwargs):
        self.solve_socket.send_json(["solve_block", args, kwargs])

    def _run(self):
        self.sub_socket.connect("tcp://{address}:{port}"
                                .format(**self.config['sub_socket']))
        self.solve_socket.connect("tcp://{address}:{port}"
                                  .format(**self.config['solve_socket']))
        spawn(self.heartbeat)

        while True:
            ret = self.sub_socket.recv_json()
            getattr(self, ret[0])(*ret[1], **ret[2])

    def push_job(self, job_data, id, flush=False, push=False):
        new_job = BlockTemplate()
        new_job.__dict__.update(job_data)

        if push:
            for idx, client in viewitems(self.stratum_manager.clients):
                try:
                    if flush:
                        client.new_block_event.set()
                    else:
                        client.new_work_event.set()
                except AttributeError:
                    pass

    def heartbeat(self):
        while True:
            self.solve_socket.send_json(["yo", [], {}])
            ret = self.solve_block.recv_json()
            if not ret:
                self.logger.error("Failed heartbeat to job manager!")
            sleep(1)

    def kill(self, *args, **kwargs):
        self.logger.info("ZeroMQ job bridge shutting down...")
        self.heartbeat.kill()
        Greenlet.kill(self, *args, **kwargs)
