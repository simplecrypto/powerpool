from gevent.server import StreamServer


class GenericServer(StreamServer):
    def __init__(self, listener, stratum_clients, config, net_state,
                 server_state, celery, **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.stratum_clients = stratum_clients
        self.config = config
        self.net_state = net_state
        self.server_state = server_state
        self.celery = celery
        self.id_count = 0

    def task_exc(self, name, *args, **kwargs):
        self.net_state['celery'].send_task(
            self.confg['celery_task_prefix'] + '.' + name, *args, **kwargs)
