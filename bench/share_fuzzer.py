import gevent
import random
import string

from powerpool.clients import StratumClients
import logging
logging.getLogger().addHandler(logging.StreamHandler())


SEED_CLIENTS = 1000
client_id = 0


def rand_str(N):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))


class DummyClient(object):
    def __init__(self, address, worker, id):
        self.address = address
        self.worker = worker
        self.id = id


class DummyReporter(object):
    def add_one_minute(self, address, acc, stamp, worker, dup, low, stale):
        print "add one minute"

    def add_shares(self, address, shares):
        print "add shares"

    def agent_send(self, address, worker, typ, data, time):
        print "agent_send"

    def transmit_block(self, address, worker, height, total_subsidy, fees,
                       hex_bits, hash, merged):
        pass


class DummyServer(object):
    reporter = DummyReporter()
    config = dict(share_batch_interval=10)

server = DummyServer()
clients = StratumClients(server)
clients.start()


def client_sim():
    global client_id

    print "client {} starting".format(client_id)
    if clients.address_lut.keys() and random.randint(1, 3) == 1:
        address = random.choice(clients.address_lut.keys())
        print "picking address from already connected users"
    else:
        address = rand_str(34)

    worker = rand_str(10)
    client = DummyClient(address, worker, client_id)
    clients[client_id] = client
    clients.set_user(client)
    client_id += 1
    try:
        while True:
            if 1 == random.randint(1, 100):  # diconnect the sim client
                break

            if 1 == random.randint(1, 5):  # submit a share
                clients.add_share(address, worker, 100, 1)

            gevent.sleep(random.uniform(0, 0.3))
            #print "iter on client {}".format(client.id)

    finally:
        del clients[client.id]
        print "client {} closing".format(client.id)


def client_maker():
    for i in xrange(SEED_CLIENTS):
        gevent.spawn(client_sim)

    while True:
        gevent.sleep(random.uniform(0.2, 2))
        client_sim()

gevent.joinall([gevent.spawn(client_maker)])
