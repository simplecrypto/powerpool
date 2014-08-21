from cryptokit.base58 import get_bcaddress_version

from .lib import Component
from .agent_server import AgentServer
from .utils import import_helper
from .stratum_server import StratumServer


class StratumManager(Component):
    """ Manages the stratum servers and keeps lookup tables for addresses. """

    one_min_stats = ['stratum_connects', 'stratum_disconnects',
                     'agent_connects', 'agent_disconnects', 'not_subbed_err',
                     'not_authed_err', 'unk_err']
    defaults = dict(aliases={},
                    vardiff=dict(spm_target=20,
                                 interval=30,
                                 tiers=[8, 16, 32, 64, 96, 128, 192, 256, 512]),
                    push_job_interval=30,
                    donate_address='',
                    interfaces={},
                    idle_worker_threshold=300,
                    idle_worker_disconnect_threshold=3600,
                    algorithms=dict(x11="drk_hash.getPoWHash",
                                    scrypt="ltc_scrypt.getPoWHash",
                                    scryptn="vtc_scrypt.getPoWHash",
                                    blake256="blake_hash.getPoWHash",
                                    sha256="cryptokit.sha256d"),
                    agent=dict(enabled=False,
                               port_diff=1111,
                               timeout=120,
                               accepted_types=['temp', 'status', 'hashrate', 'thresholds']))

    def _check_config(self):
        if get_bcaddress_version(self.config['donate_address']) is None:
            self.logger.error("No valid donation address configured! Exiting.")
            exit()

    def _setup(self):
        """ Handles starting all stratum servers and agent servers, along
        with detecting algorithm module imports """
        # A dictionary of all connected clients indexed by id
        self.clients = {}
        self.agent_clients = {}
        # A dictionary of lists of connected clients indexed by address
        self.address_lut = {}
        # A dictionary of lists of connected clients indexed by address and
        # worker tuple
        self.address_worker_lut = {}
        # each of our stream server objects for trickling down a shutdown
        # signal
        self.stratum_servers = []
        self.agent_servers = []
        # counters that allow quick display of these numbers. stratum only
        self.authed_clients = 0
        self.idle_clients = 0
        # Unique client ID counters for stratum and agents
        self.stratum_id_count = 0
        self.agent_id_count = 0

        self.algos = {}
        # Detect and load all the hash functions we can find
        for name, module_str in self.config['algorithms'].iteritems():
            try:
                self.algos[name] = import_helper(module_str)
            except ImportError:
                continue
        #self.logger.info("Enabling {} hashing algorithm from module {}"
        #                 .format(name, module_str))

        # Setup all our server components
        for cfg in self.config['interfaces']:
            # Start a corresponding agent server
            if self.config['agent']['enabled']:
                serv = AgentServer(self.manager, self, stratum_config=cfg,
                                   **self.config['agent'])
                self.components[serv.name] = serv

            serv = StratumServer(cfg)
            self.components[serv.name] = serv

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

    def set_conn(self, client):
        """ Called when a new connection is recieved by stratum """
        self.clients[client.id] = client

    def set_user(self, client):
        """ Add the client (or create) appropriate worker and address trackers
        """
        user_worker = (client.address, client.worker)
        self.address_worker_lut.setdefault(user_worker, [])
        self.address_worker_lut[user_worker].append(client)

        self.address_lut.setdefault(user_worker[0], [])
        self.address_lut[user_worker[0]].append(client)

    def remove_client(self, client):
        """ Manages removing the StratumClient from the luts """
        del self.clients[client.id]
        address, worker = client.address, client.worker

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
