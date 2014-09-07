from cryptokit.base58 import get_bcaddress_version
from gevent import spawn

from .lib import loop

import socket
import datetime
import re


class GenericClient(object):
    def convert_username(self, username):
        # if the address they passed is a valid address,
        # use it. Otherwise use the pool address
        bits = username.split('.', 1)
        username = bits[0]
        worker = ''
        if len(bits) > 1:
            parsed_w = re.sub(r'[^a-zA-Z0-9\[\]_]+', '-', str(bits[1]))
            self.logger.debug("Registering worker name {}".format(parsed_w))
            worker = parsed_w[:16]

        try:
            version = get_bcaddress_version(username)
        except Exception:
            version = False

        if self.config['valid_address_versions'] and version not in self.config['valid_address_versions']:
            version = False

        if isinstance(version, int) and version is not False:
            address = username
        else:
            # Filter all except underscores and letters
            filtered = re.sub('[\W_]+', '', username).lower()
            self.logger.debug(
                "Invalid address passed in, checking aliases against {}"
                .format(filtered))
            if filtered in self.config['aliases']:
                address = self.config['aliases'][filtered]
                self.logger.debug("Setting address alias to {}".format(address))
            else:
                address = self.config['donate_key']
                self.logger.debug("Falling back to donate key {}".format(address))
        return address, worker

    def start(self):
        self.server.add_client(self)
        try:
            self.peer_name = self.sock.getpeername()
        except socket.error:
            self.logger.warn(
                "Peer was no longer connected when trying to setup connection.")
        self.fp = self.sock.makefile()

        self._rloop = spawn(self.read)
        self._wloop = spawn(self.write)

    def stop(self, exit_exc=None, caller=None):
        spawn(self._stop)

    def _stop(self, exit_exc=None, caller=None):
        if self._stopped:
            return

        self._stopped = True
        self._rloop.kill(block=True)
        self._wloop.kill(block=True)

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
        self.logger.info("Closing connection for client {}".format(self._id))

    @property
    def connection_duration(self):
        return datetime.datetime.utcnow() - self.connection_time_dt

    @property
    def connection_time_dt(self):
        return datetime.datetime.utcfromtimestamp(self.connection_time)

    @loop(fin='stop', exit_exceptions=(socket.error, ))
    def write(self):
        for item in self.write_queue:
            self.fp.write(item)
            self.fp.flush()
