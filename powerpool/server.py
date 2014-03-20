from gevent.server import StreamServer
from cryptokit.base58 import get_bcaddress_version

import datetime
import re
import socket


class GenericServer(StreamServer):
    def task_exc(self, name, *args, **kwargs):
        self.net_state['celery'].send_task(
            self.confg['celery_task_prefix'] + '.' + name, *args, **kwargs)

    def convert_username(self, username):
        # if the address they passed is a valid address,
        # use it. Otherwise use the pool address
        bits = username.split('.', 1)
        username = bits[0]
        worker = ''
        if len(bits) > 1:
            self.logger.debug("Registering worker name {}".format(bits[1]))
            worker = bits[1][:16]
        try:
            version = get_bcaddress_version(username)
        except Exception:
            version = False

        if version:
            address = username
        else:
            filtered = re.sub('[\W_]+', '', username).lower()
            self.logger.debug(
                "Invalid address passed in, checking aliases against {}"
                .format(filtered))
            if filtered in self.config['aliases']:
                address = self.config['aliases'][filtered]
                self.logger.debug("Setting address alias to {}".format(address))
            else:
                address = self.config['donate_address']
                self.logger.debug("Falling back to donate address {}".format(address))

        return address, worker


class GenericClient(object):

    def convert_username(self, username):
        # if the address they passed is a valid address,
        # use it. Otherwise use the pool address
        bits = username.split('.', 1)
        username = bits[0]
        worker = ''
        if len(bits) > 1:
            self.logger.debug("Registering worker name {}".format(bits[1]))
            worker = bits[1][:16]
        try:
            version = get_bcaddress_version(username)
        except Exception:
            version = False

        if version:
            address = username
        else:
            filtered = re.sub('[\W_]+', '', username).lower()
            self.logger.debug(
                "Invalid address passed in, checking aliases against {}"
                .format(filtered))
            if filtered in self.config['aliases']:
                address = self.config['aliases'][filtered]
                self.logger.debug("Setting address alias to {}".format(address))
            else:
                address = self.config['donate_address']
                self.logger.debug("Falling back to donate address {}".format(address))

        return address, worker

    def write_loop(self):
        try:
            for item in self.write_queue:
                self.fp.write(item)
                self.fp.flush()
        except socket.error:
            self._disconnected = True
        except Exception:
            self.logger.warn("Unhandled exception in write loop!", exc_info=True)
            self._disconnected = True

    @property
    def connection_time_dt(self):
        return datetime.datetime.utcfromtimestamp(self.connection_time)
