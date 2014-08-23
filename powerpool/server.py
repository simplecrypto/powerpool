from cryptokit.base58 import get_bcaddress_version

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

        if isinstance(version, int):
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
                address = "donate"
                self.logger.debug("Falling back to donate address {}".format(address))
        return address, worker

    @property
    def connection_duration(self):
        return datetime.datetime.utcnow() - self.connection_time_dt

    @property
    def connection_time_dt(self):
        return datetime.datetime.utcfromtimestamp(self.connection_time)
