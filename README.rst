============
PowerPool
============

A `gevent <http://www.gevent.org/>`_ based `Stratum
<http://mining.bitcoin.cz/stratum-mining>`_ mining pool server. Uses `Celery
<http://www.celeryproject.org/>`_ to log shares and statistics for miners. Work
generation and (bit|lite)coin data structure serialization is performed by
`Cryptokit <https://github.com/icook/cryptokit>`_ and connects to bitcoind
using GBT for work generation. Currently uses Python 2.7 because Gevent doesn't
support 3.3, but support for 3.3 will quickly follow Gevent.

Built to power the `SimpleDoge <http://simpledoge.com>`_ mining pool.

**Still very green, not ready for production use by others at this point.**

DogeCoin Donation: D5pS6EBYyGwFv1PDdMUctxp21Q8wCAh3tY

============
License
============

BSD
