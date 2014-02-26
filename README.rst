============
PowerPool
============

A `gevent <http://www.gevent.org/>`_ based `Stratum
<http://mining.bitcoin.cz/stratum-mining>`_ mining pool server.

============
Features
============

* Lightweight, asynchronous, gevent based internals.
* Built in HTTP statistics/monitoring server. Deep introspection into the
  mining servers health at a glance.
* Multiple coinserver support for redundancy.
* Celery driven share logging allows multiple servers to log shares and
  statistics to a central source for easy scaling out.
* Works with most scrypt based currencies, SHA still to come.

Uses `Celery <http://www.celeryproject.org/>`_ to log shares and statistics for
miners. Work generation and (bit|lite)coin data structure serialization is
performed by `Cryptokit <https://github.com/icook/cryptokit>`_ and connects to
bitcoind using GBT for work generation. Currently uses Python 2.7 because
Gevent doesn't support 3.3, but support for 3.3 will quickly follow Gevent.

Built to power the `SimpleDoge <http://simpledoge.com>`_ mining pool.

**Still very green, not quite ready for production use by others at this point
unless you want to wade through some code**

DogeCoin Donation: D5pS6EBYyGwFv1PDdMUctxp21Q8wCAh3tY

============
License
============

BSD
