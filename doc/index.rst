.. PowerPool documentation master file, created by
   sphinx-quickstart on Sat Nov  8 14:30:26 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PowerPool: A flexible mining server
=====================================

Features
--------------

* Lightweight, asynchronous, gevent based internals.
* Built in HTTP statistics/monitoring server.
* Flexible statistics collection engine.
* Multiple coinserver (RPC server) support for redundancy. Support for
  coinserver prioritization.
* Redis driven share logging allows multiple servers to log shares and
  statistics to a central source for easy scaling out.
* SHA256, X11, scrypt, and scrypt-n support
* Support for merge mining multiple auxilury (merge mined) blockchains
* Modular architecture makes customization simple(r)
* Support for sending statistics via statsd

Uses Redis to log shares and statistics for miners. Work generation and
(bit|lite|alt)coin data structure serialization is performed by `Cryptokit
<https://github.com/simplecrypto/cryptokit>`_ and connects to bitcoind using
GBT for work generation (or getauxblock for merged work).  Currently only
Python 2.7 is supported.

Built to power the `SimpleMulti <http://simplemulti.com>`_ mining pool.


Indices and tables
-------------------

.. toctree::
   :maxdepth: 2

   setup.rst
   push.rst
   api.rst
