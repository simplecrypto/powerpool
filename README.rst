============
PowerPool
============

A `gevent <http://www.gevent.org/>`_ based `Stratum
<http://mining.bitcoin.cz/stratum-mining>`_ mining pool server.

============
Features
============

* Lightweight, asynchronous, gevent based internals.
* Built in HTTP statistics/monitoring server.
* Flexible statistics collection engine.
* Multiple coinserver support for redundancy. Support for coinserver prioritization.
* Celery driven share logging allows multiple servers to log shares and
  statistics to a central source for easy scaling out.
* SHA256, X11, scrypt, and scrypt-n support
* Support for merge mining multiple auxilury (merge mined) blockchains
* Modular architecture makes customization simple

Uses `Celery <http://www.celeryproject.org/>`_ to log shares and statistics for
miners. Work generation and (bit|lite)coin data structure serialization is
performed by `Cryptokit <https://github.com/icook/cryptokit>`_ and connects to
bitcoind using GBT for work generation. Currently uses Python 2.7 because
Gevent doesn't support 3.3.

Built to power the `SimpleDoge <http://simpledoge.com>`_ mining pool.

    
===============
Donate
===============

If you feel so inclined, you can give back to the devs at the below addresses.

DOGE DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD

BTC 185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR

VTC VkbHY8ua2TjxdL7gY2uMfCz3TxMzMPgmRR

=============
Getting Setup
=============

The only external service PowerPool relies on in is its Celery broker. By
default this will be RabbitMQ on a local connection, so simply having it
installed will work fine.

.. code-block:: bash

    sudo apt-get install rabbitmq-server

Setup a virtualenv and install...

.. code-block:: bash

    # if you've got virtualenvwrapper...
    mkvirtualenv pp
    pip install -r requirements.txt
    pip install -e .
    pip install vtc_scrypt  # for scryptn support
    pip install drk_hash  # for x11 support
    pip install ltc_scrypt  # for scrypt support

Now copy ``config.yml.example`` to ``config.yml``. All the defaults are
commented out and mandatory fields are uncommented. Fill our your coinserver
RPC connection information at the top. It should now be good to go.

.. code-block:: bash

    pp config.yml

And now your stratum server is running. Point a miner at it on
``localhost:3333`` and do some mining. View server health on the monitor port
at ``http://localhost:3855``. Various events will be getting logged
into RabbitMQ to be picked up by a celery worker. See `Simple Coin
<https://github.com/simplecrypto/simplecoin>`_ for a reference task handling
example.

========================
Architecture Overview
========================

**Reporter**
The reporter is responsible for transmitting shares, mining statistics, and new
blocks to some external storage. The reference implementation is the
CeleryReporter which aggregates shares into batches and logs them in a way
designed to interface with SimpleCoin. The reporter is also responsible for
tracking share rates for vardiff. This makes sense if you want vardiff to be
based off the shares per second of an entire address, instead of a single
connection.

**Jobmanager**
This module generates mining jobs and sends them to workers. It must provide
current jobs for the stratum server to be able to push. The reference
implementation monitors an RPC daemon server.

**Server**
This is a singleton class that holds statistics and references to all other
modules. All components get access to this object, which is largely concerned
with handling startup and shutdown, along with statistics rotation.

**Stratum Manager**
Handles spawning one or many stratum servers (which bind to a single port
each), as well as spawning corresponding agent servers as well. It holds data
structures that allow lookup of all StratumClient objects.

**Stratum Server**
A server that listens to a single port and accepts new stratum clients.

**Agent Server**
A server that listens to a single port and accepts new ppagent connections.

============
License
============

BSD
