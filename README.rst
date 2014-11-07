=========
PowerPool
=========

A `gevent <http://www.gevent.org/>`_ based `Stratum
<http://mining.bitcoin.cz/stratum-mining>`_ mining pool server.

========
Features
========

* Lightweight, asynchronous, gevent based internals.
* Built in HTTP statistics/monitoring server.
* Flexible statistics collection engine.
* Multiple coinserver (RPC server) support for redundancy. Support for coinserver prioritization.
* Celery driven share logging allows multiple servers to log shares and
  statistics to a central source for easy scaling out.
* SHA256, X11, scrypt, and scrypt-n support
* Support for merge mining multiple auxilury (merge mined) blockchains
* Modular architecture makes customization simple(r)

Uses `Celery <http://www.celeryproject.org/>`_ to log shares and statistics for
miners. Work generation and (bit|lite|alt)coin data structure serialization is
performed by `Cryptokit <https://github.com/icook/cryptokit>`_ and connects to
bitcoind using GBT for work generation (or getauxblock for merged work).
Currently only Python 2.7 is supported.

Built to power the `SimpleDoge <http://simpledoge.com>`_ mining pool.

    
======
Donate
======

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

    mkvirtualenv pp  # if you've got virtualenvwrapper...
    # Install all of powerpools dependencies
    pip install -r requirements.txt
    # Install powerpool
    pip install -e .
    # Install the hashing algorithm modules
    pip install vtc_scrypt  # for scryptn support
    pip install drk_hash  # for x11 support
    pip install ltc_scrypt  # for scrypt support
    pip install git+https://github.com/BlueDragon747/Blakecoin_Python_POW_Module.git@e3fb2a5d4ea5486f52f9568ffda132bb69ed8772#egg=blake_hash

Now copy ``config.yml.example`` to ``config.yml``. All the defaults are
commented out and mandatory fields are uncommented. Fill out all required fields
and you should be good to go for testing.

.. code-block:: bash

    pp config.yml

And now your stratum server is running. Point a miner at it on
``localhost:3333`` (or more specifically, ``stratum+tcp://localhost:3333`` and
do some mining. View server health on the monitor port at
``http://localhost:3855``. Various events will be getting logged into RabbitMQ
to be picked up by a celery worker. See `Simple Coin
<https://github.com/simplecrypto/simplecoin>`_ for a reference implementation
of Celery task handler.

=====================
Architecture Overview
=====================

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


==============================================
Setting up push block notifications (optional)
==============================================

To check for new blocks Powerpool defaults to polling each of the coinservers
you configure. It just runs the rpc call 'getblockcount' 5x/second
(configurable) to see if the block height has changed. If it has changed, it
runs getblocktemplate to grab the new info.

Since polling creates a 100ms delay (on average) for detecting new blocks one
optimization is to configure the coinservers to push PowerPool a notification
when they accept a new block. Since this reduces the delay to <1ms
you'll end up with fewer orphans. The impact of the faster speed is more
pronounced with currencies that have shorter block times.

Although this is an improvement, its worth mentioning that it is pretty minor.
We're talking about shaving off ~100ms or so, which should reduce orphan
percentages by ~0.01% - 0.1%, depending on block times. Miners often connect with
far more latency than this.

How it push block works
-----------------------

Standard Bitcoin/Litecoin based coinservers have a built in config option to
allow executing a script right after a new block is discovered. We want to run
a script that notifies our PowerPool process to check for a new block.

To accomplish this PowerPool has built in support for receiving a UDP datagram
on its monitor port. The basic system flow looks like this:

Coinserver -> Learns of new block
Coinserver -> Executes blocknotify script (Alertblock)
Alertblock -> Parses the passed in .push file
Alertblock -> Sends a UDP datagram based on that .push file
PowerPool -> Receives UDP datagram
PowerPool -> Runs `getblocktemplate` on the Coinserver

Note: Using a pushblock script to deliver a UDP datagram to PowerPool can
be accomplished in many different ways. We're going to walk
through how we've set it up on our own servers, but please note if your
server configuration/architechture differs much from ours you may have to adapt
this guide.

Modify the coinserver's config
------------------------------

This is the part that tells the coinserver what script to run when it learns
of a new block.

.. code-block:: bash

    blocknotify=/usr/bin/alertblock /home/USER/coinserver_push/vertcoin.push

You'll want something similar to this in each coinserver's config. Make sure to
restart it after.


Alertblock script
-----------------

Now that the coin server is trying to run /usr/bin/alertblock, you'll need to
make that Alertblock script.

Open your text editor of choice and save this to /usr/bin/alertblock

.. code-block:: bash

    #!/bin/bash
    cat $1 | xargs -P 0 -d '\n' -I ARGS bash -c 'a="ARGS"; args=($a); echo "${args[@]:2}" | nc -4u -w0 -q1 ${args[@]:0:2}'
    # For testing the command
    #cat $1 | xargs -P 0 -td '\n' -I ARGS bash -xc 'a="ARGS"; args=($a); echo "${args[@]:2}" | nc -4u -w0 -q1 ${args[@]:0:2}'


Block .push script
------------------

Now your Alertblock script will be looking for a
/home/USER/coinserver_push/vertcoin.push file. The data in this file is
interpreted by the Alertblock script. It looks at each line and tries to send
a UDP packet based on the info. The .push file might contain something like
this:

.. code-block:: bash

    127.0.0.1 6855 VTC getblocktemplate signal=1 __spawn=1

Basically, this tells the Alertblock script to send a UDP datagram to 127.0.0.1
on port 6855. PowerPool will parse the datagram and run getblocktemplate
for the currency VTC.

The port (6855) should be the monitor port for the stratum process you want
to send the notification to. The currency code (VTC) should match one of the
configured currencies in that stratum's config.

If you need to push to multiple monitor ports just do something like:

.. code-block:: bash

    127.0.0.1 6855 VTC getblocktemplate signal=1 __spawn=1
    127.0.0.1 6856 VTC getblocktemplate signal=1 __spawn=1

For merge mined coins you'll want something slightly different:

.. code-block:: bash

    127.0.0.1 6855 DOGE _check_new_jobs signal=1 _single_exec=True __spawn=1


Powerpool config
----------------

Now we need to update PowerPool's config to not poll, as it is no longer needed,
and makes the coinserver's logs a lot harder to use. All that needs to be done
is set the `poll` key to False for each currency you have push block setup for.

.. code-block:: python

    VTC:
        poll: False
        type: powerpool.jobmanagers.MonitorNetwork
        algo: scryptn
        currency: VTC
        etc...

Confirm it is working
---------------------

You'll want to double check push block notifications are actually
working as planned. The easiest way is to visit PowerPool's monitoring endpoint
and look for the `last_signal` key. It should be updated each time PowerPool is
notified of a block via push block.

=======
License
=======

BSD
