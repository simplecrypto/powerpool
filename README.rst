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

Now copy ``config.yml.example`` to ``config.yml``. All the defaults are
commented out and mandatory fields are uncommented. Fill our your coinserver
RPC connection information at the top. It should now be good to go.

.. code-block:: bash

    pp config.yml

And now your stratum server is running. Point a miner at it on
``localhost:3333`` and do some mining. View server health on the monitor port
at ``http://localhost:3855``. Various events will be getting logged
into RabbitMQ to be picked up by a celery worker. See `Simple Doge
<https://github.com/ericecook/simpledoge>`_ for a reference task handling
example.

============
License
============

BSD
