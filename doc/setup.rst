Getting Setup
=============

PowerPool is a Python application designed to be run on Ubuntu Linux, but will
likely run in just about any Linux. If you're brave you might be able to get it
running on Window, but I wouldn't recommend it since it's untested and
unsupported.

Requirements
------------

* **Redis** - For share/block logging and hashrate recording
* **Coinserver** - PowerPool builds mining jobs by running ``getblocktemplate``
  or ``getauxblock`` on a Bitcoin Core, or bitcoin core like node. These docs
  will always refer to this as a "coinserver".
* **Miner** - To test out mining we recommend getting a cpuminer since it's
  easy to setup

Installation
------------

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

Now copy ``config.yml.example`` to ``config.yml``. Fill out all required fields
and you should be good to go for testing. 

.. code-block:: bash

    pp config.yml

And now your stratum server is (or should be...) running. Point a miner at it
on ``localhost:3333`` (or more specifically, ``stratum+tcp://localhost:3333``
and do some mining. View server health on the monitor port at
``http://localhost:3855``. Various events will be recorded into Redis in a
format that SimpleCoin is familiar with. See `Simple Coin
<https://github.com/simplecrypto/simplecoin_multi>`_ for a reference
implementation of a frontend that is compaitble with PowerPool.

Production Use
--------------

There's no official guide at this point, but some general recommendations for
new pool ops. Realize that unfortunately running a well optimized pool is
complicated, so do your reading and don't become a hidden cost for your miners
by being uneducated.

* Increase the number of connections on your coinserver with ``maxconnections``
  configuration parameter. This helps you get notified of new blocks more
  quickly, leading to lower orphan rates.
* Recompile your coinserver from source with an increased
  ``MAX_OUTBOUND_CONNECTIONS`` in ``net.cpp``. This will cause blocks that you
  solve to propogate to the network more rapidly.
* Increase ``rpcthreads`` configuration on coinservers. Generally you want at
  least few threads for the frontend (simplecoin_multi), and a few threads for
  each powerpool that connects to the server. If you are running polling
  instead of push block the rpcserver can become thread starved and block
  sumits, etc might fail.
* Setup Nagios to monitor your coinservers. This will help you know when they're
  getting slow or thread starved.
* Change your ``stop-writes-on-bgsave-error`` configuration to ``no`` for
  Redis, in case you run out of disk space. However you should setup a Nagios
  to make sure this isn't a normal occurance.
* Run PowerPool with ``PYTHONOPTIMIZE=2`` enviroment variable to skip all
  debugging computations/logging.
* Use a service like Nagios or Sensu to monitor your Stratum server ports with
  the ``check_stratum.py`` script in the contrib folder. Your miners appreicate
  good uptime.
* Use upstart or init.d to manage starting/stopping powerpool as a service.
  There is an example upstart config in the contrib folder.
* Use a firewall to block public access to your debugging port (``3855`` by
  default..), since it contains sensative information.
* Read and understand the config.yml.example. It should be thoroughly commented
  and up to date, and if it's not open a ticket for us.
