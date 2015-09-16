Getting Setup
=============

The only external service PowerPool relies on in is Redis.

.. code-block:: bash

    sudo apt-get install redis-server

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

Now copy ``config.yml.example`` to ``config.yml``. Fill out all required fields
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

Adding a new currency
=====================

Adding a new algorithm
----------------------

Algorithm configuration is defined on the core "PowerPool" component and is
then made available to all other components.

* *key name* - the name of the configuration block, or "key name", is
  arbitrary, and is just an identifier for the hashing algorithm to be used
  elsewhere in the configuration.
* *module* - this is a dotted python import path for the actual function to run to hash the data.
* *hashes_per_share* - this defines what a difficulty 1 share **means**. IE, how many times on average would I have to hash random data to get a share of this difficulty. Computing this is a PITA, so I won't go into it here.

Network Monitor
---------------

The network monitor component pings crypto coinservers and generates jobs that
get passed to stratum servers. The parameters are well documented in the config
example.

Stratum Server
--------------

We need to define a stratum port that our users can mine on. This is a
"StratumServer" component. The config values are well described in the example,
just ensure that the algo is correct, along with valid address versions. Make
sure the component name defined matches the stratum monitor url path defined in
simplecoin configuration.
