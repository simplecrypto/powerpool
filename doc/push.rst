Setting up push block notification
==================================

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
percentages by ~0.01% - 0.1%, depending on block times. Miners often connect
with far more latency than this. The biggest reason to do this is to reduce the
rpc load on your coinservers if there are multiple powerpool instances
connected to them.

How push block works
-----------------------

Standard Bitcoin/Litecoin based coinservers have a built in config option to
allow executing a script right after a new block is discovered. We want to run
a script that notifies our PowerPool process(es) to check for a new block.

To accomplish this PowerPool has built in support for receiving a UDP datagram
on its datagram port. The basic system flow looks like this:

.. code::

    Coinserver -> Learns of new block
    Coinserver -> Executes blocknotify script (Alertblock)
    Alertblock -> Parses the passed in .push file
    Alertblock -> Sends a UDP datagram based on that .push file
    PowerPool -> Receives UDP datagram
    PowerPool -> Runs `getblocktemplate` on the Coinserver

.. note::

    Using a pushblock script to deliver a UDP datagram to PowerPool can be
    accomplished in many different ways. We're going to walk through how we've
    set it up on our own servers, but please note if your server
    configuration/architechture differs much from ours you may have to adapt
    this guide.

Open the datagram port
------------------------------

The ``datagram`` option in powerpool's config is disabled by default because
access to that port will allow users to remotely execute any command in your
powerpool instance. It must be enabled in your powerpool config for any of this
to do anything.

.. warning::

    In production the datagram port should **always** be behind a firewall, as
    it is basically root access to your mining server.

Modify the coinserver's config
------------------------------

This is the part that tells the coinserver what script to run when it learns
of a new block.

.. code-block:: bash

    blocknotify=/usr/bin/alertblock /path/to/my.push

You'll want something similar to this in each coinserver's config. Make sure to
restart it after.


Alertblock script
-----------------

Now that the coin server is trying to run ``/usr/bin/alertblock``, you'll need
to make that Alertblock script.

Open your text editor of choice and save this to ``/usr/bin/alertblock``.
You'll also need to make it executable with ``chmod +x /usr/bin/alertblock``.

.. code-block:: bash

    #!/bin/bash
    cat $1 | xargs -P 0 -d '\n' -I ARGS bash -c 'a="ARGS"; args=($a); echo "${args[@]:2}" | nc -4u -w0 -q1 ${args[@]:0:2}'
    # For testing the command
    #cat $1 | xargs -P 0 -td '\n' -I ARGS bash -xc 'a="ARGS"; args=($a); echo "${args[@]:2}" | nc -4u -w0 -q1 ${args[@]:0:2}'

.. note::

    Unfortunately netcat has a non-uniform implementation across different
    Linux platforms. Some platforms will require you to use "ncat" instead of
    "nc" in the above script.


Block .push script
------------------

Now your Alertblock script will be looking for a ``/path/to/my.push`` file. The
data in this file is interpreted by the alertblock script. It looks at each
line and tries to send a UDP packet based on the info. The .push file might
contain something like this:

.. code-block:: bash

    127.0.0.1 6855 VTC getblocktemplate signal=1 __spawn=1

The ``127.0.0.1`` and ``6855`` are the address and port to send the datagram
to. The remaining stuff are the contents of the datagram. PowerPool's datagram
format is basically:

.. code-block:: bash

    <name of component> <function to run on component> \*<positional arguments for component> \*\*<keyword arguments for components> <special flags>

The port (6855) should be the monitor port for the stratum process you want
to send the notification to. The (VTC) should match the name of the coinserver
component in the powerpool instance, normally the currency code.

If you need to push to multiple monitor ports just do something like:

.. code-block:: bash

    127.0.0.1 6855 VTC getblocktemplate signal=1 __spawn=1
    127.0.0.1 6856 VTC getblocktemplate signal=1 __spawn=1

For merge mined coins you'll want something slightly different:

.. code-block:: bash

    127.0.0.1 6855 DOGE _check_new_jobs signal=1 _single_exec=True __spawn=1

Powerpool config
----------------

Now we need to update PowerPool's config to not poll, as it is no longer
needed, and makes the coinserver's logs a lot harder to use. All that needs to
be done is set the ``poll`` key to ``False`` for each currency you have push
block setup for.

.. code-block:: python

    VTC:
        poll: False
        type: powerpool.jobmanagers.MonitorNetwork
        algo: scryptn
        currency: VTC
        etc...

Confirm it is working
---------------------

You'll want to double check push block notifications are actually working as
planned. The easiest way is to visit PowerPool's monitoring endpoint and look
for the ``last_signal`` key. It should be updated each time PowerPool is
notified of a block via push block.

.. warning::

    If the server has poll turned off and is not getting push block notifications, you will get a LOT of orphans. In the future we would have polling automatically enable on failed push block, but right now it will just not update more than every 15 seconds!

Motivations
-----------

If this whole process seems complex that's because it is. Unfortunately it
needs improvement.  The reason for all this is that we can change which
powerpool servers recieve push block notifications without needing to restart
any powerpool servers or coinservers. A hardcoded implementation is simpler to
setup, although more brittle, and requires service interruptions to add/remove
instances and coins, which we don't want.
