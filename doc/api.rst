Components
=========================

Component Base
------------------------
.. autoclass:: powerpool.lib.Component
    :members:
    :undoc-members:
    :private-members:

PowerPool (manager)
------------------------
.. autoclass:: powerpool.main.PowerPool
    :members:
    :undoc-members:
    :private-members:

Stratum Server
------------------------
Handles spawning one or many stratum servers (which bind to a single port
each), as well as spawning corresponding agent servers as well. It holds data
structures that allow lookup of all StratumClient objects.

.. autoclass:: powerpool.stratum_server.StratumServer
    :members:
    :undoc-members:
    :private-members:

.. autoclass:: powerpool.stratum_server.StratumClient
    :members:
    :undoc-members:
    :private-members:

Jobmanager
------------------------
This module generates mining jobs and sends them to workers. It must provide
current jobs for the stratum server to be able to push. The reference
implementation monitors an RPC daemon server.

.. autoclass:: powerpool.jobmanagers.monitor_aux_network.MonitorAuxNetwork
    :members:
    :undoc-members:
    :private-members:

.. autoclass:: powerpool.jobmanagers.monitor_network.MonitorNetwork
    :members:
    :undoc-members:
    :private-members:


Reporters
-------------------------
The reporter is responsible for transmitting shares, mining statistics, and new
blocks to some external storage. The reference implementation is the
CeleryReporter which aggregates shares into batches and logs them in a way
designed to interface with SimpleCoin. The reporter is also responsible for
tracking share rates for vardiff. This makes sense if you want vardiff to be
based off the shares per second of an entire address, instead of a single
connection.

.. autoclass:: powerpool.reporters.base.Reporter
    :members:
    :undoc-members:
    :private-members:

.. autoclass:: powerpool.reporters.redis_reporter.RedisReporter
    :members:
    :undoc-members:
    :private-members:

Monitor
------------------------
.. autoclass:: powerpool.monitor.ServerMonitor
    :members:
    :undoc-members:
    :private-members:
