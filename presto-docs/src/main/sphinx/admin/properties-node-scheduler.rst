=========================
Node Scheduler Properties
=========================

``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``100``

The target value for the total number of splits that can be running for
each worker node.

Using a higher value is recommended, if queries are submitted in large batches
(e.g., running a large group of reports periodically), or for connectors that
produce many splits that complete quickly. Increasing this value may improve
query latency, by ensuring that the workers have enough splits to keep them
fully utilized.

Setting this too high wastes memory and may result in lower performance
due to splits not being balanced across workers. Ideally, it should be set
such that there is always at least one split waiting to be processed, but
not higher.

``node-scheduler.max-pending-splits-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``10``

The number of outstanding splits that can be queued for each worker node
for a single stage of a query, even when the node is already at the limit for
total number of splits. Allowing a minimum number of splits per stage is
required to prevent starvation and deadlocks.

This value must be smaller than ``node-scheduler.max-splits-per-node``,
is usually increased for the same reasons, and has similar drawbacks
if set too high.

``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Minimum value:** ``1``
* **Default value:** ``10``

The minimum number of candidate nodes that are evaluated by the
node scheduler when choosing the target node for a split. Setting
this value too low may prevent splits from being properly balanced
across all worker nodes. Setting it too high may increase query
latency and increase CPU usage on the coordinator.

``node-scheduler.policy``
^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Allowed values:** ``uniform``, ``topology``
* **Default value:** ``uniform``

Sets the node scheduler policy to use when scheduling splits. ``uniform``  attempts
to schedule splits on the host where the data is located, while maintaining a uniform
distribution across all hosts. ``topology`` tries to schedule splits according to
the topology distance between nodes and splits. It is recommended to use ``uniform``
for clusters where distributed storage runs on the same nodes as Presto workers.

``node-scheduler.network-topology.segments``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``machine``

A comma-separated string describing the meaning of each segment of a network location.
For example, setting ``region,rack,machine`` means a network location contains three segments.

``node-scheduler.network-topology.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Allowed values:** ``flat``, ``file``
* **Default value:** ``flat``

Sets the network topology type. To use this option, ``node-scheduler.policy`` must be set to
``topology``. ``flat`` has only one segment, with one value for each machine.
``file`` loads the topology from a file as described below.

``node-scheduler.network-topology.file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``

Load the network topology from a file. To use this option, ``node-scheduler.network-topology.type``
must be set to ``file``. Each line contains a mapping between a host name and a
network location, separated by whitespace. Network location must begin with a leading
``/`` and segments are separated by a ``/``.

.. code-block:: none

    192.168.0.1 /region1/rack1/machine1
    192.168.0.2 /region1/rack1/machine2
    hdfs01.example.com /region2/rack2/machine3

``node-scheduler.network-topology.refresh-period``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``duration``
* **Minimum value:** ``1ms``
* **Default value:** ``5m``

Controls how often the network topology file is reloaded.  To use this option,
``node-scheduler.network-topology.type`` must be set to ``file``.
