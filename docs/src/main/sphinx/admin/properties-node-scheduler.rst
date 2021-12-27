=========================
Node scheduler properties
=========================

Splits
------

``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
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

* **Type:** :ref:`prop-type-integer`
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

* **Type:** :ref:`prop-type-integer`
* **Minimum value:** ``1``
* **Default value:** ``10``

The minimum number of candidate nodes that are evaluated by the
node scheduler when choosing the target node for a split. Setting
this value too low may prevent splits from being properly balanced
across all worker nodes. Setting it too high may increase query
latency and increase CPU usage on the coordinator.

``node-scheduler.policy``
^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Allowed values:** ``uniform``, ``topology``
* **Default value:** ``uniform``

Sets the node scheduler policy to use when scheduling splits. ``uniform``  attempts
to schedule splits on the host where the data is located, while maintaining a uniform
distribution across all hosts. ``topology`` tries to schedule splits according to
the topology distance between nodes and splits. It is recommended to use ``uniform``
for clusters where distributed storage runs on the same nodes as Trino workers.

Network topology
----------------

``node-scheduler.network-topology.segments``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Default value:** ``machine``

A comma-separated string describing the meaning of each segment of a network location.
For example, setting ``region,rack,machine`` means a network location contains three segments.

``node-scheduler.network-topology.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Allowed values:** ``flat``, ``file``, ``subnet``
* **Default value:** ``flat``

Sets the network topology type. To use this option, ``node-scheduler.policy``
must be set to ``topology``.

- ``flat``: the topology has only one segment, with one value for each machine.
- ``file``: the topology is loaded from a file using the properties
  ``node-scheduler.network-topology.file`` and
  ``node-scheduler.network-topology.refresh-period`` described in the
  following sections.
- ``subnet``: the topology is derived based on subnet configuration provided
  through properties ``node-scheduler.network-topology.subnet.cidr-prefix-lengths``
  and ``node-scheduler.network-topology.subnet.ip-address-protocol`` described
  in the following sections.

File based network topology
---------------------------

``node-scheduler.network-topology.file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`

Load the network topology from a file. To use this option, ``node-scheduler.network-topology.type``
must be set to ``file``. Each line contains a mapping between a host name and a
network location, separated by whitespace. Network location must begin with a leading
``/`` and segments are separated by a ``/``.

.. code-block:: text

    192.168.0.1 /region1/rack1/machine1
    192.168.0.2 /region1/rack1/machine2
    hdfs01.example.com /region2/rack2/machine3

``node-scheduler.network-topology.refresh-period``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Minimum value:** ``1ms``
* **Default value:** ``5m``

Controls how often the network topology file is reloaded.  To use this option,
``node-scheduler.network-topology.type`` must be set to ``file``.

Subnet based network topology
-----------------------------

``node-scheduler.network-topology.subnet.ip-address-protocol``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **Allowed values:** ``IPv4``, ``IPv6``
* **Default value:** ``IPv4``

Sets the IP address protocol to be used for computing subnet based
topology.  To use this option, ``node-scheduler.network-topology.type`` must
be set to ``subnet``.

``node-scheduler.network-topology.subnet.cidr-prefix-lengths``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A comma-separated list of :ref:`prop-type-integer` values defining CIDR prefix
lengths for subnet masks. The prefix lengths must be in increasing order. The
maximum prefix length values for IPv4 and IPv6 protocols are 32 and 128
respectively. To use this option, ``node-scheduler.network-topology.type`` must
be set to ``subnet``.

For example, the value ``24,25,27`` for this property with IPv4 protocol means
that masks applied on the IP address to compute location segments are
``255.255.255.0``, ``255.255.255.128`` and ``255.255.255.224``. So the segments
created for an address ``192.168.0.172`` are ``[192.168.0.0, 192.168.0.128,
192.168.0.160, 192.168.0.172]``.
