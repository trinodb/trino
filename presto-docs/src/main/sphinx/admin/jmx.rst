===================
Monitoring with JMX
===================

Presto exposes a large number of different metrics via the Java Management Extensions (JMX).

You have to enable JMX by setting the ports used by the RMI registry and server
in the :ref:`config.properties file <config_properties>`:

.. code-block:: none

    jmx.rmiregistry.port=9080
    jmx.rmiserver.port=9081

* ``jmx.rmiregistry.port``:
  Specifies the port for the JMX RMI registry. JMX clients should connect to this port.

* ``jmx.rmiserver.port``:
  Specifies the port for the JMX RMI server. Presto exports many metrics,
  that are useful for monitoring via JMX.

JConsole (supplied with the JDK), `VisualVM <https://visualvm.github.io/>`_, and
many other tools can be used to access the metrics in a client application.
Many monitoring solutions support JMX. You can also use the
:doc:`/connector/jmx` and query the metrics using SQL.

Many of these JMX metrics are a complex metric object such as a ``CounterStat``
that has a collection of related metrics. For example, ``InputPositions`` has
``InputPositions.TotalCount``, ``InputPositions.OneMinute.Count``, and so on.

A small subset of the available metrics are described below.

JVM
---

* Heap size: ``java.lang:type=Memory:HeapMemoryUsage.used``
* Thread count: ``java.lang:type=Threading:ThreadCount``

Presto Cluster and Nodes
------------------------

* Active nodes:
  ``presto.failureDetector:name=HeartbeatFailureDetector:ActiveCount``

* Free memory (general pool):
  ``presto.memory:type=ClusterMemoryPool:name=general:FreeDistributedBytes``

* Cumulative count (since Presto started) of queries that ran out of memory and were killed:
  ``presto.memory:name=ClusterMemoryManager:QueriesKilledDueToOutOfMemory``

Presto Queries
--------------

* Active queries currently executing or queued: ``presto.execution:name=QueryManager:RunningQueries``

* Queries started: ``presto.execution:name=QueryManager:StartedQueries.FiveMinute.Count``

* Failed queries from last 5 min (all): ``presto.execution:name=QueryManager:FailedQueries.FiveMinute.Count``
* Failed queries from last 5 min (internal): ``presto.execution:name=QueryManager:InternalFailures.FiveMinute.Count``
* Failed queries from last 5 min (external): ``presto.execution:name=QueryManager:ExternalFailures.FiveMinute.Count``
* Failed queries (user): ``presto.execution:name=QueryManager:UserErrorFailures.FiveMinute.Count``

* Execution latency (P50): ``presto.execution:name=QueryManager:ExecutionTime.FiveMinutes.P50``
* Input data rate (P90): ``presto.execution:name=QueryManager:WallInputBytesRate.FiveMinutes.P90``

Presto Tasks
------------

* Input data bytes: ``presto.execution:name=TaskManager:InputDataSize.FiveMinute.Count``
* Input rows: ``presto.execution:name=TaskManager:InputPositions.FiveMinute.Count``

Connectors
----------

Many connectors provide their own metrics. The metric names typically start with
``presto.plugin``.
