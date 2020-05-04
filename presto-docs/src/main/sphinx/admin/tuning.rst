=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you, if your cluster is facing a specific performance problem.

Config Properties
-----------------

See :doc:`/admin/properties`.

JVM Settings
------------

The following can be helpful for diagnosing garbage collection (GC) issues:

.. code-block:: none

    -Xlog:gc*,safepoint::time,level,tags,tid
