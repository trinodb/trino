==============================
Hive connector storage caching
==============================

Querying object storage with the :doc:`/connector/hive` is a
very common use case for Trino. It often involves the transfer of large amounts
of data. The objects are retrieved from HDFS, or any other supported object
storage, by multiple workers and processed on these workers. Repeated queries
with different parameters, or even different queries from different users, often
access, and therefore transfer, the same objects.

Benefits
--------

Enabling caching can result in significant benefits:

**Reduced load on object storage**

Every retrieved and cached object avoids repeated retrieval from the storage in
subsequent queries. As a result the object storage system does not have to
provide the object again and again.

For example, if your query accesses 100MB of objects from the storage, the first
time the query runs 100MB are downloaded and cached. Any following query uses
these objects. If your users run another 100 queries accessing the same objects,
your storage system does not have to do any significant work. Without caching it
has to provide the same objects again and again, resulting in 10GB of total
storage to serve.

This reduced load on the object storage can also impact the sizing, and
therefore the cost, of the object storage system.

**Increased query performance**

Caching can provide significant performance benefits, by avoiding the repeated
network transfers and instead accessing copies of the objects from a local
cache. Performance gains are more significant if the performance of directly
accessing the object storage is low compared to accessing the cache.

For example, if you access object storage in a different network, different data
center or even different cloud-provider region query performance is slow. Adding
caching using fast, local storage has a significant impact and makes your
queries much faster.

On the other hand, if your object storage is already running at very high
performance for I/O and network access, and your local cache storage is at
similar speeds, or even slower, performance benefits can be minimal.

**Reduced query costs**

A result of the reduced load on the object storage, mentioned earlier, is
significantly reduced network traffic. Network traffic however is a considerable
cost factor in an setup, specifically also when hosted in public cloud provider
systems.

Architecture
------------

Caching can operate in two modes. The default async mode provides the queried
data directly and caches any objects asynchronously afterwards. Any following
queries requesting the cached objects are served directly from the cache.

The other mode is a read-through cache. After first retrieval from storage by
any query, objects are cached in the local cache storage on the workers.

In both modes objects are cached on local storage of each worker and managed by
a BookKeeper component. Workers can request cached objects from other workers to
avoid requests from the object storage.

The cache chunks are 1MB in size and are well suited for ORC or Parquet file
formats.

Configuration
-------------

The caching feature is part of the :doc:`/connector/hive` and
can be activated in the catalog properties file:

.. code-block:: text

    connector.name=hive
    hive.cache.enabled=true
    hive.cache.location=/opt/hive-cache

The cache operates on the coordinator and all workers accessing the object
storage. The used networking ports for the managing BookKeeper and the data
transfer, by default 8898 and 8899, need to be available.

To use caching on multiple catalogs, you need to configure different caching
directories  and different BookKeeper and data-transfer ports.

.. list-table:: **Cache Configuration Parameters**
  :widths: 15, 80, 5
  :header-rows: 1

  * - Property
    - Description
    - Default
  * - ``hive.cache.enabled``
    - Toggle to enable or disable caching
    - ``false``
  * - ``hive.cache.location``
    - Required directory location to use for the cache storage on each worker.
      Separate multiple directories, which can be mount points for separate drives, with commas
      ``hive.cache.location=/var/lib/trino/cache1,/var/lib/trino/cache2``.
      More tips can be found in the :ref:`recommendations
      <hive-cache-recommendations>`.
    -
  * - ``hive.cache.data-transfer-port``
    -  The TCP/IP port used to transfer data managed by the cache.
    - ``8898``
  * - ``hive.cache.bookkeeper-port``
    -  The TCP/IP port used by the BookKeeper managing the cache.
    - ``8899``
  * - ``hive.cache.read-mode``
    - Operational mode for the cache as described earlier in the architecture
      section. ``async`` and ``read-through`` are the supported.
    - ``async``
  * - ``hive.cache.ttl``
    - Time to live for objects in the cache. Objects, which have not been
      requested for the TTL value, are removed from the cache.
    - ``7d``
  * - ``hive.cache.disk-usage-percentage``
    - Percentage of disk space used for cached data
    - 80

.. _hive-cache-recommendations:

Recommendations
---------------

The speed of the local cache storage is crucial to the performance of the cache.
The most common and cost efficient approach is to attach high performance SSD
disk or equivalents. Fast cache performance can be also be achieved with a RAM
disk used as in-memory.

In all cases, you should avoid using the root partition and disk of the node and
instead attach at multiple dedicated storage devices for the cache on each node.
The cache uses the disk up to a configurable percentage. Storage should be local
on each coordinator and worker node. The directory needs to exist before Trino
starts. We recommend using multiple devices to improve performance of the cache.

The capacity of the attached storage devices should be about 20-30% larger than
the size of the queried object storage workload. For example, your current query
workload typically accesses partitions in your HDFS storage that encapsulate
data for the last 3 months. The overall size of these partitions is currently at
1TB. As a result your cache drives have to have a total capacity of 1.2 TB or
more.

Your deployment method for Trino decides how to create the directory for
caching. Typically you need to connect a fast storage system, like an SSD drive,
and ensure that is it mounted on the configured path. Kubernetes, CFT and other
systems allow this via volumes.

Object storage systems
----------------------

The following object storage systems are tested:

* HDFS
* :doc:`Amazon S3 and S3-compatible systems <hive-s3>`
* :doc:`Azure storage systems <hive-azure>`
* Google Cloud Storage

Metrics
-------

In order to verify how caching works on your system you can take multiple
approaches:

* Inspect the disk usage on the cache storage drives on all nodes
* Query the metrics of the caching system exposed by JMX

The implementation of the cache exposes a `number of metrics
<https://rubix.readthedocs.io/en/latest/metrics.html>`_ via JMX. You can
:doc:`inspect these and other metrics directly in Trino with the JMX connector
or in external tools </admin/jmx>`.

Basic caching statistics for the catalog are available in the
``jmx.current."rubix:catalog=<catalog_name>,name=stats"`` table.
The table ``jmx.current."rubix:catalog=<catalog_name>,type=detailed,name=stats``
contains more detailed statistics.

The following example query returns the average cache hit ratio for the ``hive`` catalog:

.. code-block:: sql

  SELECT avg(cache_hit)
  FROM jmx.current."rubix:catalog=hive,name=stats"
  WHERE NOT is_nan(cache_hit);

Limitations
-----------

Caching does not support user impersonation and cannot be used with HDFS secured by Kerberos.
It does not take any user-specific access rights to the object storage into account.
The cached objects are simply transparent binary blobs to the caching system and full
access to all content is available.
