===================
Hive Object Caching
===================

Querying object storage with the Hive connector is a very common use case for
Presto, that often involves the transfer of large amounts of data. The objects
are retrieved from HDFS, or any other supported object storage, by multiple
workers and processed on these workers.

Repeated queries with different parameters, or even different queries from
different users, often access, and therefore transfer, the same objects. The
Hive object caching can provide significant performance benefits, by avoiding
the repeated network transfers and instead accessing copies of the objects from
a local cache.

.. warning::
  Hive object caching is currently available as beta release only.

.. contents::
  :local:

Architecture
------------

The Hive object caching provides a read-through cache. After first retrieval
from storage by any query, objects are cached in the local cache storage on the
workers. Objects are cached on local storage of each worker and managed by a
bookkeeper component. The cache chunks are 1MB in size and are well suited for
ORC or Parquet format objects.

Configuration
-------------

The caching feature is part of the :doc:`Hive connector <./hive>` and can be
activated in the catalog properties file:

.. code-block:: none

    connector.name=hive-hadoop2
    hive.cache.enabled=true
    hive.cache.location=/opt/hive-cache

The cache operates on the coordinator and all workers accessing the object
storage. The used networking ports for the managing bookkeeper and the data
transfer, by default 8898 and 8899, need to be available.

.. list-table:: Object Cache Configuration Parameters
  :widths: 15, 80, 5
  :header-rows: 1

  * - Property
    - Description
    - Default
  * - ``hive.cache.enabled``
    - Toggle to enable or disable the Hive object cache extension
    - ``false``
  * - ``hive.cache.location``
    - Required directory location to use for the cache storage on each worker.
      Fast cache performance can be achieved with a RAM disk used as in-memory
      cache, or with high performance SSD disk usage. Storage should be local to
      on each coordinator and worker node. The directory needs to exist before
      Presto starts.
    -
  * - ``hive.cache.data-transfer-port``
    -  The TCP/IP port used to transfer data managed by the cache.
    - ``8898``
  * - ``hive.cache.bookkeeper-port``
    -  The TCP/IP port used by the bookkeeper managing the cache.
    - ``8899``
  * - ``hive.cache.read-mode``
    - Operational mode for the cache when reading data from the source and
      managing the cache.

      The ``read-through`` configuration causes all requests to be fulfilled by
      the cache. Objects not yet in the cache, cause a download of the data into
      the cache, and then a response to Presto with the data.

      The second option, ``async``, means that Presto receives the data from the
      storage directly upon requests and in parallel a request to cache the data
      is submitted to the cache manager. The cache loads the requested data
      asynchronously every 10s.
    - ``read-through``
