# File system cache

Trino accesses files directly on object storage and remote file system storage.
This often involves the transfer of large amounts of data. The files are
retrieved from HDFS, or any other supported object storage, by multiple workers
and processed on these workers. Repeated queries with different parameters, or
even different queries from different users, often access, and therefore
transfer, the same objects.

Trino includes support for caching these files with the help of the open
source [Alluxio](https://github.com/Alluxio/alluxio) libraries with catalogs
using the following connectors:

* [](/connector/delta-lake)
* [](/connector/hive)
* [](/connector/iceberg)

(fs-cache-distributed)=
## Distributed caching

File system caching is distributed in Trino as part of the mechanism of any other
query processing. Query processing, detailed more in [](/overview/concepts) is
broken up into different stages, where tasks and splits are processed by
different nodes in the cluster. The lowest level splits retrieve data from the
data source with the help of the connector of the specific catalog. For
file system caching, these splits result in the retrieval of files from object
storage.

Different nodes process splits with data from objects storage randomly over
time, but with preference for using a fixed set of nodes for a given file. If
the preferred nodes are too busy, the split, and hence the caching, takes place
on a non-preferred, less busy node. File system caching keeps copies of the
retrieved files on a local cache storage, separate for each node. Over time the
same files from object storage are cached on any nodes that require the data
file for processing a specific task. Each cache on each node is managed
separately, following the TTL and size configuration, and cached files are
evicted from the cache.

You can limit the number of hosts that are preferred to process these tasks with
`fs.cache.preferred-hosts-count`. Query processing still uses all other nodes as
required for the parallel processing of tasks, and therefore potentially caches
files on more nodes than the preferred hosts only. A low setting, such as the
default 2, can reduce the overall size of the cache because it can reduce how
often the same file is cached on multiple nodes. A higher setting, up to the
number of nodes in the cluster, distributes the workload across more workers by
default, and leads to more resilience against node failures at the expense of
effective cache size.

(fs-cache-benefits)=
## Benefits

Enabling caching can result in the following significant benefits:

**Reduced load on storage**

Every retrieved and cached file avoids repeated retrieval from the storage in
subsequent queries on the same worker. As a result the storage system does not
have to provide the file again and again.

For example, if your query accesses 100MB of files from the storage, the first
time the query runs 100MB are downloaded and cached. Any following query uses
these files. If your users run another 100 queries accessing the same files,
your storage system does not have to provide all data repeatedly. Without
caching it has to provide the same files again and again, resulting in up to
10GB of total files to serve.

**Increased query performance**

Caching can provide significant performance benefits, by avoiding the repeated
network transfers and instead accessing copies of the files from a local
cache. Performance gains are more significant if the performance of directly
accessing the storage is low compared to accessing the local cache.

For example, if you access storage in a different network, different data
center, or even different cloud-provider region query performance is slow. Adding
caching using fast, local storage has a significant impact and makes your
queries much faster.

On the other hand, if your storage is already running at very high performance
for I/O and network access, and your local cache storage is at similar speeds,
or even slower, performance benefits can be minimal.

**Reduced query costs**

A result of the reduced load on the storage, mentioned earlier, is significantly
reduced network traffic and access to storage. Network traffic and access, often
in the form of API access, are often a considerable cost factor, specifically
also when hosted in public cloud provider systems.

(fs-cache-configuration)=
## Configuration

Use the properties from the following table in your catalog properties files to
enable and configure caching for the specific catalogs.

:::{list-table} File system cache configuration properties
:widths: 25, 75
:header-rows: 1

* - Property
  - Description
* - `fs.cache.enabled`
  - Enable object storage caching. Defaults to no caching with the value `false`.
* - `fs.cache.directories`
  - Required, comma-separated list of absolute paths to directories to use for
    caching. All directories must exist on the coordinator and all workers.
    Trino must have read and write permissions for files and nested directories.
    A valid example with only one directory is `/tmp/trino-cache`.
* - `fs.cache.max-sizes`
  - Optional, comma-separated list of maximum [data sizes](prop-type-data-size)
    for each caching directory. Order of values must be identical to the
    directories list. Can not be used together with
    `fs.cache.max-disk-usage-percentages`.
* - `fs.cache.max-disk-usage-percentages`
  - Optional, comma-separated list of maximum percentage values of the used disk
    for each directory. Each value is an integer between 1 and 100. Order of
    values must be identical to the directories list. If multiple directories
    use the same disk, ensure that total percentages per drive remains below 100
    percent. Can not be used together with `fs.cache.max-sizes`.
* - `fs.cache.ttl`
  -  The maximum [duration](prop-type-duration) for objects to remain in the cache
     before eviction. Defaults to `7d`. The minimum value of `0s` means that caching
     is effectively turned off.
* - `fs.cache.preferred-hosts-count`
  - The number of preferred nodes for caching files. Defaults to 2. Processing
    identifies and subsequently prefers using specific nodes. If the preferred
    nodes identified for caching a split are unavailable or too busy, then an
    available node is chosen at random from the cluster. More information in
    [](fs-cache-distributed).
* - `fs.cache.page-size`
  - The page [data size](prop-type-data-size) used for caching data. Each transfer of files 
    uses at least this amount of data. Defaults to `1MB`. Values must be between 
    `64kB` and `15MB`. Larger value potentially result in too much data transfer 
    smaller values are less efficient since they result in more individual downloads.
:::

## Monitoring

The cache exposes the
[Alluxio JMX client metrics](https://docs.alluxio.io/ee-da/user/stable/en/reference/Metrics-List.html#client-metrics)
under the `org.alluxio` package, and metrics on external reads and cache reads under
`io.trino.filesystem.alluxio.AlluxioCacheStats`.

The cache code uses [OpenTelemetry tracing](/admin/opentelemetry).

## Recommendations

The speed of the local cache storage is crucial to the performance of the cache.
The most common and cost efficient approach is to attach high performance SSD
disk or equivalents. Fast cache performance can be also be achieved with a RAM
disk used as in-memory cache.

In all cases, avoid using the root partition and disk of the node. Instead
attach one or more dedicated storage devices for the cache on each node. Storage
should be local, dedicated on each node, and not shared.

Your deployment method for Trino decides how to attach storage and create the
directories for caching. Typically you need to connect a fast storage system,
like an SSD drive, and ensure that is it mounted on the configured path.
