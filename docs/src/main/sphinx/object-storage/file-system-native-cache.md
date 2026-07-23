# Native file system cache

Trino can cache data read through supported file systems on local storage
attached to each coordinator and worker. The native cache implementation stores
file data as fixed-size chunks in local directories managed by Trino, without
requiring an Alluxio cluster or Alluxio cache manager.

Enable the native cache with the following catalog properties:

```properties
fs.cache.enabled=true
fs.cache.type=NATIVE
fs.cache.directories=/mnt/cache0,/mnt/cache1
fs.cache.max-sizes=1TB,1TB
```

The native cache is local to each Trino node and each catalog. When multiple
catalogs use caching, configure different cache directories for each catalog.

## Behavior

The native cache stores cached data in chunks. A cached source file can have
many chunk files, and the final chunk can be smaller than the configured chunk
size. The cache survives Trino process restart because the chunk files and recent
access tracking files are stored on disk.

On startup, Trino scans each cache directory in the background to rebuild cache
accounting and clean stale temporary files. Each cache directory has its own
background task, so nodes with multiple local disks scan and maintain those
directories independently. Reads can use existing cached chunks while this scan
runs, and new cache writes are allowed. Eviction based on configured cache limits
starts after accounting is known for the directory. Cache writes are skipped when
the local file system reports too little usable space for the chunk.

Eviction is approximate and per cache directory. Trino tracks recent file access
with bounded-memory Bloom filters and uses this recent-access history when
choosing eviction candidates. When a directory is under pressure, Trino samples
cached file groups, prefers expired and not-recently-accessed groups, and deletes
all cached chunks for a selected file group. This is intentionally simpler than
tracking a precise LRU order for every cached chunk.

The cache also has low-rate per-directory maintenance tasks that clean stale
temporary files and remove expired cached file groups on a best-effort basis.

## Storage recommendations

Use dedicated local flash storage for cache directories. The cache stores many
files and assumes the local file system handles large directory trees
efficiently. Avoid shared storage, network file systems, and the operating
system root volume. The local file system must support atomic moves within each
cache directory.

On cloud instances with multiple local disks, configure one cache directory per
disk and assign a matching size or disk usage percentage for each directory.

## Configuration

Use the properties from the following table in your catalog properties file.

:::{list-table} Native file system cache configuration properties
:widths: 30, 70
:header-rows: 1

* - Property
  - Description
* - `fs.cache.enabled`
  - Enable file system caching. Defaults to `false`.
* - `fs.cache.type`
  - Cache implementation to use. Set to `NATIVE` for the native file system
    cache.
* - `fs.cache.directories`
  - Required, comma-separated list of absolute paths to cache directories.
    The paths, or their closest existing parents, must be readable and writable
    on all Trino nodes that use the catalog.
* - `fs.cache.max-sizes`
  - Comma-separated list of maximum [data sizes](prop-type-data-size), one for
    each cache directory. Configure either `fs.cache.max-sizes` or
    `fs.cache.max-disk-usage-percentages`.
* - `fs.cache.max-disk-usage-percentages`
  - Comma-separated list of maximum percentages of total disk size, one for each
    cache directory. Each value is an integer between 0 and 100. Configure either
    `fs.cache.max-sizes` or `fs.cache.max-disk-usage-percentages`.
* - `fs.cache.page-size`
  - Cached chunk [data size](prop-type-data-size). Defaults to `1MB`. Values
    must be between `64kB` and `15MB`.
* - `fs.cache.ttl`
  - Maximum [duration](prop-type-duration) cached file groups are kept before
    they are eligible for best-effort eviction. Defaults to `7d`. A value of
    `0s` makes cached file groups immediately eligible for eviction.
* - `fs.cache.max-files-per-directory`
  - Maximum number of cached chunk files in each cache directory. Defaults to
    `10000000`.
* - `fs.cache.access-history-duration`
  - Duration of approximate recent-access history used by eviction. Defaults to
    `24h`.
* - `fs.cache.access-bucket-duration`
  - Duration of each recent-access tracking bucket. Defaults to `10m`.
* - `fs.cache.access-tracking-memory`
  - Maximum memory used for approximate recent-access tracking across cache
    directories for the catalog. Defaults to `256MB`.
* - `fs.cache.preferred-hosts-count`
  - Number of preferred nodes for scheduling splits that read the same file.
    Defaults to `2`. More information in
    [](fs-cache-distributed).
:::

For multiple cache directories, the values of `fs.cache.directories` and the
chosen size limit property must have the same number of entries in the same
order.

## Monitoring

The native cache exposes JMX metrics from
`io.trino.filesystem.cache.local.NativeFileSystemCacheStats`, including external
reads, cache reads, cache writes, skipped writes, cached bytes, cached files,
evictions, expired files, stale temporary files, and access records.

The cache code uses [OpenTelemetry tracing](/admin/opentelemetry).
