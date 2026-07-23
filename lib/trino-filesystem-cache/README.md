# Native file system cache design

This module contains Trino's native file system cache implementation. It is a
small local-file cache for `TrinoFileSystemCache`, intended to replace the
Alluxio-backed cache without turning Trino into a cache storage project.

The cache stores page-aligned chunks as deterministic files under configured
local cache directories. It uses no global index, no database, and no storage
engine. Restart recovery comes from the on-disk layout plus a background scan.

## Goals

- Preserve the existing Trino file system cache contract.
- Keep the read path small and predictable.
- Keep the cache's CPU, memory, filesystem metadata, and background IO burden
  low enough that Trino query processing remains the primary consumer of worker
  resources.
- Store cached data as ordinary local files.
- Bound byte usage and local file count per cache directory.
- Avoid per-read filesystem metadata updates.
- Survive process restart without a persistent index.
- Keep cache failures from becoming query correctness failures.
- Make the implementation understandable to maintainers who are not cache
  specialists.

## Non-goals

- Perfect LRU eviction.
- Exact per-chunk access tracking.
- A global persistent cache index.
- Multi-process sharing of one cache directory.
- Support for network or shared filesystems as cache storage.
- A packed cache file format.

## Behavior Preserved

The native cache keeps the important behavior and deployment model of the
existing file system cache:

- A worker can use multiple local cache directories, normally one directory per
  local disk.
- Each cache directory has its own size limit. Operators can configure explicit
  sizes with `fs.cache.max-sizes`, or derive per-directory limits from
  `fs.cache.max-disk-usage-percentages`.
- Cache directories are assumed to be roughly the same size for placement
  purposes. The cache still supports different configured limits per directory,
  but it does not try to perform weighted placement across uneven disks.
- Each cache directory is managed independently. A full directory evicts from
  that directory; it does not borrow capacity from another directory.
- Each cache directory is owned by one Trino process and one cache
  configuration. It is not shared with other processes or with another cache
  instance using different connector/cache settings.
- Cache identity remains delegated to `CacheKeyProvider`.
- Source reads are aligned to cache page boundaries before filling the cache.
- The default page size remains `1MB`.
- Tail chunks and small files may be smaller than the page size.
- The default TTL remains `7d`.
- TTL remains a cleanup policy for cached data, not a query correctness
  boundary. The existing Alluxio cache receives the TTL as a cache-manager
  eviction setting; Trino does not synchronously remove cached entries exactly
  at the TTL boundary.
- Cache read failures are treated as misses where possible.
- Cache write failures skip the cache write and continue serving the source
  read.
- Split affinity remains separate from the cache storage format.

## Behavior Changed

The implementation changes from an Alluxio `CacheManager` to Trino-owned local
files and Trino-owned eviction:

- Cache entries are stored in a Trino-defined versioned directory layout.
- Eviction is approximate and sample-based.
- Eviction candidates are whole file groups, not individual page files.
- Recent access is tracked at file granularity with rotating Bloom filters.
- LRU-like preference is limited to the configured recent-access window.
- Files outside the recent-access window are treated as equally cold unless
  they are older than the TTL.
- Native TTL cleanup is performed by startup scan, maintenance scan, and
  pressure eviction.
- A file-count limit prevents small-file-heavy workloads from growing local file
  count without bound.

The low-resource requirement drives the approximate design. The cache uses
file-level access tracking, rotating Bloom filters, bounded eviction samples,
and low-rate maintenance instead of exact LRU, per-page access timestamps, or a
persistent cache index. False positives and imperfect ordering are acceptable
because they reduce cache precision, not query correctness.

## Storage Assumptions

The cache is intended for local flash storage. Cloud workers commonly expose
several local NVMe devices, so each configured cache directory is treated as an
independent cache device with independent limits, accounting, maintenance, and
eviction.

Operators should not place cache directories on network filesystems, shared
filesystems, root partitions, or directories shared by multiple Trino
processes. The implementation assumes:

```text
one cache directory is owned by one Trino process
```

The filesystem must support atomic moves within a cache directory. The cache
verifies this on startup and refuses to use a cache directory where an atomic
move cannot be performed. The filesystem should also tolerate a large number of
files and should be provisioned with enough inodes for the configured
file-count limit.

## Cache Identity

The cache receives a file-level cache key from `CacheKeyProvider`. The native
cache hashes that key before using it in a local path:

```text
fileHash = SHA-256(cacheKey)
```

The source location is also hashed:

```text
locationHash = SHA-256(location.toString())
```

`cacheKey` can be long and may contain characters unsuitable for paths. Hashing
keeps the layout compact and path-safe. SHA-256 collisions are ignored for this
cache purpose.

## Directory Layout

The on-disk layout is versioned:

```text
<cache-dir>/v1/data/<locationHash[0..2]>/<locationHash[2..4]>/<locationHash>/<fileHash>/<page>.cache
<cache-dir>/v1/data/<locationHash[0..2]>/<locationHash[2..4]>/<locationHash>/<fileHash>/<page>.tmp.<uuid>
<cache-dir>/v1/access/<bucketStartMillis>.bloom
```

Page names are fixed-width hexadecimal page indexes:

```text
0000000000000000.cache
0000000000000001.cache
0000000000000002.cache
```

All cached pages for one source file version live under one `fileHash`
directory. That directory is the file group, and it is the unit of pressure
eviction and TTL expiration.

## Cache Directory Placement

Each source location maps to one configured cache directory:

```text
directoryIndex = hash(location.toString()).hashCode() mod directoryCount
```

This intentionally simple placement assumes configured cache directories are
backed by roughly equal local disks. Operators can still configure different
limits per directory, but the placement algorithm does not weight new cache
entries by those limits.

Reads for a location use only the selected directory. Expiration tries every
configured cache directory, so it remains conservative if directory placement
changes in a future version.

When `fs.cache.max-disk-usage-percentages` is configured, each percentage is
converted to an absolute byte limit using the filesystem containing the
corresponding cache directory. This preserves per-directory limit behavior and
allows different configured limits per disk.

## Read Path

The native cache follows the same broad read shape as the existing cache:

1. Compute page indexes from file position and configured page size.
2. Try to read requested bytes from local page files.
3. At the first miss, align the remaining read to page boundaries.
4. Read the aligned range from the source file.
5. Write page files for the aligned range.
6. Copy the requested bytes into the caller's buffer.

`TrinoInput` positioned reads continue to use positioned source reads. For
`TrinoInputStream`, the cache keeps a source stream and seeks it when necessary,
so sequential misses do not reopen the source for every page.

For cached stream reads, a one-page in-memory buffer avoids repeated local file
reads for small sequential reads inside the same cached page.

## Writes

Cache writes are opportunistic:

```text
write <page>.tmp.<uuid>
move to <page>.cache
```

The temporary file is created in the same directory as the final page file. The
final move does not replace an existing page. If another query wins the race and
creates the same page first, the losing writer deletes its temporary file and
the read still succeeds from the source data it already fetched.

The implementation accepts duplicate source reads during concurrent fills. It
does not keep an in-memory in-flight map for the first version.

## Startup Scan And Accounting

The cache has no persistent index. Each cache directory keeps in-memory counters
for:

```text
currentBytes
currentFiles
```

After startup, each cache directory is scanned asynchronously. The scan rebuilds
byte and file counters, removes stale temporary files, and deletes file groups
older than the TTL.

Queries can use the cache while the initial scan is running. Writes made during
the scan are tracked in memory so the final scan result does not double count
pages written by the current process after the scanner has already passed their
file group.

If eviction cannot find candidates even though accounting says the cache is
full, the cache performs another scan to reconcile accounting before giving up
on the write.

## Maintenance

Each cache directory has a single maintenance executor. It starts the initial
scan and then runs periodic maintenance. Background tasks catch
`RuntimeException` and `Error`, log the failure, and continue scheduling future
maintenance.

Maintenance does low-rate filesystem cleanup:

- delete stale temporary files
- sample file groups
- delete TTL-expired file groups
- improve accounting over time

Maintenance is not in the query read path and does not need to cover the whole
cache quickly. On a very large cache it may take many hours or longer for
maintenance to naturally walk all file groups. Correctness does not depend on
that scan being complete.

## Access Tracking

Access tracking is often one of the most resource-intensive parts of a cache.
Traditional designs may keep large exact in-memory structures, maintain an
on-disk LRU index, or update filesystem metadata on every hit. Those approaches
make eviction more precise, but they also add CPU, memory, disk IO, or metadata
write load to the Trino worker.

The native cache instead keeps access tracking deliberately small. It tracks
recent access by time bucket and uses probabilistic Bloom filters inside those
buckets. The goal is not to know the exact last access time for every cached
page. The goal is to cheaply answer whether a file was probably accessed in the
recent window, and approximately how recent that access was.

The cache does not update filesystem `mtime` on hits. That would create random
metadata writes proportional to cache hits. Each cache directory has a
`BloomFilterAccessTracker`. On every cache hit, the cache records the file hash:

```text
currentBucket.put(fileHash)
```

The access record is file-level, not page-level. A hit for any cached page marks
the source file version as recently used.

The tracker keeps rotating Bloom filter buckets. Defaults are:

```text
fs.cache.access-history-duration = 24h
fs.cache.access-bucket-duration = 10m
fs.cache.access-tracking-memory = 256MB
```

Memory is divided across buckets. If a bucket receives more distinct file hashes
than expected, the Bloom filter false-positive rate rises. False positives only
protect some cold files longer than necessary; they do not affect correctness.

On bucket rotation, the old bucket is persisted under `v1/access` with a
temporary file and atomic move. On restart, recent persisted buckets are loaded
back into memory. The current mutable bucket is not a write-ahead log; accesses
since the last rotation can be lost on process crash.

When eviction asks about a file hash, the tracker searches buckets from newest
to oldest and returns the newest matching bucket start time. Files absent from
all buckets are treated as colder than files present in any bucket.

## Eviction

Eviction is per cache directory. If one directory is over its byte or file-count
limit, the cache evicts from that directory only.

Eviction starts when a new write would exceed either configured limit:

```text
currentBytes + newPageBytes > maxBytes
currentFiles + 1 > maxFiles
```

Once eviction starts, it collects down to a low watermark:

```text
lowWatermark = 90% of max
```

This prevents the cache from hovering at the hard limit and doing tiny eviction
work on every write.

This is the pressure path. A separate background maintenance task periodically
sweeps the cache directory, deletes expired file groups, and reconciles
accounting. Depending on cache churn and system load, that sweep can keep usage
below the high watermark most of the time, so pressure eviction may run
infrequently.

Pressure eviction samples file groups instead of scanning the whole cache:

```text
sample = next 8192 file groups after evictionCursor
if end is reached, wrap and continue from the beginning
```

The cursor is a path cursor into the lexicographic directory walk. It is not
persisted. Resetting it on restart is acceptable.

The sampled file groups are sorted by:

1. expired groups first
2. groups absent from all recent Bloom filters
3. groups present in older Bloom buckets
4. groups present in newer Bloom buckets

Eviction deletes whole file groups until the pending write would leave both byte
and file counters at or below the low watermark. If the sample is exhausted and
the low watermark has not been reached, the cache samples again from the next
cursor position.

The fixed sample size is a deliberate resource bound, and it has an important
consequence. If the sampled file groups are physically smaller than the amount
that must be collected between the high limit and the low watermark, the cache
will delete the whole sample and move to the next sample. In that case, the
Bloom-filter sort order does little work because every candidate in the sample
is removed. The directory walk may look sequential in the code, but file groups
are stored under SHA-256 hashes, so a full-sample delete is effectively deleting
a random hash-distributed slice of the cache.

This is an acceptable degradation for the first implementation. It keeps
eviction work bounded and avoids global indexing or full-cache scans. Larger
sample sizes improve the chance that the sort order matters; smaller sample
sizes reduce eviction CPU and filesystem work but more often degrade to random
hash-slice deletion under pressure. If this behavior is too random in practice,
the simplest improvement is increasing the sample size. A more selective
follow-up could use a two-pass approach, such as first sampling enough file
groups to cover the high-to-low collection target and then sorting that larger
candidate set before deleting.

If no candidates can be deleted, accounting is reconciled with a scan. If the
write still cannot fit, the cache skips the cache write.

## Expiration

`expire(Location)` deletes the deterministic location directory:

```text
<cache-dir>/v1/data/<locationHash[0..2]>/<locationHash[2..4]>/<locationHash>
```

The cache tries that location path in every configured cache directory. This
requires no global index and no reverse lookup from cache key to path.

## Configuration

Existing cache configuration remains central:

```text
fs.cache.enabled
fs.cache.type
fs.cache.directories
fs.cache.max-sizes
fs.cache.max-disk-usage-percentages
fs.cache.ttl
fs.cache.page-size
fs.cache.preferred-hosts-count
```

Native-cache-specific guardrails:

```text
fs.cache.max-files-per-directory
fs.cache.access-history-duration
fs.cache.access-bucket-duration
fs.cache.access-tracking-memory
```

Defaults:

```text
fs.cache.page-size = 1MB
fs.cache.ttl = 7d
fs.cache.max-files-per-directory = 10000000
fs.cache.access-history-duration = 24h
fs.cache.access-bucket-duration = 10m
fs.cache.access-tracking-memory = 256MB
```

## Failure Behavior

The source filesystem remains authoritative. The cache should degrade rather
than fail queries when possible:

- missing cache pages are misses
- malformed or short cache pages are deleted and treated as misses
- failed cache writes are skipped
- full cache directories skip writes when eviction cannot make space
- stale temporary files are cleaned asynchronously
- expiration and maintenance cleanup are best effort

User-visible failures should come from the source filesystem or from cache
configuration/health problems, not from ordinary cache misses.

## Observability

The cache records JMX stats for reads, writes, skipped writes, cached bytes,
cached files, eviction, expiration, stale temporary cleanup, and access records.

Tracing spans preserve cache observability around:

- cached reads
- external reads
- cache writes

Spans include the cache key, source location, read or write position, and size
where available.
