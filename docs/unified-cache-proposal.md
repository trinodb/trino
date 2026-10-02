# Unified Tiered Cache Management for Trino

## Problem Statement

Trino's caching is fragmented across two disconnected systems:

1. **In-memory metadata caching** — 20+ independent Guava `LoadingCache` instances spread across connectors (Hive metastore, JDBC, Delta Lake, Iceberg, BigQuery). Each connector creates and manages its own caches. The engine has zero visibility, cannot enforce memory limits, and cannot invalidate caches on catalog lifecycle events.

2. **Filesystem data caching** — A partially-unified `TrinoFileSystemCache` interface with `MemoryFileSystemCache` (heap) and `AlluxioFileSystemCache` (disk) implementations. Better structured but disconnected from metadata caching and invisible to the engine's resource management.

This fragmentation causes:
- **No global memory control** — each cache independently sized, collective OOM risk
- **No engine management** — cannot invalidate/drop caches on catalog reload or removal
- **No unified observability** — no single view of all caches, hit rates, memory usage
- **No extensibility** — proprietary cache backends require forking connectors
- **No tiering** — metadata caches are memory-only; no path to disk or distributed caching

## Proposed Solution

Introduce a **unified cache management system** where:
- The **engine** owns cache infrastructure and provides it to connectors via `ConnectorContext`
- **Connectors** declare what they need (name, size, TTL, priority, tiers) and receive managed cache instances
- A **pluggable `CacheManager`** handles storage, tiering, eviction, and lifecycle
- A **global memory budget** prevents OOM by evicting low-priority entries across all catalogs
- **Namespace isolation** ensures each catalog's caches are independent

## Architecture

```
                    ┌─────────────────────────────────┐
                    │       CacheManagerRegistry       │
                    │  (loads from plugin or uses       │
                    │   DefaultCacheManager)            │
                    └────────────┬────────────────────┘
                                 │
                    ┌────────────▼────────────────────┐
                    │      CacheManager (SPI)          │
                    │  Global memory budget             │
                    │  Priority-based eviction          │
                    │  Catalog namespace management     │
                    └────────────┬────────────────────┘
                                 │
              ┌──────────────────┼──────────────────────┐
              │                  │                       │
   ┌──────────▼──────┐ ┌────────▼────────┐ ┌───────────▼──────┐
   │  Catalog "hive"  │ │ Catalog "iceberg"│ │ Catalog "pg"     │
   │  CacheFactory    │ │  CacheFactory    │ │  CacheFactory    │
   │                  │ │                  │ │                  │
   │ tables (KV,MEM)  │ │ manifests(FILE)  │ │ schemas (KV,MEM) │
   │ partitions(KV)   │ │ metadata (KV)    │ │ tables (KV,MEM)  │
   │ files (FILE,DISK)│ │ files (FILE,DISK)│ │ columns (KV,MEM) │
   └──────────────────┘ └─────────────────┘ └──────────────────┘
```

## SPI Changes

### New package: `io.trino.spi.cache`

All new types live in a dedicated SPI package. No Guava or third-party types leak into the SPI.

### Cache tiers and priority

```java
public enum CacheTier {
    MEMORY,   // Heap-based, fastest, bounded by global budget
    DISK,     // Local SSD/disk, page-based
    REMOTE    // Distributed (Redis, Hazelcast, proprietary)
}

public enum CachePriority {
    LOW,       // First to be evicted under memory pressure
    NORMAL,    // Default priority
    HIGH,      // Evicted only after LOW and NORMAL
    CRITICAL   // Last resort eviction only
}
```

### CacheFactory — connector entry point

Provided to connectors via `ConnectorContext`. Automatically namespaced per catalog.

```java
public interface CacheFactory
{
    /**
     * Create a key-value cache builder. The cacheName must be stable and descriptive
     * (e.g., "metastore-tables", "column-stats"). The engine automatically namespaces
     * by catalog identity.
     */
    <K, V> CacheBuilder<K, V> cacheBuilder(String cacheName, Class<K> keyType, Class<V> valueType);

    /**
     * Create a file data cache builder for caching file contents (byte ranges, streams).
     */
    FileCacheBuilder fileCacheBuilder(String cacheName);
}
```

### CacheBuilder — key-value cache configuration

```java
public interface CacheBuilder<K, V>
{
    /** Maximum number of entries. Mutually exclusive with maximumWeight. */
    CacheBuilder<K, V> maximumSize(long maximumSize);

    /** Maximum total weight in bytes. Requires weigher(). */
    CacheBuilder<K, V> maximumWeight(long maximumWeight);

    /** Computes entry weight in bytes. Required for maximumWeight and global budget participation. */
    CacheBuilder<K, V> weigher(CacheWeigher<K, V> weigher);

    /** Entries expire this long after being written. */
    CacheBuilder<K, V> expireAfterWrite(Duration duration);

    /** Entries are asynchronously refreshed after this duration. */
    CacheBuilder<K, V> refreshAfterWrite(Duration duration);

    /** Eviction priority under global memory pressure. Default: NORMAL. */
    CacheBuilder<K, V> priority(CachePriority priority);

    /** Which storage tiers to use. Default: MEMORY only. */
    CacheBuilder<K, V> tiers(CacheTier first, CacheTier... rest);

    /** Serializers for disk/remote tiers. Required if tiers include DISK or REMOTE. */
    CacheBuilder<K, V> serializer(CacheSerializer<K> keySerializer, CacheSerializer<V> valueSerializer);

    /** Enable cache statistics collection. */
    CacheBuilder<K, V> recordStats();

    /** Build a manually-populated cache. */
    ConnectorCache<K, V> build();

    /** Build a cache that automatically loads missing entries. */
    ConnectorLoadingCache<K, V> build(CacheLoader<K, V> loader);
}
```

### ConnectorCache — cache instance used by connectors

```java
public interface ConnectorCache<K, V>
{
    V getIfPresent(K key);
    V get(K key, CacheLoader<K, V> loader);
    void put(K key, V value);
    void invalidate(K key);
    void invalidateAll();
    long size();
    CacheStats stats();
}

public interface ConnectorLoadingCache<K, V> extends ConnectorCache<K, V>
{
    /** Get using the configured loader. */
    V get(K key);

    /** Force reload of a specific key. */
    void refresh(K key);
}
```

### ConnectorFileCache — file data cache (absorbs TrinoFileSystemCache)

```java
public interface ConnectorFileCache
{
    TrinoInput cacheInput(TrinoInputFile delegate, String key) throws IOException;
    TrinoInputStream cacheStream(TrinoInputFile delegate, String key) throws IOException;
    long cacheLength(TrinoInputFile delegate, String key) throws IOException;
    void expire(Location location) throws IOException;
    void expire(Collection<Location> locations) throws IOException;
}
```

This has the same shape as the current `TrinoFileSystemCache` interface but is now managed by the engine — participating in global budget, priority eviction, and unified metrics.

### FileCacheBuilder

```java
public interface FileCacheBuilder
{
    FileCacheBuilder tiers(CacheTier first, CacheTier... rest);
    FileCacheBuilder priority(CachePriority priority);
    FileCacheBuilder recordStats();
    ConnectorFileCache build();
}
```

### Supporting types

```java
@FunctionalInterface
public interface CacheWeigher<K, V> {
    /** Returns the weight of an entry in bytes. */
    long weigh(K key, V value);
}

@FunctionalInterface
public interface CacheLoader<K, V> {
    V load(K key) throws Exception;
}

public interface CacheSerializer<T> {
    byte[] serialize(T value);
    T deserialize(byte[] bytes);
}

public record CacheStats(
    long hitCount, long missCount,
    long loadCount, long loadFailureCount,
    long evictionCount, long size, long weightedSize) {}
```

### ConnectorContext change (backward compatible)

```java
// Added to existing ConnectorContext interface
default CacheFactory getCacheFactory()
{
    throw new UnsupportedOperationException();
}
```

Follows the established pattern of every other method in `ConnectorContext`.

### CacheManager — pluggable engine-level SPI

Not visible to connectors. The engine uses this to implement the `CacheFactory` instances it hands to connectors.

```java
public interface CacheManagerFactory
{
    String getName();
    CacheManager create(Map<String, String> config, CacheManagerContext context);
}

public interface CacheManager
{
    <K, V> ManagedCache<K, V> createCache(CacheDescriptor<K, V> descriptor);
    ManagedFileCache createFileCache(FileCacheDescriptor descriptor);

    /** All managed caches across all catalogs. */
    Collection<CacheInfo> getCaches();

    /** Invalidate all entries in all caches for a catalog. */
    void invalidateCatalog(String catalogNamespace);

    /** Invalidate a specific named cache. */
    void invalidateCache(String catalogNamespace, String cacheName);

    /** Drop all caches for a catalog (called on catalog removal). */
    void dropCatalog(String catalogNamespace);

    /** Current global memory usage across all caches. */
    long getGlobalMemoryUsage();

    /** Configured global memory limit. */
    long getGlobalMemoryLimit();

    void shutdown();
}

public interface CacheManagerContext
{
    OpenTelemetry getOpenTelemetry();
    NodeManager getNodeManager();
}
```

### Plugin registration (backward compatible)

```java
// Added to existing Plugin interface
default Iterable<CacheManagerFactory> getCacheManagerFactories()
{
    return emptyList();
}
```

Follows the `ExchangeManagerFactory` / `SpoolingManagerFactory` pattern.

## Engine-Side Implementation

### CacheManagerRegistry

Follows the `ExchangeManagerRegistry` pattern exactly:
- Reads `etc/cache-manager.properties`
- Key property: `cache-manager.name=default`
- Loads the named `CacheManagerFactory` from registered plugins
- Creates a singleton `CacheManager` instance
- Provides `@PreDestroy shutdown()` lifecycle

### DefaultCacheManager

Built-in implementation (`name = "default"`):

- **Global memory budget**: configured via `cache-manager.memory.max-size` property
- **Priority-based eviction**: when budget exceeded, evicts entries starting with lowest priority, then oldest TTL, then least-recently-used
- **Memory tier**: wraps `EvictableCacheBuilder` from existing `lib/trino-cache` with eviction listeners for global budget tracking
- **Disk tier**: initially `UnsupportedOperationException` (future work)
- **Remote tier**: initially `UnsupportedOperationException` (provided by proprietary plugins)
- **File cache**: wraps existing `MemoryFileSystemCache` / `AlluxioFileSystemCache`

### CatalogCacheFactory

Created per catalog by `DefaultCatalogFactory`:
- Implements `CacheFactory`
- Holds `CacheManager` reference + `catalogNamespace` (from `CatalogName`)
- All caches automatically namespaced

### Wiring

1. `DefaultCatalogFactory` injects `CacheManagerRegistry`, creates `CatalogCacheFactory` per catalog
2. `ConnectorContextInstance` gains `CacheFactory` field
3. `CatalogConnector.shutdown()` calls `cacheManager.dropCatalog(namespace)`
4. `PluginManager` registers `CacheManagerFactory` instances from plugins

## Observability

### System table: `system.runtime.caches`

| Column | Type | Description |
|--------|------|-------------|
| catalog_name | varchar | Catalog namespace |
| cache_name | varchar | Cache identifier |
| cache_type | varchar | "key-value" or "file" |
| tiers | varchar | e.g. "MEMORY,DISK" |
| priority | varchar | LOW/NORMAL/HIGH/CRITICAL |
| size | bigint | Entry count |
| weighted_size | bigint | Bytes |
| hit_count | bigint | Total hits |
| miss_count | bigint | Total misses |
| hit_rate | double | Hit ratio |
| eviction_count | bigint | Total evictions |

### JMX

- Global: `trino.cache:name=CacheManager` with `globalMemoryUsage`, `globalMemoryLimit`, `totalCacheCount`
- Per-cache: `trino.cache:catalog=<name>,cache=<cacheName>` with hit/miss/size/eviction metrics

## Configuration

### Engine-level: `etc/cache-manager.properties`

```properties
cache-manager.name=default
cache-manager.memory.max-size=2GB
# Future:
# cache-manager.disk.data-directory=/var/trino/cache
# cache-manager.disk.max-size=100GB
```

### Connector-level: unchanged

Connectors keep their existing config properties (`hive.metastore-cache-ttl`, `metadata.cache-ttl`, etc.). These drive the `CacheBuilder` calls. The connector decides TTL, max size, priority, and which tiers to request.

## Migration Path

Phase 1 is SPI-only. Subsequent phases migrate connectors one at a time:

1. **CachingJdbcClient** (6 caches) — best first candidate, self-contained
2. **CachingHiveMetastore** (7+ caches) — highest impact, shared by Hive/Iceberg/Delta
3. **TransactionLogAccess** (Delta Lake) — weight-based snapshot cache
4. **CachingDirectoryLister** (Hive) — file status cache
5. **TrinoHiveCatalog** (Iceberg) — per-query table metadata

Per-transaction caches (e.g., `CachingHiveMetastore.createPerTransactionCache()`) stay connector-internal — they are ephemeral and don't need engine management.

File system cache migration happens last: `CacheFileSystem` moves to `ConnectorFileCache` from `CacheFactory`, and `TrinoFileSystemCache` is deprecated.
