# Unified Cache Management for Trino - Implementation Plan

## Context

Trino's file system caching is currently managed per-catalog inside `FileSystemModule` via Guice. Two implementations exist: `MemoryFileSystemCache` (heap-based, coordinator-only) and `AlluxioFileSystemCache` (disk-based, all nodes). The engine has no visibility into these caches -- no global budget, no coordinated invalidation across catalogs, no unified observability.

This plan implements unified caching starting with the **file system cache** as the first consumer of the new cache SPI, then follows with key-value cache migration.

**Scope:** Trino open-source only. Result set caching is out of scope.

---

## Phase 0: Move Filesystem Types to SPI [COMPLETE]

Before the cache SPI can reference file system types, several types must move from `lib/trino-filesystem` (`io.trino.filesystem`) to `core/trino-spi` under the new package `io.trino.spi.filesystem`.

### Step 0.1: Rewrite `Location` to Remove Guava [DONE]

`Location.java` rewritten to remove all Guava dependencies (Splitter, checkArgument, checkState, getLast). Now depends only on stdlib.

### Step 0.2: Move Types to `io.trino.spi.filesystem` [DONE]

Moved these files:

| Type | Notes |
|------|-------|
| `TrinoInputStream` | Abstract class extending `InputStream`. No issues. |
| `TrinoInput` | Depends on `io.airlift.slice.Slice` (already SPI dep) and `Metrics` (already in SPI). |
| `Location` | stdlib only after rewrite. |
| `TrinoInputFile` | All dependencies co-moved. |

### Step 0.3: Update ~1,084 Import Sites [DONE]

All files importing these types updated: `io.trino.filesystem.X` → `io.trino.spi.filesystem.X`.

### Step 0.4: Update `Locations` Utility [DONE]

`removeOneTrailingSlash()` moved from `Location` to `Locations` class. `Locations` stays in `lib/trino-filesystem`.

### Step 0.5: Update `lib/trino-filesystem` pom.xml [DONE]

Dependency on `trino-spi` added.

---

## Phase 1: Cache SPI + File System Cache Integration

### Step 1.1: Cache SPI

#### Architecture

Cache management is **plugin-based**. Each cache tier (MEMORY, DISK) is provided by a separate plugin via `FileSystemCacheManagerFactory` (and, in Phase 2, `KeyValueCacheManagerFactory`). The engine-side `CacheManagerRegistry` aggregates all tier-specific cache managers and creates per-catalog `ConnectorCacheFactory` objects that route requests by tier.

Cache interfaces are **split by cache kind** — `FileSystemCacheManager` (Phase 1) and `KeyValueCacheManager` (Phase 2). This avoids forcing plugins to stub out methods for cache kinds they don't support (e.g., Alluxio disk cache is file-oriented, not KV-oriented).

Connectors can request **layered caches** — a chain of tiers tried in order (e.g., `[MEMORY, DISK]` for L1→L2). The `CacheManagerRegistry` composes per-tier caches into a `LayeredFileSystemCache` internally; plugins remain single-tier.

**Plugin flow:**
```
Plugin.getFileSystemCacheManagerFactories()
  → FileSystemCacheManagerFactory (one per tier)
    → FileSystemCacheManagerFactory.create(config, context)
      → FileSystemCacheManager (tier-specific, plugin-provided)
```

**Connector flow:**
```
ConnectorContext.getCacheFactory()
  → ConnectorCacheFactory (per-catalog, multi-tier, engine-created)
    → createFileSystemCache(ttl, MEMORY, DISK)
      → CacheManagerRegistry creates LayeredFileSystemCache
        → layer 0: FileSystemCacheManager(MEMORY).createFileSystemCache(catalog, ttl)
        → layer 1: FileSystemCacheManager(DISK).createFileSystemCache(catalog, ttl)
      → returns composed ConnectorFileSystemCache
```

#### SPI Interfaces

**`io.trino.spi.cache` package:**

```java
public enum CacheTier { MEMORY, DISK }

// Plugin-provided factory for file system caches, one per tier
public interface FileSystemCacheManagerFactory {
    String getName();
    CacheTier cacheTier();
    FileSystemCacheManager create(Map<String, String> config, CacheManagerContext context);
}

// Phase 2: plugin-provided factory for KV caches, one per tier
// public interface KeyValueCacheManagerFactory {
//     String getName();
//     CacheTier cacheTier();
//     KeyValueCacheManager create(Map<String, String> config, CacheManagerContext context);
// }

// Context passed to factory (follows ExchangeManagerContext pattern)
public interface CacheManagerContext {
    OpenTelemetry getOpenTelemetry();
    Tracer getTracer();
}

// Plugin-provided, tier-specific file system cache manager
public interface FileSystemCacheManager {
    ConnectorFileSystemCache createFileSystemCache(CatalogName catalog, Duration ttl);
    void invalidate(CatalogName catalog);
    void drop(CatalogName catalog);
    void shutdown();
}

// Phase 2: KV cache manager (separate interface so plugins opt in per cache kind)
// public interface KeyValueCacheManager {
//     <K, V> ConnectorCache<K, V> createCache(CatalogName catalog, String name, Duration ttl);
//     void invalidate(CatalogName catalog);
//     void drop(CatalogName catalog);
//     void shutdown();
// }

// Connector-facing, multi-tier, supports layered caches (passed via ConnectorContext)
public interface ConnectorCacheFactory {
    // tiers: ordered layer chain; first element is L1 (tried first), later elements fall through on miss
    ConnectorFileSystemCache createFileSystemCache(Duration ttl, CacheTier... tiers);
}

// Observability
public record CacheInfo(
    CatalogName catalog, String name, CacheTier tier,
    long size, long hitCount, long missCount, long evictionCount) {}
```

**`io.trino.spi.filesystem.cache` package:**

```java
// SPI version of existing TrinoFileSystemCache
public interface ConnectorFileSystemCache {
    TrinoInput cacheInput(TrinoInputFile delegate, String key) throws IOException;
    TrinoInputStream cacheStream(TrinoInputFile delegate, String key) throws IOException;
    long cacheLength(TrinoInputFile delegate, String key) throws IOException;
    void expire(Location location) throws IOException;
    void expire(Collection<Location> locations) throws IOException;
}
```

**SPI modifications:**

```java
// ConnectorContext
default ConnectorCacheFactory getCacheFactory() {
    throw new UnsupportedOperationException();
}

// Plugin
default Iterable<FileSystemCacheManagerFactory> getFileSystemCacheManagerFactories() {
    return List.of();
}
// Phase 2:
// default Iterable<KeyValueCacheManagerFactory> getKeyValueCacheManagerFactories() {
//     return List.of();
// }
```

#### New files

```
core/trino-spi/src/main/java/io/trino/spi/cache/
  CacheTier.java
  FileSystemCacheManager.java
  FileSystemCacheManagerFactory.java
  CacheManagerContext.java
  ConnectorCacheFactory.java
  CacheInfo.java

core/trino-spi/src/main/java/io/trino/spi/filesystem/cache/
  ConnectorFileSystemCache.java
```

### Step 1.2: CacheManagerRegistry (Engine-Side)

`CacheManagerRegistry` lives in `core/trino-main` and is the engine-side glue:

- Holds `Map<CacheTier, FileSystemCacheManager>` — one per tier loaded from plugin factories
- Creates per-catalog `ConnectorCacheFactory` instances that route/compose by tier chain:
  ```java
  // in DefaultCatalogFactory.createConnector()
  CatalogName catalog = new CatalogName(catalogName);
  ConnectorCacheFactory cacheFactory = (ttl, tiers) -> {
      if (tiers.length == 1) {
          return cacheManagerRegistry.getFileSystemCacheManager(tiers[0])
              .createFileSystemCache(catalog, ttl);
      }
      // compose into a LayeredFileSystemCache
      List<ConnectorFileSystemCache> layers = Arrays.stream(tiers)
          .map(tier -> cacheManagerRegistry.getFileSystemCacheManager(tier)
              .createFileSystemCache(catalog, ttl))
          .toList();
      return new LayeredFileSystemCache(layers);
  };
  ```
- `LayeredFileSystemCache` — engine-side composite that tries each layer in order on read, populates higher layers on lower-layer hits, propagates `expire()` to all layers
- Fans out lifecycle calls to all tier managers:
  - `invalidate(CatalogName catalog)` — calls `invalidate(catalog)` on every `FileSystemCacheManager`
  - `drop(CatalogName catalog)` — calls `drop(catalog)` on every `FileSystemCacheManager`
- `@PreDestroy shutdown()` — shuts down all cache managers
- Follows `ExchangeManagerRegistry` pattern for factory registration and config loading

**Config:** one properties file per tier, e.g.:
- `etc/cache-manager-disk.properties` — `cache-manager.name=alluxio`, plus Alluxio-specific config (`fs.cache.directories`, `fs.cache.max-sizes`, `fs.cache.page-size`)
- `etc/cache-manager-memory.properties` — `cache-manager.name=memory`, plus memory-specific config (`fs.memory-cache.max-size`)

**New files:**
```
core/trino-main/src/main/java/io/trino/cache/
  CacheManagerRegistry.java
  CacheManagerConfig.java
  CacheManagerContextInstance.java
```

**Modified files:**
- `PluginManager.java` — register `FileSystemCacheManagerFactory` instances from plugins:
  ```java
  for (FileSystemCacheManagerFactory factory : plugin.getFileSystemCacheManagerFactories()) {
      cacheManagerRegistry.addFileSystemCacheManagerFactory(factory);
  }
  ```
- `Server.java` — `cacheManagerRegistry.loadCacheManagers()` BEFORE catalogs load

### Step 1.3: Built-in FileSystemCacheManager Implementations

Move existing cache implementations behind `FileSystemCacheManagerFactory`:

**Alluxio disk cache plugin:**
- `AlluxioFileSystemCacheManagerFactory` — `getName()` returns `"alluxio"`, `cacheTier()` returns `DISK`
- `create()` returns Alluxio-based `FileSystemCacheManager` that:
  - Owns global disk cache directories (shared across all catalogs)
  - `createFileSystemCache(catalog, ttl)` — creates `AlluxioFileSystemCache` with catalog-namespaced cache identifiers
  - `invalidate(catalog)` — uses Alluxio `CacheManager.invalidate(Predicate<PageInfo>)` to invalidate pages matching the catalog's cache identifiers (tracked in `ConcurrentHashMap<CatalogName, Set<String>>`)
  - `drop(catalog)` — invalidates + removes tracking
- Replaces `AlluxioFileSystemCacheModule`

**Memory cache plugin:**
- `MemoryFileSystemCacheManagerFactory` — `getName()` returns `"memory"`, `cacheTier()` returns `MEMORY`
- `create()` returns memory-based `FileSystemCacheManager` that:
  - Owns global heap cache budget
  - `createFileSystemCache(catalog, ttl)` — creates `MemoryFileSystemCache` scoped to catalog
  - `invalidate(catalog)` / `drop(catalog)` — clears catalog's entries
- Replaces `MemoryFileSystemCacheModule`

**Alluxio namespacing strategy:**
- Current Alluxio cache uses Murmur3 hash of cache key as `cacheIdentifier` — no prefix-based grouping
- New approach: prefix cache keys with catalog name (e.g., `catalog_name/location#modified#length`) so the `cacheIdentifier` is catalog-specific
- Track `Set<cacheIdentifier>` per catalog in a `ConcurrentHashMap` for targeted invalidation via `CacheManager.invalidate(Predicate<PageInfo>)`
- `expire()` methods (currently no-op) can be implemented using this tracking

### Step 1.4: Wire Through ConnectorContext

**Current flow:**
```
FileSystemModule (per-catalog Guice)
  → installs AlluxioFileSystemCacheModule or MemoryFileSystemCacheModule
  → creates TrinoFileSystemCache in Guice
  → CacheFileSystemFactory wraps delegate
```

**Target flow:**
```
DefaultCatalogFactory.createConnector()
  → CacheManagerRegistry creates ConnectorCacheFactory (routes by tier, bakes in CatalogName)
  → passes ConnectorCacheFactory to ConnectorContextInstance
  → Connector calls ConnectorCacheFactory.createFileSystemCache(tier, ttl)
  → CacheManagerRegistry routes to CacheManager for that tier
  → CacheManager.createFileSystemCache(catalog, ttl) returns ConnectorFileSystemCache
  → CacheFileSystemFactory wraps delegate with it
```

**Changes:**

- **`ConnectorContextInstance.java`** — add `ConnectorCacheFactory` field + constructor param + getter

- **`DefaultCatalogFactory.java`** — inject `CacheManagerRegistry`. In `createConnector()`:
  - Create `ConnectorCacheFactory` via registry (closes over `CatalogName`)
  - Pass to `ConnectorContextInstance`

- **`FileSystemModule.java`** — instead of installing `AlluxioFileSystemCacheModule`/`MemoryFileSystemCacheModule`:
  - Get `ConnectorCacheFactory` from `ConnectorContext`
  - Connector decides tier (MEMORY for coordinator metadata caching, DISK for general file caching)
  - Connector decides TTL
  - Calls `connectorCacheFactory.createFileSystemCache(ttl, tiers...)` to get `ConnectorFileSystemCache`
  - `CacheFileSystemFactory` wraps delegate with this cache

- **`CacheFileSystemFactory.java`** — accept `ConnectorFileSystemCache` instead of `TrinoFileSystemCache`

- **`coordinatorFileCaching` handling:**
  - Currently: connector sets `coordinatorFileCaching=true` in `FileSystemModule` to request `MemoryFileSystemCache` on coordinator
  - New: connector calls `connectorCacheFactory.createFileSystemCache(ttl, CacheTier.MEMORY)` — explicit, no boolean flag
  - The decision of MEMORY vs DISK tier stays with the connector (e.g. Iceberg uses MEMORY tier when `isMetadataCacheEnabled()` is true)

### Step 1.5: Catalog Lifecycle Integration

- **`CoordinatorDynamicCatalogManager`** — on `dropCatalog()`, call `cacheManagerRegistry.drop(catalog)`

### Step 1.6: Observability

- **`CacheSystemTable`** (new, `core/trino-main/.../connector/system/`):
  - Schema: `system.runtime.caches`
  - Columns: `node_id`, `catalog_name`, `cache_name`, `cache_type`, `size`, `hit_count`, `miss_count`, `hit_rate`, `eviction_count`
  - Distribution: `ALL_NODES`
  - Register in `SystemConnectorModule.java`

### Step 1.7: Testing

- Unit: `TestCacheManagerRegistry`
- Integration: create catalog with file caching → `SELECT * FROM system.runtime.caches` → drop catalog → verify cleanup
- Regression: all existing file system cache tests pass

---

## Phase 2: Key-Value Cache SPI + Connector Migration

*(Deferred — to be planned after Phase 1 is complete)*

Extend SPI with cache builder, loading cache, tag extractor, etc. Migrate 30+ connector caches from `EvictableCacheBuilder`. Add dependency-based invalidation.

---

## File Change Summary

### Phase 0 — Move types to SPI [COMPLETE]

**Moved files (4):**
```
io.trino.filesystem.Location        → io.trino.spi.filesystem.Location (rewritten, no Guava)
io.trino.filesystem.TrinoInput      → io.trino.spi.filesystem.TrinoInput
io.trino.filesystem.TrinoInputFile  → io.trino.spi.filesystem.TrinoInputFile
io.trino.filesystem.TrinoInputStream → io.trino.spi.filesystem.TrinoInputStream
```

**Modified files (~1,084 import updates)**

### Phase 1 — Cache SPI + Integration

**New SPI files (7):**
```
core/trino-spi/src/main/java/io/trino/spi/cache/
  CacheTier.java, FileSystemCacheManager.java, FileSystemCacheManagerFactory.java,
  CacheManagerContext.java, ConnectorCacheFactory.java, CacheInfo.java

core/trino-spi/src/main/java/io/trino/spi/filesystem/cache/
  ConnectorFileSystemCache.java
```

**New engine files (4):**
```
core/trino-main/src/main/java/io/trino/cache/
  CacheManagerRegistry.java, CacheManagerConfig.java,
  CacheManagerContextInstance.java, LayeredFileSystemCache.java
```

**New plugin files (2):**
```
lib/trino-filesystem-cache-alluxio/.../AlluxioFileSystemCacheManagerFactory.java
lib/trino-filesystem-manager/.../MemoryFileSystemCacheManagerFactory.java (or similar location)
```

**Modified SPI files (2):**
```
core/trino-spi/.../connector/ConnectorContext.java
core/trino-spi/.../Plugin.java
```

**Modified engine files (4):**
```
core/trino-main/.../connector/ConnectorContextInstance.java
core/trino-main/.../connector/DefaultCatalogFactory.java
core/trino-main/.../server/PluginManager.java
core/trino-main/.../server/Server.java
```

**Modified filesystem files (2):**
```
lib/trino-filesystem-manager/.../FileSystemModule.java
lib/trino-filesystem/.../cache/CacheFileSystemFactory.java
```

**Removed files (2):**
```
lib/trino-filesystem-cache-alluxio/.../AlluxioFileSystemCacheModule.java
lib/trino-filesystem-manager/.../MemoryFileSystemCacheModule.java (or similar)
```
