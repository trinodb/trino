/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Partition;
import io.trino.metastore.Storage;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveConfig;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class CachingDirectoryLister
        implements DirectoryLister
{
    private static final Logger log = Logger.get(CachingDirectoryLister.class);

    private final Cache<CacheKey, ValueHolder> cache;
    private final Predicate<SchemaTableName> tablePredicate;
    private final Predicate<FileEntry> filterPredicate;

    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(
                hiveClientConfig.getFileStatusCacheExpireAfterWrite(),
                hiveClientConfig.getFileStatusCacheMaxRetainedSize(),
                hiveClientConfig.getFileStatusCacheTables(),
                hiveClientConfig.getFileStatusCacheExcludedTables(),
                hiveClientConfig.getS3GlacierFilter().toFileEntryPredicate());
    }

    public CachingDirectoryLister(
            Duration expireAfterWrite,
            DataSize maxSize,
            List<String> includedTables,
            List<String> excludedTables,
            Predicate<FileEntry> filterPredicate)
    {
        requireNonNull(expireAfterWrite, "expireAfterWrite is null");
        requireNonNull(maxSize, "maxSize is null");
        requireNonNull(includedTables, "includedTables is null");
        requireNonNull(excludedTables, "excludedTables is null");
        requireNonNull(filterPredicate, "filterPredicate is null");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<CacheKey, ValueHolder>) (key, value) -> toIntExact(key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.tablePredicate = matches(includedTables).and(not(matches(excludedTables)));
        this.filterPredicate = filterPredicate;
    }

    private static Predicate<SchemaTableName> matches(List<String> tables)
    {
        return tables.stream()
                .map(CachingDirectoryLister::parseTableName)
                .map(prefix -> (Predicate<SchemaTableName>) prefix::matches)
                .reduce(Predicate::or)
                .orElse(_ -> false);
    }

    private static SchemaTablePrefix parseTableName(String tableName)
    {
        if (tableName.equals("*")) {
            return new SchemaTablePrefix();
        }
        String[] parts = tableName.split("\\.");
        checkArgument(parts.length == 2, "Invalid schemaTableName: %s", tableName);
        String schema = parts[0];
        String table = parts[1];
        if (table.equals("*")) {
            return new SchemaTablePrefix(schema);
        }
        return new SchemaTablePrefix(schema, table);
    }

    @Override
    public RemoteIterator<TrinoFileStatus> listFilesRecursively(TrinoFileSystem fs, Table table, Location location)
            throws IOException
    {
        if (!isCacheEnabledFor(table.getSchemaTableName())) {
            return new TrinoFileStatusRemoteIterator(fs.listFiles(location), filterPredicate);
        }

        return listInternal(fs, location, table.getSchemaTableName());
    }

    private RemoteIterator<TrinoFileStatus> listInternal(TrinoFileSystem fs, Location location, SchemaTableName schemaTableName)
            throws IOException
    {
        CacheKey cacheKey = new CacheKey(location, schemaTableName);
        ValueHolder cachedValueHolder = uncheckedCacheGet(cache, cacheKey, ValueHolder::new);
        if (cachedValueHolder.getFiles().isPresent()) {
            return new SimpleRemoteIterator(cachedValueHolder.getFiles().get().iterator());
        }

        return cachingRemoteIterator(cachedValueHolder, createListingRemoteIterator(fs, location, filterPredicate), cacheKey);
    }

    private static RemoteIterator<TrinoFileStatus> createListingRemoteIterator(TrinoFileSystem fs, Location location, Predicate<FileEntry> filterPredicate)
            throws IOException
    {
        return new TrinoFileStatusRemoteIterator(fs.listFiles(location), filterPredicate);
    }

    @Override
    public void invalidate(Location location, SchemaTableName schemaTableName)
    {
        log.debug("Invalidating cache for schemaTableName: %s and location: %s", schemaTableName, location);
        cache.invalidate(new CacheKey(location, schemaTableName));
    }

    @Override
    public void invalidate(Table table)
    {
        if (isCacheEnabledFor(table.getSchemaTableName()) && isLocationPresent(table.getStorage())) {
            if (table.getPartitionColumns().isEmpty()) {
                log.debug("Invalidating cache for unpartitioned table: %s", table.getSchemaTableName());
                cache.invalidate(new CacheKey(Location.of(table.getStorage().getLocation()), table.getSchemaTableName()));
            }
            else {
                // a partitioned table can have multiple paths in cache
                SchemaTableName tableName = table.getSchemaTableName();
                log.debug("Invalidating cache for partitioned table: %s", table.getSchemaTableName());
                cache.asMap().keySet().removeIf(key -> key.schemaTableName().equals(tableName));
            }
        }
    }

    @Override
    public void invalidate(Partition partition)
    {
        if (isCacheEnabledFor(partition.getSchemaTableName()) && isLocationPresent(partition.getStorage())) {
            log.debug("Invalidating partition cache for table: %s partition: %s", partition.getSchemaTableName(), partition.getStorage().getLocation());
            Location partitionLocation = Location.of(partition.getStorage().getLocation());
            cache.invalidate(new CacheKey(partitionLocation, partition.getSchemaTableName()));
        }
    }

    @Override
    public void invalidateAll()
    {
        log.debug("Invalidating partition cache (all)");
        cache.invalidateAll();
    }

    private RemoteIterator<TrinoFileStatus> cachingRemoteIterator(ValueHolder cachedValueHolder, RemoteIterator<TrinoFileStatus> iterator, CacheKey cacheKey)
    {
        return new RemoteIterator<>()
        {
            private final List<TrinoFileStatus> files = new ArrayList<>();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    // The cachedValueHolder acts as an invalidation guard. If a cache invalidation happens while this iterator goes over
                    // the files from the specified path, the eventually outdated file listing will not be added anymore to the cache.
                    cache.asMap().replace(cacheKey, cachedValueHolder, new ValueHolder(files));
                }
                return hasNext;
            }

            @Override
            public TrinoFileStatus next()
                    throws IOException
            {
                TrinoFileStatus next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public Double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Override
    public boolean isCached(Location location, SchemaTableName schemaTableName)
    {
        ValueHolder cached = cache.getIfPresent(new CacheKey(location, schemaTableName));
        return cached != null && cached.getFiles().isPresent();
    }

    @VisibleForTesting // for testing exclusion rules
    boolean isCacheEnabledFor(SchemaTableName schemaTableName)
    {
        return tablePredicate.test(schemaTableName);
    }

    private static boolean isLocationPresent(Storage storage)
    {
        // Some Hive table types (e.g.: views) do not have a storage location
        return storage.getOptionalLocation().isPresent() && !storage.getLocation().isEmpty();
    }

    /**
     * The class enforces intentionally object identity semantics for the value holder,
     * not value-based class semantics to correctly act as an invalidation guard in the
     * cache.
     */
    private static class ValueHolder
    {
        private static final long INSTANCE_SIZE = instanceSize(ValueHolder.class);

        private final Optional<List<TrinoFileStatus>> files;

        public ValueHolder()
        {
            files = Optional.empty();
        }

        public ValueHolder(List<TrinoFileStatus> files)
        {
            this.files = Optional.of(ImmutableList.copyOf(requireNonNull(files, "files is null")));
        }

        public Optional<List<TrinoFileStatus>> getFiles()
        {
            return files;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(files, value -> estimatedSizeOf(value, TrinoFileStatus::getRetainedSizeInBytes));
        }
    }

    private record CacheKey(Location location, SchemaTableName schemaTableName)
    {
        private static final long INSTANCE_SIZE = instanceSize(CacheKey.class);

        private CacheKey(Location location, SchemaTableName schemaTableName)
        {
            this.location = requireNonNull(location, "location is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    estimatedSizeOf(location.toString()) +
                    schemaTableName.getRetainedSizeInBytes();
        }
    }
}
