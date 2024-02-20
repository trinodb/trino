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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CachingDirectoryLister
        implements DirectoryLister
{
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Cache<Location, ValueHolder> cache;
    private final List<SchemaTablePrefix> tablePrefixes;

    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(), hiveClientConfig.getFileStatusCacheMaxRetainedSize(), hiveClientConfig.getFileStatusCacheTables());
    }

    public CachingDirectoryLister(Duration expireAfterWrite, DataSize maxSize, List<String> tables)
    {
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<Location, ValueHolder>) (key, value) -> toIntExact(estimatedSizeOf(key.toString()) + value.getRetainedSizeInBytes()))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.tablePrefixes = tables.stream()
                .map(CachingDirectoryLister::parseTableName)
                .collect(toImmutableList());
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
            return new TrinoFileStatusRemoteIterator(fs.listFiles(location));
        }

        return listInternal(fs, location);
    }

    private RemoteIterator<TrinoFileStatus> listInternal(TrinoFileSystem fs, Location location)
            throws IOException
    {
        ValueHolder cachedValueHolder = uncheckedCacheGet(cache, location, ValueHolder::new);
        if (cachedValueHolder.getFiles().isPresent()) {
            return new SimpleRemoteIterator(cachedValueHolder.getFiles().get().iterator());
        }

        return cachingRemoteIterator(cachedValueHolder, createListingRemoteIterator(fs, location), location);
    }

    private static RemoteIterator<TrinoFileStatus> createListingRemoteIterator(TrinoFileSystem fs, Location location)
            throws IOException
    {
        return new TrinoFileStatusRemoteIterator(fs.listFiles(location));
    }

    @Override
    public void invalidate(Table table)
    {
        if (isCacheEnabledFor(table.getSchemaTableName()) && isLocationPresent(table.getStorage())) {
            if (table.getPartitionColumns().isEmpty()) {
                cache.invalidate(Location.of(table.getStorage().getLocation()));
            }
            else {
                // a partitioned table can have multiple paths in cache
                cache.invalidateAll();
            }
        }
    }

    @Override
    public void invalidate(Partition partition)
    {
        if (isCacheEnabledFor(partition.getSchemaTableName()) && isLocationPresent(partition.getStorage())) {
            cache.invalidate(Location.of(partition.getStorage().getLocation()));
        }
    }

    private RemoteIterator<TrinoFileStatus> cachingRemoteIterator(ValueHolder cachedValueHolder, RemoteIterator<TrinoFileStatus> iterator, Location location)
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
                    cache.asMap().replace(location, cachedValueHolder, new ValueHolder(files));
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

    @VisibleForTesting
    boolean isCached(Location location)
    {
        ValueHolder cached = cache.getIfPresent(location);
        return cached != null && cached.getFiles().isPresent();
    }

    private boolean isCacheEnabledFor(SchemaTableName schemaTableName)
    {
        return tablePrefixes.stream().anyMatch(prefix -> prefix.matches(schemaTableName));
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
}
