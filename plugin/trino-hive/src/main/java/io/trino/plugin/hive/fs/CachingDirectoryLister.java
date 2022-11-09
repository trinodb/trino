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
import io.airlift.units.Duration;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;

public class CachingDirectoryLister
        implements DirectoryLister
{
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Cache<DirectoryListingCacheKey, ValueHolder> cache;
    private final List<SchemaTablePrefix> tablePrefixes;

    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(), hiveClientConfig.getFileStatusCacheMaxSize(), hiveClientConfig.getFileStatusCacheTables());
    }

    public CachingDirectoryLister(Duration expireAfterWrite, long maxSize, List<String> tables)
    {
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<DirectoryListingCacheKey, ValueHolder>) (key, value) -> value.files.map(List::size).orElse(1))
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
    public RemoteIterator<TrinoFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        if (!isCacheEnabledFor(table.getSchemaTableName())) {
            return new TrinoFileStatusRemoteIterator(fs.listLocatedStatus(path));
        }

        return listInternal(fs, new DirectoryListingCacheKey(path, false));
    }

    @Override
    public RemoteIterator<TrinoFileStatus> listFilesRecursively(FileSystem fs, Table table, Path path)
            throws IOException
    {
        if (!isCacheEnabledFor(table.getSchemaTableName())) {
            return new TrinoFileStatusRemoteIterator(fs.listFiles(path, true));
        }

        return listInternal(fs, new DirectoryListingCacheKey(path, true));
    }

    private RemoteIterator<TrinoFileStatus> listInternal(FileSystem fs, DirectoryListingCacheKey cacheKey)
            throws IOException
    {
        ValueHolder cachedValueHolder = uncheckedCacheGet(cache, cacheKey, ValueHolder::new);
        if (cachedValueHolder.getFiles().isPresent()) {
            return new SimpleRemoteIterator(cachedValueHolder.getFiles().get().iterator());
        }

        return cachingRemoteIterator(cachedValueHolder, createListingRemoteIterator(fs, cacheKey), cacheKey);
    }

    private static RemoteIterator<TrinoFileStatus> createListingRemoteIterator(FileSystem fs, DirectoryListingCacheKey cacheKey)
            throws IOException
    {
        if (cacheKey.isRecursiveFilesOnly()) {
            return new TrinoFileStatusRemoteIterator(fs.listFiles(cacheKey.getPath(), true));
        }
        return new TrinoFileStatusRemoteIterator(fs.listLocatedStatus(cacheKey.getPath()));
    }

    @Override
    public void invalidate(Table table)
    {
        if (isCacheEnabledFor(table.getSchemaTableName()) && isLocationPresent(table.getStorage())) {
            if (table.getPartitionColumns().isEmpty()) {
                cache.invalidateAll(DirectoryListingCacheKey.allKeysWithPath(new Path(table.getStorage().getLocation())));
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
            cache.invalidateAll(DirectoryListingCacheKey.allKeysWithPath(new Path(partition.getStorage().getLocation())));
        }
    }

    private RemoteIterator<TrinoFileStatus> cachingRemoteIterator(ValueHolder cachedValueHolder, RemoteIterator<TrinoFileStatus> iterator, DirectoryListingCacheKey key)
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
                    cache.asMap().replace(key, cachedValueHolder, new ValueHolder(files));
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
    boolean isCached(Path path)
    {
        return isCached(new DirectoryListingCacheKey(path, false));
    }

    @VisibleForTesting
    boolean isCached(DirectoryListingCacheKey cacheKey)
    {
        ValueHolder cached = cache.getIfPresent(cacheKey);
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
    }
}
