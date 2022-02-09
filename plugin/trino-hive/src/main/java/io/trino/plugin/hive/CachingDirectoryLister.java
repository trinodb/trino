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
package io.trino.plugin.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.collect.cache.NonKeyEvictableCache;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.SchemaTablePrefix;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;

public class CachingDirectoryLister
        implements DirectoryLister
{
    // TODO (https://github.com/trinodb/trino/issues/10621) this cache lacks invalidation
    private final NonKeyEvictableCache<Path, List<LocatedFileStatus>> cache;
    private final List<SchemaTablePrefix> tablePrefixes;

    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(), hiveClientConfig.getFileStatusCacheMaxSize(), hiveClientConfig.getFileStatusCacheTables());
    }

    public CachingDirectoryLister(Duration expireAfterWrite, long maxSize, List<String> tables)
    {
        this.cache = buildNonEvictableCacheWithWeakInvalidateAll(CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<Path, List<LocatedFileStatus>>) (key, value) -> value.size())
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .recordStats());
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
    public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        List<LocatedFileStatus> files = cache.getIfPresent(path);
        if (files != null) {
            return simpleRemoteIterator(files);
        }
        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(path);

        if (tablePrefixes.stream().noneMatch(prefix -> prefix.matches(table.getSchemaTableName()))) {
            return iterator;
        }
        return cachingRemoteIterator(iterator, path);
    }

    private RemoteIterator<LocatedFileStatus> cachingRemoteIterator(RemoteIterator<LocatedFileStatus> iterator, Path path)
    {
        return new RemoteIterator<>()
        {
            private final List<LocatedFileStatus> files = new ArrayList<>();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    cache.put(path, ImmutableList.copyOf(files));
                }
                return hasNext;
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                LocatedFileStatus next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }

    private static RemoteIterator<LocatedFileStatus> simpleRemoteIterator(List<LocatedFileStatus> files)
    {
        return new RemoteIterator<>()
        {
            private final Iterator<LocatedFileStatus> iterator = ImmutableList.copyOf(files).iterator();

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next()
            {
                return iterator.next();
            }
        };
    }

    @Managed
    public void flushCache()
    {
        // Note: this may not invalidate ongoing loads (https://github.com/trinodb/trino/issues/10512, https://github.com/google/guava/issues/1881)
        // This is acceptable, since this operation is invoked manually, and not relied upon for correctness.
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
}
