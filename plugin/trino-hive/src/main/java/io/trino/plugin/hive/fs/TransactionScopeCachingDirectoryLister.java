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
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Caches directory content (including listings that were started concurrently).
 * {@link TransactionScopeCachingDirectoryLister} assumes that all listings
 * are performed by same user within single transaction, therefore any failure can
 * be shared between concurrent listings.
 */
public class TransactionScopeCachingDirectoryLister
        implements DirectoryLister
{
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Cache<Path, FetchingValueHolder> cache;
    private final DirectoryLister delegate;

    public TransactionScopeCachingDirectoryLister(DirectoryLister delegate, long maxFileStatuses)
    {
        EvictableCacheBuilder<Path, FetchingValueHolder> cacheBuilder = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxFileStatuses)
                .weigher((key, value) -> value.getCachedFilesSize());
        this.cache = cacheBuilder.build();
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        FetchingValueHolder cachedValueHolder;
        try {
            cachedValueHolder = cache.get(path, () -> new FetchingValueHolder(delegate.list(fs, table, path)));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable throwable = e.getCause();
            throwIfInstanceOf(throwable, IOException.class);
            throwIfUnchecked(throwable);
            throw new RuntimeException("Failed to list directory: " + path, throwable);
        }

        if (cachedValueHolder.isFullyCached()) {
            return new SimpleRemoteIterator(cachedValueHolder.getCachedFiles());
        }

        return cachingRemoteIterator(cachedValueHolder, path);
    }

    @Override
    public void invalidate(Table table)
    {
        if (isLocationPresent(table.getStorage())) {
            if (table.getPartitionColumns().isEmpty()) {
                cache.invalidate(new Path(table.getStorage().getLocation()));
            }
            else {
                // a partitioned table can have multiple paths in cache
                cache.invalidateAll();
            }
        }
        delegate.invalidate(table);
    }

    @Override
    public void invalidate(Partition partition)
    {
        if (isLocationPresent(partition.getStorage())) {
            cache.invalidate(new Path(partition.getStorage().getLocation()));
        }
        delegate.invalidate(partition);
    }

    private RemoteIterator<LocatedFileStatus> cachingRemoteIterator(FetchingValueHolder cachedValueHolder, Path path)
    {
        return new RemoteIterator<>()
        {
            private int fileIndex;

            @Override
            public boolean hasNext()
                    throws IOException
            {
                try {
                    boolean hasNext = cachedValueHolder.getCachedFile(fileIndex).isPresent();
                    // Update cache weight of cachedValueHolder for a given path.
                    // The cachedValueHolder acts as an invalidation guard. If a cache invalidation happens while this iterator goes over
                    // the files from the specified path, the eventually outdated file listing will not be added anymore to the cache.
                    cache.asMap().replace(path, cachedValueHolder, cachedValueHolder);
                    return hasNext;
                }
                catch (Exception exception) {
                    // invalidate cached value to force retry of directory listing
                    cache.invalidate(path);
                    throw exception;
                }
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                // force cache entry weight update in case next file is cached
                checkState(hasNext());
                return cachedValueHolder.getCachedFile(fileIndex++).orElseThrow();
            }
        };
    }

    @VisibleForTesting
    boolean isCached(Path path)
    {
        FetchingValueHolder cached = cache.getIfPresent(path);
        return cached != null && cached.isFullyCached();
    }

    private static boolean isLocationPresent(Storage storage)
    {
        // Some Hive table types (e.g.: views) do not have a storage location
        return storage.getOptionalLocation().isPresent() && isNotEmpty(storage.getLocation());
    }

    private static class FetchingValueHolder
    {
        private final List<LocatedFileStatus> cachedFiles = synchronizedList(new ArrayList<>());
        @GuardedBy("this")
        @Nullable
        private RemoteIterator<LocatedFileStatus> fileIterator;
        @GuardedBy("this")
        @Nullable
        private Exception exception;

        public FetchingValueHolder(RemoteIterator<LocatedFileStatus> fileIterator)
        {
            this.fileIterator = requireNonNull(fileIterator, "fileIterator is null");
        }

        public synchronized boolean isFullyCached()
        {
            return fileIterator == null && exception == null;
        }

        public int getCachedFilesSize()
        {
            return cachedFiles.size();
        }

        public Iterator<LocatedFileStatus> getCachedFiles()
        {
            checkState(isFullyCached());
            return cachedFiles.iterator();
        }

        public Optional<LocatedFileStatus> getCachedFile(int index)
                throws IOException
        {
            int filesSize = cachedFiles.size();
            checkArgument(index >= 0 && index <= filesSize, "File index (%s) out of bounds [0, %s]", index, filesSize);

            // avoid fileIterator synchronization (and thus blocking) for already cached files
            if (index < filesSize) {
                return Optional.of(cachedFiles.get(index));
            }

            return fetchNextCachedFile(index);
        }

        private synchronized Optional<LocatedFileStatus> fetchNextCachedFile(int index)
                throws IOException
        {
            if (exception != null) {
                throw new IOException("Exception while listing directory", exception);
            }

            if (index < cachedFiles.size()) {
                // file was fetched concurrently
                return Optional.of(cachedFiles.get(index));
            }

            try {
                if (fileIterator == null || !fileIterator.hasNext()) {
                    // no more files
                    fileIterator = null;
                    return Optional.empty();
                }

                LocatedFileStatus fileStatus = fileIterator.next();
                cachedFiles.add(fileStatus);
                return Optional.of(fileStatus);
            }
            catch (Exception exception) {
                fileIterator = null;
                this.exception = exception;
                throw exception;
            }
        }
    }
}
