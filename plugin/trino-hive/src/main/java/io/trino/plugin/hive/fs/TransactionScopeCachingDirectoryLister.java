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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.requireNonNull;

/**
 * Caches directory content (including listings that were started concurrently).
 * {@link TransactionScopeCachingDirectoryLister} assumes that all listings
 * are performed by same user within single transaction, therefore any failure can
 * be shared between concurrent listings.
 */
public class TransactionScopeCachingDirectoryLister
        implements DirectoryLister
{
    private final long transactionId;
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder> cache;
    private final DirectoryLister delegate;

    public TransactionScopeCachingDirectoryLister(DirectoryLister delegate, long transactionId, Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder> cache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.transactionId = transactionId;
        this.cache = requireNonNull(cache, "cache is null");
    }

    @Override
    public RemoteIterator<TrinoFileStatus> listFilesRecursively(TrinoFileSystem fs, Table table, Location location)
            throws IOException
    {
        return listInternal(fs, table, new TransactionDirectoryListingCacheKey(transactionId, location));
    }

    private RemoteIterator<TrinoFileStatus> listInternal(TrinoFileSystem fs, Table table, TransactionDirectoryListingCacheKey cacheKey)
            throws IOException
    {
        FetchingValueHolder cachedValueHolder;
        try {
            cachedValueHolder = cache.get(cacheKey, () -> new FetchingValueHolder(createListingRemoteIterator(fs, table, cacheKey)));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable throwable = e.getCause();
            throwIfInstanceOf(throwable, IOException.class);
            throwIfUnchecked(throwable);
            throw new RuntimeException("Failed to list directory: " + cacheKey.getPath(), throwable);
        }

        if (cachedValueHolder.isFullyCached()) {
            return new SimpleRemoteIterator(cachedValueHolder.getCachedFiles());
        }

        return cachingRemoteIterator(cachedValueHolder, cacheKey);
    }

    private RemoteIterator<TrinoFileStatus> createListingRemoteIterator(TrinoFileSystem fs, Table table, TransactionDirectoryListingCacheKey cacheKey)
            throws IOException
    {
        return delegate.listFilesRecursively(fs, table, cacheKey.getPath());
    }

    @Override
    public void invalidate(Table table)
    {
        if (isLocationPresent(table.getStorage())) {
            if (table.getPartitionColumns().isEmpty()) {
                cache.invalidate(new TransactionDirectoryListingCacheKey(transactionId, Location.of(table.getStorage().getLocation())));
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
            cache.invalidate(new TransactionDirectoryListingCacheKey(transactionId, Location.of(partition.getStorage().getLocation())));
        }
        delegate.invalidate(partition);
    }

    private RemoteIterator<TrinoFileStatus> cachingRemoteIterator(FetchingValueHolder cachedValueHolder, TransactionDirectoryListingCacheKey cacheKey)
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
                    cache.asMap().replace(cacheKey, cachedValueHolder, cachedValueHolder);
                    return hasNext;
                }
                catch (Exception exception) {
                    // invalidate cached value to force retry of directory listing
                    cache.invalidate(cacheKey);
                    throw exception;
                }
            }

            @Override
            public TrinoFileStatus next()
                    throws IOException
            {
                // force cache entry weight update in case next file is cached
                checkState(hasNext());
                return cachedValueHolder.getCachedFile(fileIndex++).orElseThrow();
            }
        };
    }

    @VisibleForTesting
    boolean isCached(Location location)
    {
        return isCached(new TransactionDirectoryListingCacheKey(transactionId, location));
    }

    @VisibleForTesting
    boolean isCached(TransactionDirectoryListingCacheKey cacheKey)
    {
        FetchingValueHolder cached = cache.getIfPresent(cacheKey);
        return cached != null && cached.isFullyCached();
    }

    private static boolean isLocationPresent(Storage storage)
    {
        // Some Hive table types (e.g.: views) do not have a storage location
        return storage.getOptionalLocation().isPresent() && !storage.getLocation().isEmpty();
    }

    static class FetchingValueHolder
    {
        private static final long ATOMIC_LONG_SIZE = instanceSize(AtomicLong.class);
        private static final long INSTANCE_SIZE = instanceSize(FetchingValueHolder.class);

        private final List<TrinoFileStatus> cachedFiles = synchronizedList(new ArrayList<>());
        private final AtomicLong cachedFilesSize = new AtomicLong();
        @GuardedBy("this")
        @Nullable
        private RemoteIterator<TrinoFileStatus> fileIterator;
        @GuardedBy("this")
        @Nullable
        private Exception exception;

        public FetchingValueHolder(RemoteIterator<TrinoFileStatus> fileIterator)
        {
            this.fileIterator = requireNonNull(fileIterator, "fileIterator is null");
        }

        public synchronized boolean isFullyCached()
        {
            return fileIterator == null && exception == null;
        }

        public long getRetainedSizeInBytes()
        {
            // ignore fileIterator and exception as they are ephemeral
            return INSTANCE_SIZE + ATOMIC_LONG_SIZE + sizeOfObjectArray(cachedFiles.size()) + cachedFilesSize.get();
        }

        public Iterator<TrinoFileStatus> getCachedFiles()
        {
            checkState(isFullyCached());
            return cachedFiles.iterator();
        }

        public Optional<TrinoFileStatus> getCachedFile(int index)
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

        private synchronized Optional<TrinoFileStatus> fetchNextCachedFile(int index)
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

                TrinoFileStatus fileStatus = fileIterator.next();
                cachedFiles.add(fileStatus);
                cachedFilesSize.addAndGet(fileStatus.getRetainedSizeInBytes());
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
