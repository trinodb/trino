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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.function.Predicate.not;

public class CachingDirectoryLister
        implements DirectoryLister
{
    private static final Logger log = Logger.get(CachingDirectoryLister.class);

    // special value that producer puts to the queue when the listing is finished
    private static final Object END_OF_LISTING = new Object();

    private final Cache<CacheKey, ValueHolder> cache;
    private final Predicate<SchemaTableName> tablePredicate;
    private final Predicate<FileEntry> filterPredicate;
    private final Duration listingTimeout;
    private final Duration listingElementTimeout;
    private final int listingMaxRetries;
    private final int listingQueueCapacity;
    // not null only when timeout or retry is set. we run listing in a separate thread to be able to stop waiting on it by timeout
    @Nullable
    private final ExecutorService listingExecutor;

    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(),
                hiveClientConfig.getFileStatusCacheMaxRetainedSize(),
                hiveClientConfig.getFileStatusCacheTables(),
                hiveClientConfig.getFileStatusCacheExcludedTables(),
                hiveClientConfig.getS3GlacierFilter().toFileEntryPredicate(),
                hiveClientConfig.getFileStatusCacheListingTimeout(),
                hiveClientConfig.getFileStatusCacheListingElementTimeout(),
                hiveClientConfig.getFileStatusCacheListingMaxRetries(),
                hiveClientConfig.getFileStatusCacheListingMaxThreads(),
                hiveClientConfig.getFileStatusCacheListingQueueCapacity());
    }

    public CachingDirectoryLister(
            Duration expireAfterWrite,
            DataSize maxSize,
            List<String> includedTables,
            List<String> excludedTables,
            Predicate<FileEntry> filterPredicate)
    {
        this(expireAfterWrite, maxSize, includedTables, excludedTables, filterPredicate, new Duration(0, TimeUnit.MILLISECONDS), new Duration(0, TimeUnit.MILLISECONDS), 0, 1, 1000);
    }

    public CachingDirectoryLister(
            Duration expireAfterWrite,
            DataSize maxSize,
            List<String> includedTables,
            List<String> excludedTables,
            Predicate<FileEntry> filterPredicate,
            Duration listingTimeout,
            Duration listingElementTimeout,
            int listingMaxRetries,
            int listingMaxThreads,
            int listingQueueCapacity)
    {
        requireNonNull(expireAfterWrite, "expireAfterWrite is null");
        requireNonNull(maxSize, "maxSize is null");
        requireNonNull(includedTables, "includedTables is null");
        requireNonNull(excludedTables, "excludedTables is null");
        requireNonNull(filterPredicate, "filterPredicate is null");
        checkArgument(listingMaxRetries >= 0, "listingMaxRetries is negative");
        checkArgument(listingMaxThreads >= 1, "listingMaxThreads must be at least 1");
        checkArgument(listingQueueCapacity >= 1, "listingQueueCapacity must be at least 1");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<CacheKey, ValueHolder>) (key, value) -> toIntExact(key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.tablePredicate = matches(includedTables).and(not(matches(excludedTables)));
        this.filterPredicate = filterPredicate;
        this.listingTimeout = requireNonNull(listingTimeout, "listingTimeout is null");
        this.listingElementTimeout = requireNonNull(listingElementTimeout, "listingElementTimeout is null");
        this.listingMaxRetries = listingMaxRetries;
        this.listingQueueCapacity = listingQueueCapacity;
        this.listingExecutor = isBoundedListingEnabled()
                ? newFixedThreadPool(listingMaxThreads, daemonThreadsNamed("hive-cached-directory-lister-%s"))
                : null;
    }

    @PreDestroy
    public void shutdown()
    {
        if (listingExecutor != null) {
            listingExecutor.shutdownNow();
        }
    }

    /**
     * true when at least one timeout or retry is set. only then we use the bounded (materialized) listing path.
     *
     * @return true if the timeout or retry listing is enabled
     */
    private boolean isBoundedListingEnabled()
    {
        return listingTimeout.toMillis() > 0 || listingElementTimeout.toMillis() > 0 || listingMaxRetries > 0;
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

        if (!isBoundedListingEnabled()) {
            return cachingRemoteIterator(cachedValueHolder, createListingRemoteIterator(fs, location, filterPredicate), cacheKey);
        }
        else {
            return boundedListInternal(fs, location, cacheKey, cachedValueHolder);
        }
    }

    /**
     * Read all files with timeout and retries, then put the full result to cache.
     * On any error we do not cache partial result and remove the empty placeholder.
     *
     * @param fs file system to list
     * @param location directory to list
     * @param cacheKey cache key for this listing
     * @param cachedValueHolder empty placeholder that works as invalidation guard
     * @return iterator over the listed files
     */
    private RemoteIterator<TrinoFileStatus> boundedListInternal(TrinoFileSystem fs, Location location, CacheKey cacheKey, ValueHolder cachedValueHolder)
    {
        // read the whole listing first, so timeout can limit it and we never save partial result to cache
        List<TrinoFileStatus> files;
        try {
            files = loadFilesWithTimeout(fs, location);
        }
        catch (RuntimeException e) {
            // remove empty placeholder, so next query starts new listing and does not use broken cache
            cache.asMap().remove(cacheKey, cachedValueHolder);
            throw e;
        }
        // cachedValueHolder works like a guard: if cache was invalidated in the middle, we do not save old listing
        cache.asMap().replace(cacheKey, cachedValueHolder, new ValueHolder(files));
        return new SimpleRemoteIterator(files.iterator());
    }

    /**
     * Run one bounded listing and retry it up to listingMaxRetries times if it fails or times out.
     *
     * @param fs file system to list
     * @param location directory to list
     * @return all files from the listing
     */
    private List<TrinoFileStatus> loadFilesWithTimeout(TrinoFileSystem fs, Location location)
    {
        TrinoException lastError = null;
        for (int attempt = 0; attempt <= listingMaxRetries; attempt++) {
            try {
                return listWithTimeout(fs, location);
            }
            catch (TrinoException e) {
                lastError = e;
                // do not retry when the thread was interrupted (query cancelled): the next attempt would fail at once
                if (Thread.currentThread().isInterrupted()) {
                    throw e;
                }
                if (attempt < listingMaxRetries) {
                    log.warn(e, "Cached directory listing attempt %s of %s failed for location %s, retrying", attempt + 1, listingMaxRetries + 1, location);
                }
            }
        }
        throw requireNonNull(lastError, "lastError is null");
    }

    /**
     * Run one listing on a separate thread (producer) and read results with timeout (consumer).
     * We always cancel the producer at the end, also when timeout happens.
     *
     * @param fs file system to list
     * @param location directory to list
     * @return all files from the listing
     */
    private List<TrinoFileStatus> listWithTimeout(TrinoFileSystem fs, Location location)
    {
        BlockingQueue<Object> queue = new LinkedBlockingQueue<>(listingQueueCapacity);
        Future<?> producer = submitListing(fs, location, queue);
        try {
            return drainListing(queue, new ListingBudget(listingElementTimeout, listingTimeout), location);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Interrupted while listing directory " + location, e);
        }
        finally {
            // try to stop stuck or not needed listing. blocking filesystem calls may not react to interrupt at once
            producer.cancel(true);
        }
    }

    /**
     * Producer: list files in background thread and put every file to the bounded queue, then END_OF_LISTING or ListingError.
     * put() blocks when the queue is full, so once a timed-out consumer stops reading the producer blocks and is stopped by
     * producer.cancel(true) instead of filling memory nobody reads.
     *
     * @param fs file system to list
     * @param location directory to list
     * @param queue bounded queue where the producer puts files and the end marker
     * @return future of the background listing task
     */
    private Future<?> submitListing(TrinoFileSystem fs, Location location, BlockingQueue<Object> queue)
    {
        return requireNonNull(listingExecutor, "listingExecutor is null").submit(() -> {
            try {
                RemoteIterator<TrinoFileStatus> iterator = createListingRemoteIterator(fs, location, filterPredicate);
                while (iterator.hasNext()) {
                    queue.put(iterator.next());
                }
                queue.put(END_OF_LISTING);
            }
            catch (InterruptedException e) {
                // consumer gone (timeout/cancel): stop quietly
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                // listing failed: hand the error to the consumer
                queue.offer(new ListingError(t));
            }
        });
    }

    /**
     * Consumer: take files from the queue until END_OF_LISTING.
     * If we wait too long for the next file, budget is over and we throw timeout error.
     *
     * @param queue queue with files, the end marker or an error from the producer
     * @param budget how long we can wait for the next file
     * @param location directory to list, used for error messages
     * @return all files from the listing
     * @throws InterruptedException if the waiting thread is interrupted
     */
    private List<TrinoFileStatus> drainListing(BlockingQueue<Object> queue, ListingBudget budget, Location location)
            throws InterruptedException
    {
        List<TrinoFileStatus> files = new ArrayList<>();
        while (true) {
            long waitNanos = budget.nextWaitNanos();
            Object item = waitNanos <= 0 ? null : queue.poll(waitNanos, TimeUnit.NANOSECONDS);
            if (item == null) {
                throw timeoutException(location);
            }
            if (item == END_OF_LISTING) {
                return files;
            }
            if (item instanceof ListingError error) {
                throw asListingException(error.cause(), location);
            }
            files.add((TrinoFileStatus) item);
        }
    }

    private TrinoException timeoutException(Location location)
    {
        return new TrinoException(HIVE_FILESYSTEM_ERROR, format(
                "Timed out listing directory %s (listing-timeout=%s, listing-element-timeout=%s)",
                location,
                listingTimeout,
                listingElementTimeout));
    }

    private static TrinoException asListingException(Throwable cause, Location location)
    {
        if (cause instanceof TrinoException trinoException) {
            return trinoException;
        }
        return new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to list directory " + location, cause);
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

    // holds error from the producer thread, so we can pass it back to the consumer through the queue
    private record ListingError(Throwable cause) {}

    /**
     * Keeps the time that the consumer can wait for the next listing element. It uses the per-element
     * timeout and also the total listing deadline together. When {@link #nextWaitNanos()} is not positive,
     * the time is over and the listing is timed out.
     */
    private static final class ListingBudget
    {
        private final long elementTimeoutNanos;
        private final boolean hasTotalTimeout;
        private final long deadlineNanos;

        private ListingBudget(Duration elementTimeout, Duration totalTimeout)
        {
            this.elementTimeoutNanos = elementTimeout.toMillis() > 0 ? elementTimeout.roundTo(TimeUnit.NANOSECONDS) : Long.MAX_VALUE;
            this.hasTotalTimeout = totalTimeout.toMillis() > 0;
            this.deadlineNanos = System.nanoTime() + (hasTotalTimeout ? totalTimeout.roundTo(TimeUnit.NANOSECONDS) : 0);
        }

        private long nextWaitNanos()
        {
            if (!hasTotalTimeout) {
                return elementTimeoutNanos;
            }
            return Math.min(elementTimeoutNanos, deadlineNanos - System.nanoTime());
        }
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
