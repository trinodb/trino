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
package io.trino.plugin.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.reverse;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.cache.PlanSignature.canonicalizePlanSignature;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * {@link CacheManager} implementation that caches split pages in revocable memory.
 * <p>
 * Cache structure essentially consists of multimap:
 * <pre>
 * (CanonicalPlanSignature, ColumnID, SplitID) -> [(StoreID1, ColumnBlocks1), (StoreID2, ColumnBlocks2), ...]
 * </pre>
 * Therefore, cache operates at column level rather than page level. Hence, cache can serve requests
 * for subset of cached columns that share same {@code StoreID}.
 * <p>
 * Whenever pages are cached a unique {@code StoreID} is assigned and all cached columns share that ID.
 * This is required, because two table scans (for different subset of columns) could produce slightly
 * different blocks (e.g. due to adaptive dynamic row filtering). It also means that there might be multiple
 * entries for single {@code (CanonicalPlanSignature, ColumnID, SplitID)}. When fetching pages from cache,
 * all cached entries for all columns must share same {@code StoreID}.
 * <p>
 * {@link MemoryCacheManager} does not have support for any filtering adaptation.
 */
public class MemoryCacheManager
        implements CacheManager
{
    // based on SizeOf.estimatedSizeOf(java.util.Map<K,V>, java.util.function.ToLongFunction<K>, java.util.function.ToLongFunction<V>)
    static final int MAP_ENTRY_SIZE = instanceSize(AbstractMap.SimpleEntry.class);

    private static final int MAP_SIZE_LIMIT = 1_000_000_000;
    static final int MAX_CACHED_CHANNELS_PER_COLUMN = 20;

    private final MemoryAllocator revocableMemoryAllocator;
    private final boolean forceStore;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    @GuardedBy("lock")
    private final LinkedListMultimap<SplitKey, Channel> splitCache = LinkedListMultimap.create();
    @GuardedBy("lock")
    private final ObjectToIdMap<PlanSignature> signatureToId = new ObjectToIdMap<>(PlanSignature::getRetainedSizeInBytes);
    @GuardedBy("lock")
    private final ObjectToIdMap<CacheColumnId> columnToId = new ObjectToIdMap<>(CacheColumnId::getRetainedSizeInBytes);
    @GuardedBy("lock")
    private final ObjectToIdMap<TupleDomain<CacheColumnId>> predicateToId = new ObjectToIdMap<>(predicate -> predicate.getRetainedSizeInBytes(CacheColumnId::getRetainedSizeInBytes));
    private final AtomicLong nextStoreId = new AtomicLong();
    @GuardedBy("lock")
    private long cacheRevocableBytes;

    public MemoryCacheManager(MemoryAllocator memoryAllocator, boolean forceStore)
    {
        this.revocableMemoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.forceStore = forceStore;
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return new MemorySplitCache(allocateSignatureId(signature));
    }

    @Override
    public long revokeMemory(long bytesToRevoke)
    {
        return revokeMemory(bytesToRevoke, Integer.MAX_VALUE);
    }

    public long revokeMemory(long bytesToRevoke, int maxElementsToRevoke)
    {
        checkArgument(bytesToRevoke >= 0);
        return runWithLock(lock.writeLock(), () -> {
            long initialRevocableBytes = getRevocableBytes();
            return removeEldestSplits(() -> initialRevocableBytes - getRevocableBytes() >= bytesToRevoke, maxElementsToRevoke);
        });
    }

    public long getRevocableBytes()
    {
        return runWithLock(lock.readLock(), () ->
                cacheRevocableBytes
                        + signatureToId.getRevocableBytes()
                        + columnToId.getRevocableBytes()
                        + predicateToId.getRevocableBytes());
    }

    public int getCachedPlanSignaturesCount()
    {
        return runWithLock(lock.readLock(), signatureToId::size);
    }

    public int getCachedColumnIdsCount()
    {
        return runWithLock(lock.readLock(), columnToId::size);
    }

    public int getCachedSplitsCount()
    {
        return runWithLock(lock.readLock(), splitCache::size);
    }

    private Optional<ConnectorPageSource> loadPages(SignatureIds ids, CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        checkPredicates(ids, predicate, unenforcedPredicate);
        return getLoadedChannelsWithSameStoreId(ids, splitId, predicate, unenforcedPredicate)
                .map(channels -> new MemoryCachePageSource(updateChannels(channels.toArray(new Channel[0]))));
    }

    private Optional<ConnectorPageSink> storePages(SignatureIds ids, CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        checkPredicates(ids, predicate, unenforcedPredicate);

        // no column queries cannot be cached
        if (ids.columnIds().length == 0) {
            return Optional.empty();
        }

        if (hasChannelsWithSameStoreId(ids, splitId, predicate, unenforcedPredicate) && !forceStore) {
            // split is already cached or currently being stored
            return Optional.empty();
        }

        return createPageSink(ids, splitId, predicate, unenforcedPredicate, nextStoreId.getAndIncrement());
    }

    private void checkPredicates(SignatureIds ids, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        checkArgument(ids.columnSet().containsAll(predicate.getDomains().orElse(ImmutableMap.of()).keySet()), "Predicate references missing column");
        checkArgument(ids.columnSet().containsAll(unenforcedPredicate.getDomains().orElse(ImmutableMap.of()).keySet()), "Unenforced predicate references missing column");
    }

    private Channel[] updateChannels(Channel[] channels)
    {
        // make channels the freshest in cache
        runWithLock(lock.writeLock(), () -> {
            for (Channel channel : channels) {
                if (removeChannel(channel)) {
                    // channel might have been purged in the meantime
                    splitCache.put(channel.getKey(), channel);
                }
            }
        });
        return channels;
    }

    private Optional<ConnectorPageSink> createPageSink(
            SignatureIds ids,
            CacheSplitId splitId,
            TupleDomain<CacheColumnId> predicate,
            TupleDomain<CacheColumnId> unenforcedPredicate,
            long storeId)
    {
        return runWithLock(lock.writeLock(), () -> {
            PredicateIds predicateIds = allocatePredicateIds(predicate, unenforcedPredicate, ids.columnIds().length);

            SplitKey[] keys = getSplitKeys(ids, predicateIds, splitId);
            Channel[] channels = new Channel[keys.length];
            for (int i = 0; i < keys.length; i++) {
                channels[i] = new Channel(keys[i], storeId);
            }

            // increment non-revocable reference count for ids used by sink channels
            signatureToId.acquireId(ids.signatureId(), keys.length);
            for (int i = 0; i < keys.length; i++) {
                columnToId.acquireId(keys[i].columnId());
                splitCache.put(keys[i], channels[i]);
            }

            return Optional.of(new MemoryCachePageSink(ids, predicateIds, channels));
        });
    }

    private boolean hasChannelsWithSameStoreId(SignatureIds ids, CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        return getChannelsWithSameStoreId(ids, splitId, predicate, unenforcedPredicate, false).long2ObjectEntrySet().stream()
                // find store id that contain channels for all columns
                .anyMatch(entry -> entry.getValue().size() == ids.columnIds().length);
    }

    private Optional<List<Channel>> getLoadedChannelsWithSameStoreId(
            SignatureIds ids,
            CacheSplitId splitId,
            TupleDomain<CacheColumnId> predicate,
            TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        return getChannelsWithSameStoreId(ids, splitId, predicate, unenforcedPredicate, true).long2ObjectEntrySet().stream()
                // filter store ids that contain channels for all columns
                .filter(entry -> entry.getValue().size() == ids.columnIds().length)
                // get channels with the newest store id
                .sorted(comparing(entry -> -entry.getLongKey()))
                .map(Map.Entry::getValue)
                .findAny();
    }

    private Long2ObjectMap<List<Channel>> getChannelsWithSameStoreId(
            SignatureIds ids,
            CacheSplitId splitId,
            TupleDomain<CacheColumnId> predicate,
            TupleDomain<CacheColumnId> unenforcedPredicate,
            boolean onlyLoaded)
    {
        if (ids.columnIds().length == 0) {
            return new Long2ObjectOpenHashMap<>();
        }

        Long2ObjectMap<List<Channel>> channels = new Long2ObjectOpenHashMap<>(ids.columnIds().length);
        runWithLock(lock.readLock(), () -> {
            Optional<PredicateIds> predicateIds = getPredicateIds(predicate, unenforcedPredicate);
            if (predicateIds.isEmpty()) {
                // missing predicate ids, hence data cannot be cached
                return;
            }

            SplitKey[] keys = getSplitKeys(ids, predicateIds.get(), splitId);
            getColumnChannels(keys[0], 0, onlyLoaded, channels);
            for (int i = 1; i < keys.length; i++) {
                getColumnChannels(keys[i], i, onlyLoaded, channels);
            }
        });

        return channels;
    }

    private SplitKey[] getSplitKeys(SignatureIds ids, PredicateIds predicateIds, CacheSplitId splitId)
    {
        SplitKey[] keys = new SplitKey[ids.columnIds().length];
        for (int i = 0; i < ids.columnIds().length; i++) {
            long columnId = ids.columnIds()[i];
            keys[i] = new SplitKey(ids.signatureId(), columnId, splitId, predicateIds.predicateId(), predicateIds.unenforcedPredicateId());
        }
        return keys;
    }

    private void getColumnChannels(SplitKey key, int channelIndex, boolean onlyLoaded, Long2ObjectMap<List<Channel>> channels)
    {
        // fetch MAX_CACHED_CHANNELS_PER_COLUMN latest cached channels
        reverse(splitCache.get(key)).stream()
                .limit(MAX_CACHED_CHANNELS_PER_COLUMN)
                .forEach(channel -> {
                    if (onlyLoaded && !channel.isLoaded()) {
                        return;
                    }

                    if (channelIndex == 0) {
                        List<Channel> list = new ArrayList<>();
                        list.add(channel);
                        checkState(channels.put(channel.getStoreId(), list) == null);
                    }
                    else {
                        List<Channel> list = channels.get(channel.getStoreId());
                        if (list != null && list.size() == channelIndex) {
                            list.add(channel);
                        }
                    }
                });
    }

    private void finishStoreChannels(SignatureIds signatureIds, PredicateIds predicateIds, Channel[] channels)
    {
        runWithLock(lock.writeLock(), () -> {
            checkState(signatureToId.getTotalUsageCount(signatureIds.signatureId()) >= channels.length, "Signature id must not be released while split is cached");
            long initialRevocableBytes = getRevocableBytes();

            // make memory retained by ids revocable
            signatureToId.acquireRevocableId(signatureIds.signatureId(), channels.length);
            predicateToId.acquireRevocableId(predicateIds.predicateId(), channels.length);
            predicateToId.acquireRevocableId(predicateIds.unenforcedPredicateId(), channels.length);
            for (long columnId : signatureIds.columnIds()) {
                columnToId.acquireRevocableId(columnId);
            }

            // account for channels retained size
            long entriesSize = 0L;
            for (Channel channel : channels) {
                channel.setLoaded();
                entriesSize += getCacheEntrySize(channel);
            }

            long currentRevocableBytes = getRevocableBytes();
            checkState(currentRevocableBytes >= initialRevocableBytes);
            if (!trySetRevocableBytes(initialRevocableBytes, currentRevocableBytes + entriesSize)) {
                // not sufficient memory to store split pages
                signatureToId.releaseRevocableId(signatureIds.signatureId(), channels.length);
                predicateToId.releaseRevocableId(predicateIds.predicateId(), channels.length);
                predicateToId.releaseRevocableId(predicateIds.unenforcedPredicateId(), channels.length);
                for (long columnId : signatureIds.columnIds()) {
                    columnToId.releaseRevocableId(columnId);
                }
                abortStoreChannels(signatureIds, predicateIds, channels);
                return;
            }

            // dereference non-revocable ids
            signatureToId.releaseId(signatureIds.signatureId(), channels.length);
            predicateToId.releaseId(predicateIds.predicateId(), channels.length);
            predicateToId.releaseId(predicateIds.unenforcedPredicateId(), channels.length);
            for (long columnId : signatureIds.columnIds()) {
                columnToId.releaseId(columnId);
            }

            cacheRevocableBytes += entriesSize;
            removeEldestChannels(channels);
        });
    }

    private void abortStoreChannels(SignatureIds signatureIds, PredicateIds predicateIds, Channel[] channels)
    {
        runWithLock(lock.writeLock(), () -> {
            signatureToId.releaseId(signatureIds.signatureId(), channels.length);
            predicateToId.releaseId(predicateIds.predicateId(), channels.length);
            predicateToId.releaseId(predicateIds.unenforcedPredicateId(), channels.length);
            for (Channel channel : channels) {
                checkState(removeChannel(channel), "Expected channel to be removed");
                columnToId.releaseId(channel.getKey().columnId());
            }
        });
    }

    private boolean removeChannel(Channel channel)
    {
        boolean removed = false;
        // Multimap remove(key, elem) can take significant about of time if list of elements
        // for a given key is large. However, aborted channels are usually the latest elements,
        // therefore we can search for a given channel by reversing the elements list.
        // Ideally, we could keep pointer to a Channel entry in a LinkedListMultimap, but the API
        // doesn't expose that.
        for (Iterator<Channel> iterator = reverse(splitCache.get(channel.getKey())).iterator(); iterator.hasNext(); ) {
            if (iterator.next() == channel) {
                iterator.remove();
                removed = true;
                break;
            }
        }
        return removed;
    }

    /**
     * Removes the eldest channels for a given split that exceed MAX_CACHED_CHANNELS_PER_COLUMN size threshold.
     */
    private void removeEldestChannels(Channel[] splitChannels)
    {
        runWithLock(lock.writeLock(), () -> {
            long initialRevocableBytes = getRevocableBytes();
            for (Channel splitChannel : splitChannels) {
                SplitKey key = splitChannel.getKey();
                List<Channel> channels = splitCache.get(key);
                int counter = channels.size() - MAX_CACHED_CHANNELS_PER_COLUMN;
                for (Iterator<Channel> iterator = channels.iterator(); iterator.hasNext() && counter > 0; counter--) {
                    Channel channel = iterator.next();

                    if (!channel.isLoaded()) {
                        continue;
                    }

                    iterator.remove();

                    signatureToId.releaseRevocableId(key.signatureId());
                    columnToId.releaseRevocableId(key.columnId());
                    predicateToId.releaseRevocableId(key.predicateId());
                    predicateToId.releaseRevocableId(key.unenforcedPredicateId());

                    cacheRevocableBytes -= getCacheEntrySize(channel);
                }
            }
            checkState(cacheRevocableBytes >= 0);
            long currentRevocableBytes = getRevocableBytes();
            checkState(initialRevocableBytes >= currentRevocableBytes);
            checkState(trySetRevocableBytes(initialRevocableBytes, currentRevocableBytes));
        });
    }

    private long removeEldestSplits(BooleanSupplier stopCondition, int maxElementsToRevoke)
    {
        return runWithLock(lock.writeLock(), () -> {
            if (splitCache.isEmpty()) {
                // no splits to remove
                return 0L;
            }

            long initialRevocableBytes = getRevocableBytes();
            int elementsToRevoke = maxElementsToRevoke;
            for (Iterator<Map.Entry<SplitKey, Channel>> iterator = splitCache.entries().iterator(); iterator.hasNext(); ) {
                if (stopCondition.getAsBoolean() || elementsToRevoke <= 0) {
                    break;
                }

                Map.Entry<SplitKey, Channel> entry = iterator.next();
                SplitKey key = entry.getKey();
                Channel channel = entry.getValue();

                // skip unloaded entries
                if (!channel.isLoaded()) {
                    continue;
                }

                iterator.remove();

                signatureToId.releaseRevocableId(key.signatureId());
                columnToId.releaseRevocableId(key.columnId());
                predicateToId.releaseRevocableId(key.predicateId());
                predicateToId.releaseRevocableId(key.unenforcedPredicateId());

                elementsToRevoke--;
                cacheRevocableBytes -= getCacheEntrySize(channel);
            }
            checkState(cacheRevocableBytes >= 0);

            // freeing memory should always succeed, while any non-negative allocation might return false
            long currentRevocableBytes = getRevocableBytes();
            checkState(initialRevocableBytes >= currentRevocableBytes);
            checkState(trySetRevocableBytes(initialRevocableBytes, currentRevocableBytes));
            return initialRevocableBytes - currentRevocableBytes;
        });
    }

    private SignatureIds allocateSignatureId(PlanSignature signature)
    {
        PlanSignature canonicalSignature = canonicalizePlanSignature(signature);
        Set<CacheColumnId> columnSet = ImmutableSet.copyOf(signature.getColumns());
        return runWithLock(lock.writeLock(), () -> {
            // allocate non-revocable ids for signature and columns
            long signatureId = signatureToId.allocateId(canonicalSignature);
            long[] columnIds = new long[signature.getColumns().size()];
            for (int i = 0; i < columnIds.length; i++) {
                columnIds[i] = columnToId.allocateId(signature.getColumns().get(i));
            }
            return new SignatureIds(signatureId, columnSet, columnIds, signature.getColumns());
        });
    }

    private record SignatureIds(long signatureId, Set<CacheColumnId> columnSet, long[] columnIds, List<CacheColumnId> columns) {}

    private void releaseSignatureIds(SignatureIds ids)
    {
        runWithLock(lock.writeLock(), () -> {
            signatureToId.releaseId(ids.signatureId());
            for (long columnId : ids.columnIds()) {
                columnToId.releaseId(columnId);
            }
        });
    }

    private Optional<PredicateIds> getPredicateIds(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
    {
        return runWithLock(lock.readLock(), () -> predicateToId.getId(predicate)
                .flatMap(predicateId -> predicateToId.getId(unenforcedPredicate)
                        .map(unenforcedPredicateId -> new PredicateIds(predicateId, unenforcedPredicateId))));
    }

    private PredicateIds allocatePredicateIds(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate, int count)
    {
        // allocate non-revocable ids for predicates
        return runWithLock(lock.writeLock(), () -> new PredicateIds(predicateToId.allocateId(predicate, count), predicateToId.allocateId(unenforcedPredicate, count)));
    }

    private record PredicateIds(long predicateId, long unenforcedPredicateId) {}

    private boolean trySetRevocableBytes(long initialRevocableBytes, long currentRevocableBytes)
    {
        return initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes);
    }

    private static long getCacheEntrySize(Channel channel)
    {
        return MAP_ENTRY_SIZE + channel.getKey().getRetainedSizeInBytes() + channel.getRetainedSizeInBytes();
    }

    private static void runWithLock(Lock lock, Runnable runnable)
    {
        lock.lock();
        try {
            runnable.run();
        }
        finally {
            lock.unlock();
        }
    }

    private static <T> T runWithLock(Lock lock, Supplier<T> supplier)
    {
        lock.lock();
        try {
            return supplier.get();
        }
        finally {
            lock.unlock();
        }
    }

    private class MemorySplitCache
            implements SplitCache
    {
        private final SignatureIds ids;
        private volatile boolean closed;

        private MemorySplitCache(SignatureIds ids)
        {
            this.ids = ids;
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            checkState(!closed, "MemorySplitCache already closed");
            return MemoryCacheManager.this.loadPages(ids, splitId, predicate, unenforcedPredicate);
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            checkState(!closed, "MemorySplitCache already closed");
            return MemoryCacheManager.this.storePages(ids, splitId, predicate, unenforcedPredicate);
        }

        @Override
        public void close()
        {
            checkState(!closed, "MemorySplitCache already closed");
            closed = true;
            releaseSignatureIds(ids);
            // prevent cache overflow
            removeEldestSplits(() -> splitCache.size() <= MAP_SIZE_LIMIT && signatureToId.size() <= MAP_SIZE_LIMIT && columnToId.size() <= MAP_SIZE_LIMIT, Integer.MAX_VALUE);
        }
    }

    private class MemoryCachePageSink
            implements ConnectorPageSink
    {
        private final SignatureIds signatureIds;
        private final PredicateIds predicateIds;
        private final Channel[] channels;
        private final List<Block>[] blocks;
        private long memoryUsageBytes;
        private boolean finished;

        public MemoryCachePageSink(SignatureIds signatureIds, PredicateIds predicateIds, Channel[] channels)
        {
            this.signatureIds = requireNonNull(signatureIds, "signatureIds is null");
            this.predicateIds = requireNonNull(predicateIds, "predicateIds is null");
            this.channels = requireNonNull(channels, "channels is null");
            // noinspection unchecked
            this.blocks = (List<Block>[]) new List[channels.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = new ArrayList<>();
            }
        }

        @Override
        public long getMemoryUsage()
        {
            return memoryUsageBytes;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            for (int i = 0; i < channels.length; i++) {
                // Compact the block
                Block block = page.getBlock(i);
                block = block.copyRegion(0, block.getPositionCount());
                blocks[i].add(block);
                memoryUsageBytes += block.getRetainedSizeInBytes();
            }
            return completedFuture(null);
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            checkState(!finished);
            for (int i = 0; i < channels.length; i++) {
                channels[i].setBlocks(blocks[i].toArray(new Block[0]));
            }
            finishStoreChannels(signatureIds, predicateIds, channels);
            finished = true;
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort()
        {
            checkState(!finished);
            abortStoreChannels(signatureIds, predicateIds, channels);
            finished = true;
        }
    }

    @VisibleForTesting
    static final class SplitKey
    {
        static final int INSTANCE_SIZE = instanceSize(SplitKey.class);
        private final long signatureId;
        private final long columnId;
        private final CacheSplitId splitId;
        private final long predicateId;
        private final long unenforcedPredicateId;

        private volatile int hashCode;

        SplitKey(long signatureId, long columnId, CacheSplitId splitId, long predicateId, long unenforcedPredicateId)
        {
            this.signatureId = signatureId;
            this.columnId = columnId;
            this.splitId = requireNonNull(splitId, "splitId is null");
            this.predicateId = predicateId;
            this.unenforcedPredicateId = unenforcedPredicateId;
        }

        public long signatureId()
        {
            return signatureId;
        }

        public long columnId()
        {
            return columnId;
        }

        public long predicateId()
        {
            return predicateId;
        }

        public long unenforcedPredicateId()
        {
            return unenforcedPredicateId;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + splitId.getRetainedSizeInBytes();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SplitKey splitKey = (SplitKey) o;
            return signatureId == splitKey.signatureId
                    && columnId == splitKey.columnId
                    && splitId.equals(splitKey.splitId)
                    && predicateId == splitKey.predicateId
                    && unenforcedPredicateId == splitKey.unenforcedPredicateId;
        }

        @Override
        public int hashCode()
        {
            if (hashCode == 0) {
                hashCode = Objects.hash(signatureId, columnId, splitId, predicateId, unenforcedPredicateId);
            }
            return hashCode;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("signatureId", signatureId)
                    .add("columnId", columnId)
                    .add("splitId", splitId)
                    .add("predicateId", predicateId)
                    .add("unenforcedPredicateId", unenforcedPredicateId)
                    .toString();
        }
    }

    static class Channel
    {
        private static final int INSTANCE_SIZE = instanceSize(Channel.class);

        private final SplitKey key;
        private final long storeId;
        private volatile boolean loaded;
        private volatile Block[] blocks;
        private volatile long blocksRetainedSizeInBytes;
        private volatile long positionCount;

        public Channel(SplitKey key, long storeId)
        {
            this.key = requireNonNull(key, "key is null");
            this.storeId = storeId;
        }

        public boolean isLoaded()
        {
            return loaded;
        }

        public void setLoaded()
        {
            checkState(!loaded);
            loaded = true;
        }

        public Block[] getBlocks()
        {
            checkState(loaded);
            return blocks;
        }

        public void setBlocks(Block[] blocks)
        {
            checkState(!loaded);
            this.blocks = requireNonNull(blocks, "blocks is null");
            long blocksRetainedSizeInBytes = 0;
            long positionCount = 0L;
            for (Block block : blocks) {
                blocksRetainedSizeInBytes += block.getRetainedSizeInBytes();
                positionCount += block.getPositionCount();
            }
            this.blocksRetainedSizeInBytes = blocksRetainedSizeInBytes;
            this.positionCount = positionCount;
        }

        public long getRetainedSizeInBytes()
        {
            checkState(loaded);
            return INSTANCE_SIZE + sizeOf(blocks) + blocksRetainedSizeInBytes;
        }

        public long getBlocksRetainedSizeInBytes()
        {
            checkState(loaded);
            return blocksRetainedSizeInBytes;
        }

        public long getPositionCount()
        {
            checkState(loaded);
            return positionCount;
        }

        public SplitKey getKey()
        {
            return key;
        }

        public long getStoreId()
        {
            return storeId;
        }
    }
}
