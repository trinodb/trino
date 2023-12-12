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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.hash.HashCode;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.airlift.stats.Distribution;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.hash.Hashing.combineOrdered;
import static com.google.common.hash.Hashing.consistentHash;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.memory.EmptySplitCache.EMPTY_SPLIT_CACHE;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;

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

    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 10;
    private static final int DISTRIBUTION_ENTRY_COUNT = 1_000_000;
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
    private final AtomicLong nextStoreId = new AtomicLong();
    @GuardedBy("lock")
    private long cacheRevocableBytes;

    public MemoryCacheManager(MemoryAllocator memoryAllocator, boolean forceStore)
    {
        this.revocableMemoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.forceStore = forceStore;
    }

    public static PlanSignature canonicalizePlanSignature(PlanSignature signature)
    {
        return new PlanSignature(
                signature.getKey(),
                signature.getGroupByColumns(),
                // columns are stored independently
                ImmutableList.of(),
                ImmutableList.of(),
                signature.getPredicate(),
                signature.getDynamicPredicate());
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return allocateSignatureId(signature)
                .<SplitCache>map(MemorySplitCache::new)
                .orElse(EMPTY_SPLIT_CACHE);
    }

    @Override
    public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
    {
        return new MemoryPreferredAddressProvider(signature, nodeManager);
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

    public void addCachedSplitSizeDistribution(Distribution distribution)
    {
        runWithLock(lock.readLock(), () -> {
            int counter = 0;
            for (Iterator<Map.Entry<SplitKey, Channel>> iterator = reverse(splitCache.entries()).iterator(); iterator.hasNext() && counter < DISTRIBUTION_ENTRY_COUNT; counter++) {
                Map.Entry<SplitKey, Channel> entry = iterator.next();
                distribution.add(entry.getValue().getRetainedSizeInBytes());
            }
        });
    }

    @Override
    public long getRevocableBytes()
    {
        return runWithLock(lock.readLock(), () -> cacheRevocableBytes + signatureToId.getRevocableBytes() + columnToId.getRevocableBytes());
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

    public ReentrantReadWriteLock getLock()
    {
        return lock;
    }

    private Optional<ConnectorPageSource> loadPages(long signatureId, long[] columnIds, CacheSplitId splitId)
    {
        SplitKey[] keys = new SplitKey[columnIds.length];
        for (int i = 0; i < columnIds.length; i++) {
            keys[i] = new SplitKey(signatureId, columnIds[i], splitId);
        }

        return getLoadedChannelsWithSameStoreId(keys)
                .map(channels -> new MemoryCachePageSource(updateChannels(keys, channels.toArray(new Channel[0]))));
    }

    private Optional<ConnectorPageSink> storePages(long signatureId, long[] columnIds, CacheSplitId splitId)
    {
        // no column queries cannot be cached
        if (columnIds.length == 0) {
            return Optional.empty();
        }

        SplitKey[] keys = new SplitKey[columnIds.length];
        for (int i = 0; i < columnIds.length; i++) {
            keys[i] = new SplitKey(signatureId, columnIds[i], splitId);
        }

        if (hasChannelsWithSameStoreId(keys) && !forceStore) {
            // split is already cached or currently being stored
            return Optional.empty();
        }

        long storeId = nextStoreId.getAndIncrement();
        Channel[] channels = new Channel[keys.length];
        for (int i = 0; i < keys.length; i++) {
            channels[i] = new Channel(storeId);
        }

        return Optional.of(new MemoryCachePageSink(keys, createStoreChannels(signatureId, keys, channels)));
    }

    private Channel[] updateChannels(SplitKey[] keys, Channel[] channels)
    {
        // make channels the freshest in cache
        runWithLock(lock.writeLock(), () -> {
            for (int i = 0; i < keys.length; i++) {
                removeChannel(keys[i], channels[i]);
                splitCache.put(keys[i], channels[i]);
            }
        });
        return channels;
    }

    private Channel[] createStoreChannels(long signatureId, SplitKey[] keys, Channel[] channels)
    {
        runWithLock(lock.writeLock(), () -> {
            signatureToId.acquireId(signatureId, keys.length);
            for (int i = 0; i < keys.length; i++) {
                columnToId.acquireId(keys[i].columnId());
                splitCache.put(keys[i], channels[i]);
            }
        });
        return channels;
    }

    private boolean hasChannelsWithSameStoreId(SplitKey[] keys)
    {
        return getChannelsWithSameStoreId(keys, false).long2ObjectEntrySet().stream()
                // find store id that contain channels for all columns
                .anyMatch(entry -> entry.getValue().size() == keys.length);
    }

    private Optional<List<Channel>> getLoadedChannelsWithSameStoreId(SplitKey[] keys)
    {
        return getChannelsWithSameStoreId(keys, true).long2ObjectEntrySet().stream()
                // filter store ids that contain channels for all columns
                .filter(entry -> entry.getValue().size() == keys.length)
                // get channels with the newest store id
                .sorted(comparing(entry -> -entry.getLongKey()))
                .map(Map.Entry::getValue)
                .findAny();
    }

    private Long2ObjectMap<List<Channel>> getChannelsWithSameStoreId(SplitKey[] keys, boolean onlyLoaded)
    {
        if (keys.length == 0) {
            return new Long2ObjectOpenHashMap<>();
        }

        Long2ObjectMap<List<Channel>> channels = new Long2ObjectOpenHashMap<>(keys.length);
        runWithLock(lock.readLock(), () -> {
            getColumnChannels(keys[0], 0, onlyLoaded, channels);
            for (int i = 1; i < keys.length; i++) {
                getColumnChannels(keys[i], i, onlyLoaded, channels);
            }
        });

        return channels;
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

    private void finishStoreChannels(SplitKey[] keys, Channel[] channels)
    {
        runWithLock(lock.writeLock(), () -> {
            long entriesSize = 0L;
            for (int i = 0; i < keys.length; i++) {
                channels[i].setLoaded();
                entriesSize += getCacheEntrySize(keys[i], channels[i]);
                checkState(signatureToId.getUsageCount(keys[i].signatureId()) > 0, "Signature id must not be released while split is cached");
            }

            if (!revocableMemoryAllocator.trySetBytes(getRevocableBytes() + entriesSize)) {
                // not sufficient memory to store split pages
                abortStoreChannels(keys, channels);
            }

            cacheRevocableBytes += entriesSize;
            removeEldestSplits(keys);
        });
    }

    private void abortStoreChannels(SplitKey[] keys, Channel[] channels)
    {
        checkArgument(keys.length > 0);
        runWithLock(lock.writeLock(), () -> {
            long initialRevocableBytes = getRevocableBytes();
            signatureToId.releaseId(keys[0].signatureId(), keys.length);
            for (int i = 0; i < keys.length; i++) {
                removeChannel(keys[i], channels[i]);
                columnToId.releaseId(keys[i].columnId());
            }
            long currentRevocableBytes = getRevocableBytes();
            checkState(initialRevocableBytes >= currentRevocableBytes);
            checkState(initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes));
        });
    }

    private void removeChannel(SplitKey key, Channel channel)
    {
        boolean removed = false;
        // Multimap remove(key, elem) can take significant about of time if list of elements
        // for a given key is large. However, aborted channels are usually the latest elements,
        // therefore we can search for a given channel by reversing the elements list.
        // Ideally, we could keep pointer to a Channel entry in a LinkedListMultimap, but the API
        // doesn't expose that.
        for (Iterator<Channel> iterator = reverse(splitCache.get(key)).iterator(); iterator.hasNext(); ) {
            if (iterator.next() == channel) {
                iterator.remove();
                removed = true;
                break;
            }
        }
        checkState(removed, "Expected channel to be removed");
    }

    /**
     * Removes the eldest channels for a given splits that exceed MAX_CACHED_CHANNELS_PER_COLUMN size threshold.
     */
    private void removeEldestSplits(SplitKey[] keys)
    {
        runWithLock(lock.writeLock(), () -> {
            long initialRevocableBytes = getRevocableBytes();
            for (SplitKey key : keys) {
                List<Channel> channels = splitCache.get(key);
                int counter = channels.size() - MAX_CACHED_CHANNELS_PER_COLUMN;
                for (Iterator<Channel> iterator = channels.iterator(); iterator.hasNext() && counter > 0; counter--) {
                    Channel channel = iterator.next();

                    if (!channel.isLoaded()) {
                        continue;
                    }

                    iterator.remove();
                    signatureToId.releaseId(key.signatureId());
                    columnToId.releaseId(key.columnId());
                    cacheRevocableBytes -= getCacheEntrySize(key, channel);
                }
            }
            checkState(cacheRevocableBytes >= 0);
            long currentRevocableBytes = getRevocableBytes();
            checkState(initialRevocableBytes >= currentRevocableBytes);
            checkState(initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes));
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
                signatureToId.releaseId(key.signatureId());
                columnToId.releaseId(key.columnId());
                elementsToRevoke--;

                cacheRevocableBytes -= getCacheEntrySize(key, channel);
            }
            checkState(cacheRevocableBytes >= 0);

            // freeing memory should always succeed, while any non-negative allocation might return false
            long currentRevocableBytes = getRevocableBytes();
            checkState(initialRevocableBytes >= currentRevocableBytes);
            checkState(initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes));
            return initialRevocableBytes - currentRevocableBytes;
        });
    }

    private Optional<SignatureIds> allocateSignatureId(PlanSignature signature)
    {
        PlanSignature canonicalSignature = canonicalizePlanSignature(signature);
        return runWithLock(lock.writeLock(), () -> {
            long initialRevocableBytes = getRevocableBytes();

            long signatureId = signatureToId.allocateId(canonicalSignature);
            long[] columnIds = new long[signature.getColumns().size()];
            for (int i = 0; i < columnIds.length; i++) {
                columnIds[i] = columnToId.allocateId(signature.getColumns().get(i));
            }

            long currentRevocableBytes = getRevocableBytes();
            checkState(currentRevocableBytes >= initialRevocableBytes);
            if (currentRevocableBytes > initialRevocableBytes && !revocableMemoryAllocator.trySetBytes(currentRevocableBytes)) {
                // couldn't allocate ids due to memory constraints
                signatureToId.releaseId(signatureId);
                for (long columnId : columnIds) {
                    columnToId.releaseId(columnId);
                }
                return Optional.empty();
            }

            return Optional.of(new SignatureIds(signatureId, columnIds));
        });
    }

    private record SignatureIds(long signatureId, long[] columnIds) {}

    private void releaseSignatureIds(long signatureId, long... columnIds)
    {
        runWithLock(lock.writeLock(), () -> {
            long initialRevocableBytes = getRevocableBytes();
            signatureToId.releaseId(signatureId);
            for (long columnId : columnIds) {
                columnToId.releaseId(columnId);
            }
            long currentRevocableBytes = getRevocableBytes();
            checkState(initialRevocableBytes >= currentRevocableBytes);
            checkState(initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes));
        });
    }

    private static long getCacheEntrySize(SplitKey splitKey, Channel channel)
    {
        return MAP_ENTRY_SIZE + splitKey.getRetainedSizeInBytes() + channel.getRetainedSizeInBytes();
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
        private final long signatureId;
        private final long[] columnIds;
        private volatile boolean closed;

        private MemorySplitCache(SignatureIds ids)
        {
            this.signatureId = ids.signatureId();
            this.columnIds = ids.columnIds();
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId)
        {
            checkState(!closed, "MemorySplitCache already closed");
            return MemoryCacheManager.this.loadPages(signatureId, columnIds, splitId);
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId)
        {
            checkState(!closed, "MemorySplitCache already closed");
            return MemoryCacheManager.this.storePages(signatureId, columnIds, splitId);
        }

        @Override
        public void close()
        {
            checkState(!closed, "MemorySplitCache already closed");
            closed = true;
            releaseSignatureIds(signatureId, columnIds);
            // prevent cache overflow
            removeEldestSplits(() -> splitCache.size() <= MAP_SIZE_LIMIT && signatureToId.size() <= MAP_SIZE_LIMIT && columnToId.size() <= MAP_SIZE_LIMIT, Integer.MAX_VALUE);
        }
    }

    private class MemoryCachePageSink
            implements ConnectorPageSink
    {
        private final SplitKey[] keys;
        private final Channel[] channels;
        private final List<Block>[] blocks;
        private long memoryUsageBytes;
        private boolean finished;

        public MemoryCachePageSink(SplitKey[] keys, Channel[] channels)
        {
            this.keys = requireNonNull(keys, "keys is null");
            this.channels = requireNonNull(channels, "channels is null");
            // noinspection unchecked
            this.blocks = (List<Block>[]) new List[keys.length];
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
            finishStoreChannels(keys, channels);
            finished = true;
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort()
        {
            checkState(!finished);
            abortStoreChannels(keys, channels);
            finished = true;
        }
    }

    private static class MemoryPreferredAddressProvider
            implements PreferredAddressProvider
    {
        private final HashCode signatureHash;
        private final Supplier<List<Node>> nodesSupplier;

        public MemoryPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
        {
            signatureHash = HashCode.fromInt(canonicalizePlanSignature(signature).hashCode());
            nodesSupplier = Suppliers.memoizeWithExpiration(
                    () -> nodeManager.getWorkerNodes()
                            .stream()
                            .sorted(comparing(Node::getHost))
                            .collect(toImmutableList()),
                    WORKER_NODES_CACHE_TIMEOUT_SECS,
                    SECONDS);
        }

        @Override
        public HostAddress getPreferredAddress(CacheSplitId splitId)
        {
            List<Node> nodes = nodesSupplier.get();
            return nodes.get(consistentHash(combineOrdered(ImmutableList.of(signatureHash, HashCode.fromInt(splitId.hashCode()))), nodes.size())).getHostAndPort();
        }
    }

    @VisibleForTesting
    record SplitKey(long signatureId, long columnId, CacheSplitId splitId)
    {
        static final int INSTANCE_SIZE = instanceSize(SplitKey.class);

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + splitId.getRetainedSizeInBytes();
        }
    }

    static class Channel
    {
        private static final int INSTANCE_SIZE = instanceSize(Channel.class);

        private final long storeId;
        private volatile boolean loaded;
        private volatile Block[] blocks;
        private volatile long blocksRetainedSizeInBytes;
        private volatile long positionCount;

        public Channel(long storeId)
        {
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

        public long getStoreId()
        {
            return storeId;
        }
    }
}
