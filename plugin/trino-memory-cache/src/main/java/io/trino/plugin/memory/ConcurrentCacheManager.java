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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.spi.cache.PlanSignature.canonicalizePlanSignature;
import static java.lang.Math.floorMod;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;

/**
 * Distributed cache requests between {@link MemoryCacheManager}s thus reducing locking pressure.
 */
public class ConcurrentCacheManager
        implements CacheManager
{
    private static final int CACHE_MANAGERS_COUNT = 128;

    private final MemoryAllocator revocableMemoryAllocator;
    private final MemoryCacheManager[] cacheManagers;
    private final ReentrantReadWriteLock revokeLock = new ReentrantReadWriteLock();
    @GuardedBy("this")
    private long allocatedMemory;

    @Inject
    public ConcurrentCacheManager(CacheManagerContext context)
    {
        this(context, false);
    }

    @VisibleForTesting
    ConcurrentCacheManager(CacheManagerContext context, boolean forceStore)
    {
        requireNonNull(context, "context is null");
        this.revocableMemoryAllocator = context.revocableMemoryAllocator();
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(new CacheMemoryReservationHandler(), 0L);
        cacheManagers = new MemoryCacheManager[CACHE_MANAGERS_COUNT];
        for (int i = 0; i < CACHE_MANAGERS_COUNT; i++) {
            cacheManagers[i] = new MemoryCacheManager(memoryContext.newLocalMemoryContext("ignored")::trySetBytes, forceStore);
        }
    }

    @Managed
    public int getCachedPlanSignaturesCount()
    {
        return Arrays.stream(cacheManagers)
                .mapToInt(MemoryCacheManager::getCachedPlanSignaturesCount)
                .sum();
    }

    @Managed
    public int getCachedColumnIdsCount()
    {
        return Arrays.stream(cacheManagers)
                .mapToInt(MemoryCacheManager::getCachedColumnIdsCount)
                .sum();
    }

    @Managed
    public int getCachedSplitsCount()
    {
        return Arrays.stream(cacheManagers)
                .mapToInt(MemoryCacheManager::getCachedSplitsCount)
                .sum();
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return new ConcurrentSplitCache(signature);
    }

    @Override
    public long revokeMemory(long bytesToRevoke)
    {
        return revokeMemory(bytesToRevoke, 10);
    }

    @VisibleForTesting
    long revokeMemory(long bytesToRevoke, int minElementsToRevoke)
    {
        // shuffle managers to prevent bias when revoking
        List<MemoryCacheManager> shuffledManagers = new ArrayList<>(Arrays.asList(cacheManagers));
        shuffle(shuffledManagers);

        revokeLock.writeLock().lock();
        try {
            synchronized (this) {
                long initialAllocatedMemory = allocatedMemory;
                int elementsToRevoke = minElementsToRevoke;
                while (initialAllocatedMemory - allocatedMemory < bytesToRevoke) {
                    boolean revoked = false;
                    for (MemoryCacheManager manager : shuffledManagers) {
                        if (manager.revokeMemory(bytesToRevoke, elementsToRevoke) > 0) {
                            revoked = true;
                        }
                    }
                    if (!revoked) {
                        // no more elements to revoke
                        break;
                    }
                    // increase the revoke batch size
                    elementsToRevoke *= 2;
                }
                return initialAllocatedMemory - allocatedMemory;
            }
        }
        finally {
            revokeLock.writeLock().unlock();
        }
    }

    private class ConcurrentSplitCache
            implements SplitCache
    {
        private final PlanSignature signature;
        private final int signatureHash;
        private final AtomicReferenceArray<SplitCache> splitCaches = new AtomicReferenceArray<>(CACHE_MANAGERS_COUNT);

        public ConcurrentSplitCache(PlanSignature signature)
        {
            this.signature = requireNonNull(signature, "signature is null");
            this.signatureHash = canonicalizePlanSignature(signature).hashCode();
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            if (!revokeLock.readLock().tryLock()) {
                return Optional.empty();
            }
            try {
                return getSplitCache(splitId).loadPages(splitId, predicate, unenforcedPredicate);
            }
            finally {
                revokeLock.readLock().unlock();
            }
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            if (!revokeLock.readLock().tryLock()) {
                return Optional.empty();
            }
            try {
                return getSplitCache(splitId).storePages(splitId, predicate, unenforcedPredicate);
            }
            finally {
                revokeLock.readLock().unlock();
            }
        }

        @Override
        public void close()
                throws IOException
        {
            for (int i = 0; i < splitCaches.length(); i++) {
                SplitCache cache = splitCaches.getAndSet(i, null);
                if (cache != null) {
                    cache.close();
                }
            }
        }

        private SplitCache getSplitCache(CacheSplitId splitId)
        {
            int index = getCacheManagerIndex(signatureHash, splitId);
            SplitCache splitCache = splitCaches.get(index);
            if (splitCache != null) {
                return splitCache;
            }

            splitCache = cacheManagers[index].getSplitCache(signature);
            if (!splitCaches.compareAndSet(index, null, splitCache)) {
                // split cache instance was set concurrently
                try {
                    splitCache.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                splitCache = requireNonNull(splitCaches.get(index));
            }

            return splitCache;
        }
    }

    @VisibleForTesting
    MemoryCacheManager getCacheManager(PlanSignature signature, CacheSplitId splitId)
    {
        int signatureHash = canonicalizePlanSignature(signature).hashCode();
        return cacheManagers[getCacheManagerIndex(signatureHash, splitId)];
    }

    @VisibleForTesting
    MemoryCacheManager[] getCacheManagers()
    {
        return cacheManagers;
    }

    private static int getCacheManagerIndex(int signatureHash, CacheSplitId splitId)
    {
        return floorMod(Objects.hash(signatureHash, splitId.hashCode()), CACHE_MANAGERS_COUNT);
    }

    private class CacheMemoryReservationHandler
            implements MemoryReservationHandler
    {
        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            if (delta == 0) {
                // noop
                return true;
            }

            synchronized (ConcurrentCacheManager.this) {
                if (!revocableMemoryAllocator.trySetBytes(allocatedMemory + delta)) {
                    return false;
                }

                allocatedMemory += delta;
                return true;
            }
        }
    }
}
