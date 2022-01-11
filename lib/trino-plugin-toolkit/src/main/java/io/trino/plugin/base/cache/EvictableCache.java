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
package io.trino.plugin.base.cache;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.CheckForNull;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.airlift.concurrent.MoreFutures.getDone;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

/**
 * A Cache implementation similar to ones produced by {@code com.google.common.cache.CacheBuilder},
 * but one that does not exhibits <a href="https://github.com/google/guava/issues/1881">Guava issue #1881</a>, i.e.
 * a {@link #getIfPresent(Object)} after {@link #invalidate(Object)} is guaranteed to return {@code null} and
 * {@link #get(Object, Callable)} after {@link #invalidate(Object)} is guaranteed to load a fresh value.
 */
public class EvictableCache<K, V>
        extends AbstractCache<K, V>
        implements Cache<K, V>
{
    /**
     * @apiNote Piggy-back on {@link CacheBuilder} for cache TTL.
     */
    public static <K, V> EvictableCache<K, V> buildWith(CacheBuilder<? super K, Object> cacheBuilder)
    {
        return new EvictableCache<>(cacheBuilder);
    }

    // private final Map<K, Future<V>> map = new ConcurrentHashMap<>();
    private final Cache<K, Future<V>> delegate;

    private final StatsCounter statsCounter = new SimpleStatsCounter();

    private EvictableCache(CacheBuilder<? super K, Object> cacheBuilder)
    {
        requireNonNull(cacheBuilder, "cacheBuilder is null");
        this.delegate = cacheBuilder.build();
    }

    @CheckForNull
    @Override
    public V getIfPresent(Object key)
    {
        Future<V> future = delegate.getIfPresent(key);
        if (future != null && future.isDone()) {
            statsCounter.recordHits(1);
            return getDone(future);
        }
        statsCounter.recordMisses(1);
        return null;
    }

    @Override
    public V get(K key, Callable<? extends V> loader)
            throws ExecutionException
    {
        requireNonNull(key, "key is null");
        requireNonNull(loader, "loader is null");

        while (true) {
            SettableFuture<V> newFuture = SettableFuture.create();
            Future<V> future = delegate.asMap().computeIfAbsent(key, ignored -> newFuture);
            if (future.isDone() && !future.isCancelled()) {
                statsCounter.recordHits(1);
                return getDone(future);
            }

            statsCounter.recordMisses(1);
            if (future == newFuture) {
                // We put the future in.

                V computed;
                long loadStartNanos = nanoTime();
                try {
                    computed = loader.call();
                    requireNonNull(computed, "computed is null");
                }
                catch (Exception e) {
                    statsCounter.recordLoadException(nanoTime() - loadStartNanos);
                    delegate.asMap().remove(key, newFuture);
                    // wake up waiters, let them retry
                    newFuture.cancel(false);
                    throw new ExecutionException(e);
                }
                statsCounter.recordLoadSuccess(nanoTime() - loadStartNanos);
                newFuture.set(computed);
                return computed;
            }

            // Someone else is loading the key, let's wait.
            try {
                return future.get();
            }
            catch (CancellationException e) {
                // Invalidated, or load failed
            }
            catch (ExecutionException e) {
                // Should never happen
                throw new IllegalStateException("Future unexpectedly completed with exception", e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }

            // Someone else was loading the key, but the load was invalidated.
        }
    }

    @Override
    public void put(K key, V value)
    {
        throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation. Use get(key, callable) instead.");
    }

    @Override
    public void invalidate(Object key)
    {
        delegate.invalidate(key);
    }

    @Override
    public void invalidateAll(Iterable<?> keys)
    {
        delegate.invalidateAll(keys);
    }

    @Override
    public void invalidateAll()
    {
        delegate.invalidateAll();
    }

    @Override
    public long size()
    {
        // Includes entries being computed. Approximate, as allowed per method contract.
        return delegate.size();
    }

    @Override
    public CacheStats stats()
    {
        return statsCounter.snapshot().plus(
                new CacheStats(
                        0,
                        0,
                        0,
                        0,
                        0,
                        delegate.stats().evictionCount()));
    }

    @Override
    public ConcurrentMap<K, V> asMap()
    {
        // TODO implement and remove non-interface keySet()
        throw new UnsupportedOperationException();
    }

    public Set<K> keySet()
    {
        return delegate.asMap().keySet();
    }

    @Override
    public void cleanUp()
    {
        delegate.cleanUp();
    }
}
