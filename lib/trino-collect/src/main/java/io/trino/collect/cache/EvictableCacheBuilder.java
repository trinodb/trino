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
package io.trino.collect.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.errorprone.annotations.CheckReturnValue;
import io.trino.collect.cache.EvictableCache.Token;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Builder for {@link Cache} and {@link LoadingCache} instances, similar to {@link CacheBuilder},
 * but creating cache implementations that do not exhibit <a href="https://github.com/google/guava/issues/1881">Guava issue #1881</a>:
 * a cache inspection with {@link Cache#getIfPresent(Object)} or {@link Cache#get(Object, Callable)} is guaranteed to return
 * fresh state after {@link Cache#invalidate(Object)}, {@link Cache#invalidateAll(Iterable)} or {@link Cache#invalidateAll()} were called.
 */
public final class EvictableCacheBuilder<K, V>
{
    @CheckReturnValue
    public static EvictableCacheBuilder<Object, Object> newBuilder()
    {
        return new EvictableCacheBuilder<>();
    }

    private Optional<Duration> expireAfterWrite = Optional.empty();
    private Optional<Duration> refreshAfterWrite = Optional.empty();
    private Optional<Long> maximumSize = Optional.empty();
    private Optional<Long> maximumWeight = Optional.empty();
    private Optional<Weigher<? super Token<K>, ? super V>> weigher = Optional.empty();
    private boolean recordStats;
    private Optional<DisabledCacheImplementation> disabledCacheImplementation = Optional.empty();

    private EvictableCacheBuilder() {}

    public EvictableCacheBuilder<K, V> expireAfterWrite(long duration, TimeUnit unit)
    {
        return expireAfterWrite(toDuration(duration, unit));
    }

    public EvictableCacheBuilder<K, V> expireAfterWrite(Duration duration)
    {
        checkState(!this.expireAfterWrite.isPresent(), "expireAfterWrite already set");
        this.expireAfterWrite = Optional.of(duration);
        return this;
    }

    public EvictableCacheBuilder<K, V> refreshAfterWrite(long duration, TimeUnit unit)
    {
        return refreshAfterWrite(toDuration(duration, unit));
    }

    public EvictableCacheBuilder<K, V> refreshAfterWrite(Duration duration)
    {
        checkState(!this.refreshAfterWrite.isPresent(), "refreshAfterWrite already set");
        this.refreshAfterWrite = Optional.of(duration);
        return this;
    }

    public EvictableCacheBuilder<K, V> maximumSize(long maximumSize)
    {
        checkState(!this.maximumSize.isPresent(), "maximumSize already set");
        checkState(!this.maximumWeight.isPresent(), "maximumWeight already set");
        this.maximumSize = Optional.of(maximumSize);
        return this;
    }

    public EvictableCacheBuilder<K, V> maximumWeight(long maximumWeight)
    {
        checkState(!this.maximumWeight.isPresent(), "maximumWeight already set");
        checkState(!this.maximumSize.isPresent(), "maximumSize already set");
        this.maximumWeight = Optional.of(maximumWeight);
        return this;
    }

    public <K1 extends K, V1 extends V> EvictableCacheBuilder<K1, V1> weigher(Weigher<? super K1, ? super V1> weigher)
    {
        checkState(!this.weigher.isPresent(), "weigher already set");
        @SuppressWarnings("unchecked") // see com.google.common.cache.CacheBuilder.weigher
        EvictableCacheBuilder<K1, V1> cast = (EvictableCacheBuilder<K1, V1>) this;
        cast.weigher = Optional.of(new TokenWeigher<>(weigher));
        return cast;
    }

    public EvictableCacheBuilder<K, V> recordStats()
    {
        recordStats = true;
        return this;
    }

    /**
     * Choose a behavior for case when caching is disabled that may allow data and failure sharing between concurrent callers.
     */
    public EvictableCacheBuilder<K, V> shareResultsAndFailuresEvenIfDisabled()
    {
        checkState(!disabledCacheImplementation.isPresent(), "disabledCacheImplementation already set");
        disabledCacheImplementation = Optional.of(DisabledCacheImplementation.GUAVA);
        return this;
    }

    /**
     * Choose a behavior for case when caching is disabled that prevents data and failure sharing between concurrent callers.
     * Note: disabled cache won't report any statistics.
     */
    public EvictableCacheBuilder<K, V> shareNothingWhenDisabled()
    {
        checkState(!disabledCacheImplementation.isPresent(), "disabledCacheImplementation already set");
        disabledCacheImplementation = Optional.of(DisabledCacheImplementation.NOOP);
        return this;
    }

    @CheckReturnValue
    public <K1 extends K, V1 extends V> Cache<K1, V1> build()
    {
        return build(unimplementedCacheLoader());
    }

    @CheckReturnValue
    public <K1 extends K, V1 extends V> LoadingCache<K1, V1> build(CacheLoader<? super K1, V1> loader)
    {
        if (cacheDisabled()) {
            // Silently providing a behavior different from Guava's could be surprising, so require explicit choice.
            DisabledCacheImplementation disabledCacheImplementation = this.disabledCacheImplementation.orElseThrow(() -> new IllegalStateException(
                    "Even when cache is disabled, the loads are synchronized and both load results and failures are shared between threads. " +
                            "This is rarely desired, thus builder caller is expected to either opt-in into this behavior with shareResultsAndFailuresEvenIfDisabled(), " +
                            "or choose not to share results (and failures) between concurrent invocations with shareNothingWhenDisabled()."));
            switch (disabledCacheImplementation) {
                case NOOP:
                    return new EmptyCache<>(loader, recordStats);
                case GUAVA:
                    // Disabled cache is always empty, so doesn't exhibit invalidation problems.
                    // Avoid overhead of EvictableCache wrapper.
                    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                            .maximumSize(0)
                            .expireAfterWrite(0, SECONDS);
                    if (recordStats) {
                        cacheBuilder.recordStats();
                    }
                    return buildUnsafeCache(cacheBuilder, loader);
            }
            throw new UnsupportedOperationException("Unsupported option: " + disabledCacheImplementation);
        }

        if (!(maximumSize.isPresent() || maximumWeight.isPresent() || expireAfterWrite.isPresent())) {
            // EvictableCache invalidation (e.g. invalidateAll) happening concurrently with a load may
            // lead to an entry remaining in the cache, without associated token. This would lead to
            // a memory leak in an unbounded cache.
            throw new IllegalStateException("Unbounded cache is not supported");
        }

        // CacheBuilder is further modified in EvictableCache::new, so cannot be shared between build() calls.
        CacheBuilder<Object, ? super V> cacheBuilder = CacheBuilder.newBuilder();
        expireAfterWrite.ifPresent(cacheBuilder::expireAfterWrite);
        refreshAfterWrite.ifPresent(cacheBuilder::refreshAfterWrite);
        maximumSize.ifPresent(cacheBuilder::maximumSize);
        maximumWeight.ifPresent(cacheBuilder::maximumWeight);
        weigher.ifPresent(cacheBuilder::weigher);
        if (recordStats) {
            cacheBuilder.recordStats();
        }
        return new EvictableCache<>(cacheBuilder, loader);
    }

    private boolean cacheDisabled()
    {
        return (maximumSize.isPresent() && maximumSize.get() == 0) ||
                (expireAfterWrite.isPresent() && expireAfterWrite.get().isZero());
    }

    @SuppressModernizer // CacheBuilder.build(CacheLoader) is forbidden, advising to use this class as a safety-adding wrapper.
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    private static <K, V> CacheLoader<K, V> unimplementedCacheLoader()
    {
        return CacheLoader.from(ignored -> {
            throw new UnsupportedOperationException();
        });
    }

    private static final class TokenWeigher<K, V>
            implements Weigher<Token<K>, V>
    {
        private final Weigher<? super K, ? super V> delegate;

        public TokenWeigher(Weigher<? super K, ? super V> delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public int weigh(Token<K> key, V value)
        {
            return delegate.weigh(key.getKey(), value);
        }
    }

    private static Duration toDuration(long duration, TimeUnit unit)
    {
        // Saturated conversion, as in com.google.common.cache.CacheBuilder.toNanosSaturated
        return Duration.ofNanos(unit.toNanos(duration));
    }

    private enum DisabledCacheImplementation
    {
        NOOP,
        GUAVA,
    }
}
