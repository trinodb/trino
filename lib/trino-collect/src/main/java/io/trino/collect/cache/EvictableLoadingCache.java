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

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link LoadingCache} implementation similar to ones produced by {@link CacheBuilder#build(CacheLoader)},
 * but one that does not exhibit <a href="https://github.com/google/guava/issues/1881">Guava issue #1881</a>:
 * a cache inspection with {@link #getIfPresent(Object)} or {@link #get(Object)} is guaranteed to return fresh
 * state after {@link #invalidate(Object)}, {@link #invalidateAll(Iterable)} or {@link #invalidateAll()} were called.
 */
public class EvictableLoadingCache<K, V>
        extends AbstractLoadingCache<K, V>
{
    public static <K, V> LoadingCache<K, V> build(
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMillis,
            long maximumSize,
            boolean recordStats,
            CacheLoader<K, V> cacheLoader)
    {
        requireNonNull(cacheLoader, "cacheLoader is null");

        CacheBuilder<Object, Object> tokenCache = CacheBuilder.newBuilder();
        CacheBuilder<Object, Object> dataCache = CacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            tokenCache.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
            dataCache.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }

        if (refreshMillis.isPresent() && (!expiresAfterWriteMillis.isPresent() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            dataCache.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
        }

        tokenCache.maximumSize(maximumSize);
        dataCache.maximumSize(maximumSize);

        if (recordStats) {
            dataCache.recordStats();
        }

        return new EvictableLoadingCache<>(
                EvictableCache.buildWith(tokenCache),
                buildUnsafeCache(dataCache, new TokenCacheLoader<>(cacheLoader)));
    }

    @SuppressModernizer // CacheBuilder.build(CacheLoader) is forbidden, advising to use this class as a safety-adding wrapper.
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    // Token<K> is a freshness marker. tokenCache keeps the fresh token for given key.
    // Cache invalidation correctness (read-after-invalidate consistency) relies on tokenCache
    // invalidation.
    private final EvictableCache<K, Token<K>> tokensCache;

    // dataCache keeps data for tokens, potentially including stale entries.
    // dataCache invalidation is opportunistic. It's necessary for releasing memory, but
    // is *not* necessary for correctness (read-after-invalidate consistency).
    private final LoadingCache<Token<K>, V> dataCache;

    private EvictableLoadingCache(EvictableCache<K, Token<K>> tokensCache, LoadingCache<Token<K>, V> dataCache)
    {
        this.tokensCache = requireNonNull(tokensCache, "tokensCache is null");
        this.dataCache = requireNonNull(dataCache, "dataCache is null");
    }

    @Override
    public V get(K key)
            throws ExecutionException
    {
        Token<K> token = tokensCache.get(key, () -> new Token<>(key));
        return dataCache.get(token);
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys)
            throws ExecutionException
    {
        BiMap<K, Token<K>> keyToToken = HashBiMap.create();
        for (K key : keys) {
            // This is not bulk, but is fast local operation
            keyToToken.put(key, tokensCache.get(key, () -> new Token<>(key)));
        }

        Map<Token<K>, V> values = dataCache.getAll(keyToToken.values());

        BiMap<Token<K>, K> tokenToKey = keyToToken.inverse();
        ImmutableMap.Builder<K, V> result = ImmutableMap.builder();
        for (Map.Entry<Token<K>, V> entry : values.entrySet()) {
            Token<K> token = entry.getKey();

            // While token.getKey() returns equal key, a caller may expect us to maintain key identity, in case equal keys are still distinguishable.
            K key = tokenToKey.get(token);
            checkState(key != null, "No key found for %s in %s when loading %s", token, tokenToKey, keys);

            result.put(key, entry.getValue());
        }
        return result.buildOrThrow();
    }

    @Override
    public V getIfPresent(Object key)
    {
        Token<K> token = tokensCache.getIfPresent(key);
        if (token == null) {
            return null;
        }
        return dataCache.getIfPresent(token);
    }

    @Override
    public void invalidate(Object key)
    {
        Token<K> token = tokensCache.getIfPresent(key);
        // Invalidate irrespective of whether we got a token, to invalidate result of ongoing token load (instantiation).
        tokensCache.invalidate(key);
        if (token != null) {
            dataCache.invalidate(token);
        }
    }

    @Override
    public void invalidateAll(Iterable<?> keys)
    {
        ImmutableMap<K, Token<K>> tokens = tokensCache.getAllPresent(keys);
        tokensCache.invalidateAll(keys);
        // This is racy with respect to concurrent cache loading. dataCache is not guaranteed not to have entries for keys after invalidateAll(keys) returns.
        // Note that dataCache invalidation is not necessary for correctness. Still one can expect that cache invalidation releases memory.
        dataCache.invalidateAll(tokens.values());
    }

    @Override
    public void invalidateAll()
    {
        tokensCache.invalidateAll();
        dataCache.invalidateAll();
    }

    @Override
    public long size()
    {
        return dataCache.size();
    }

    @Override
    public CacheStats stats()
    {
        return dataCache.stats();
    }

    @Override
    public ConcurrentMap<K, V> asMap()
    {
        return new ConcurrentMap<K, V>()
        {
            private final ConcurrentMap<K, Token<K>> tokenCacheMap = tokensCache.asMap();
            private final ConcurrentMap<Token<K>, V> dataCacheMap = dataCache.asMap();

            @Override
            public V putIfAbsent(K key, V value)
            {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation");
            }

            @Override
            public boolean remove(Object key, Object value)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean replace(K key, V oldValue, V newValue)
            {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation");
            }

            @Override
            public V replace(K key, V value)
            {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation");
            }

            @Override
            public int size()
            {
                return dataCache.asMap().size();
            }

            @Override
            public boolean isEmpty()
            {
                return dataCache.asMap().isEmpty();
            }

            @Override
            public boolean containsKey(Object key)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsValue(Object value)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public V get(Object key)
            {
                return getIfPresent(key);
            }

            @Override
            public V put(K key, V value)
            {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation. Use get(key, callable) instead.");
            }

            @Override
            public V remove(Object key)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m)
            {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation. Use get(key, callable) instead.");
            }

            @Override
            public void clear()
            {
                dataCacheMap.clear();
                tokenCacheMap.clear();
            }

            @Override
            public Set<K> keySet()
            {
                return tokenCacheMap.keySet();
            }

            @Override
            public Collection<V> values()
            {
                return dataCacheMap.values();
            }

            @Override
            public Set<Entry<K, V>> entrySet()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void cleanUp()
    {
        tokensCache.cleanUp();
        dataCache.cleanUp();
    }

    // instance-based equality
    private static class Token<K>
    {
        private final K key;

        Token(K key)
        {
            this.key = requireNonNull(key, "key is null");
        }

        K getKey()
        {
            return key;
        }

        @Override
        public String toString()
        {
            return format("Cache Token(%s)", key);
        }
    }

    private static class TokenCacheLoader<K, V>
            extends CacheLoader<Token<K>, V>
    {
        private final CacheLoader<K, V> delegate;

        public TokenCacheLoader(CacheLoader<K, V> delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public V load(Token<K> token)
                throws Exception
        {
            return delegate.load(token.getKey());
        }

        @Override
        public ListenableFuture<V> reload(Token<K> token, V oldValue)
                throws Exception
        {
            return delegate.reload(token.getKey(), oldValue);
        }

        @Override
        public Map<Token<K>, V> loadAll(Iterable<? extends Token<K>> tokens)
                throws Exception
        {
            List<Token<K>> tokenList = ImmutableList.copyOf(tokens);
            List<K> keys = new ArrayList<>();
            for (Token<K> token : tokenList) {
                keys.add(token.getKey());
            }
            Map<K, V> values = delegate.loadAll(keys);

            ImmutableMap.Builder<Token<K>, V> result = ImmutableMap.builder();
            for (int i = 0; i < tokenList.size(); i++) {
                Token<K> token = tokenList.get(i);
                K key = keys.get(i);
                V value = values.get(key);
                // CacheLoader.loadAll is not guaranteed to return values for all the keys
                if (value != null) {
                    result.put(token, value);
                }
            }
            return result.buildOrThrow();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .addValue(delegate)
                    .toString();
        }
    }
}
