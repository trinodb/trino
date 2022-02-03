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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.Cache;
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

import javax.annotation.CheckForNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A {@link Cache} and {@link LoadingCache} implementation similar to ones produced by {@link CacheBuilder#build()},
 * but one that does not exhibit <a href="https://github.com/google/guava/issues/1881">Guava issue #1881</a>:
 * a cache inspection with {@link #getIfPresent(Object)} or {@link #get(Object, Callable)} is guaranteed to return
 * fresh state after {@link #invalidate(Object)}, {@link #invalidateAll(Iterable)} or {@link #invalidateAll()} were called.
 *
 * @see EvictableCacheBuilder
 */
class EvictableCache<K, V>
        extends AbstractLoadingCache<K, V>
        implements LoadingCache<K, V>
{
    // Invariant: for every (K, token) entry in the tokens map, there is a live
    // cache entry (token, ?) in dataCache, that, upon eviction, will cause the tokens'
    // entry to be removed.
    private final ConcurrentHashMap<K, Token<K>> tokens = new ConcurrentHashMap<>();
    // The dataCache can have entries with no corresponding tokens in the tokens map.
    // For example, this can happen when invalidation concurs with load.
    // The dataCache must be bounded.
    private final LoadingCache<Token<K>, V> dataCache;

    EvictableCache(CacheBuilder<? super Token<K>, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        dataCache = buildUnsafeCache(
                cacheBuilder
                        .<Token<K>, V>removalListener(removal -> {
                            Token<K> token = removal.getKey();
                            verify(token != null, "token is null");
                            tokens.remove(token.getKey(), token);
                        }),
                new TokenCacheLoader<>(cacheLoader));
    }

    @SuppressModernizer // CacheBuilder.build(CacheLoader) is forbidden, advising to use this class as a safety-adding wrapper.
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    @CheckForNull
    @Override
    public V getIfPresent(Object key)
    {
        @SuppressWarnings("SuspiciousMethodCalls") // Object passed to map as key K
        Token<K> token = tokens.get(key);
        if (token == null) {
            return null;
        }
        return dataCache.getIfPresent(token);
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader)
            throws ExecutionException
    {
        Token<K> newToken = new Token<>(key);
        Token<K> token = tokens.computeIfAbsent(key, ignored -> newToken);
        try {
            return dataCache.get(token, valueLoader);
        }
        catch (Throwable e) {
            if (newToken == token) {
                // Failed to load and it was our new token persisted in tokens map.
                // No cache entry exists for the token (unless concurrent load happened),
                // so we need to remove it.
                tokens.remove(key, newToken);
            }
            throw e;
        }
    }

    @Override
    public V get(K key)
            throws ExecutionException
    {
        Token<K> newToken = new Token<>(key);
        Token<K> token = tokens.computeIfAbsent(key, ignored -> newToken);
        try {
            return dataCache.get(token);
        }
        catch (Throwable e) {
            if (newToken == token) {
                // Failed to load and it was our new token persisted in tokens map.
                // No cache entry exists for the token (unless concurrent load happened),
                // so we need to remove it.
                tokens.remove(key, newToken);
            }
            throw e;
        }
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys)
            throws ExecutionException
    {
        List<Token<K>> newTokens = new ArrayList<>();
        try {
            BiMap<K, Token<K>> keyToToken = HashBiMap.create();
            for (K key : keys) {
                // This is not bulk, but is fast local operation
                Token<K> newToken = new Token<>(key);
                Token<K> token = tokens.computeIfAbsent(key, ignored -> newToken);
                keyToToken.put(key, token);
                if (token == newToken) {
                    newTokens.add(newToken);
                }
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
        catch (Throwable e) {
            for (Token<K> token : newTokens) {
                // Failed to load and it was our new token persisted in tokens map.
                // No cache entry exists for the token (unless concurrent load happened),
                // so we need to remove it.
                tokens.remove(token.getKey(), token);
            }
            throw e;
        }
    }

    @Override
    public void refresh(K key)
    {
        // The refresh loads a new entry, if it wasn't in the cache yet. Thus, we would create a new Token.
        // However, dataCache.refresh is asynchronous and may fail, so no cache entry may be created.
        // In such case we would leak the newly created token.
        throw new UnsupportedOperationException();
    }

    @Override
    public long size()
    {
        return dataCache.size();
    }

    @Override
    public void cleanUp()
    {
        dataCache.cleanUp();
    }

    @VisibleForTesting
    int tokensCount()
    {
        return tokens.size();
    }

    @Override
    public void invalidate(Object key)
    {
        @SuppressWarnings("SuspiciousMethodCalls") // Object passed to map as key K
        Token<K> token = tokens.remove(key);
        if (token != null) {
            dataCache.invalidate(token);
        }
    }

    @Override
    public void invalidateAll()
    {
        dataCache.invalidateAll();
        tokens.clear();
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
            private final ConcurrentMap<Token<K>, V> dataCacheMap = dataCache.asMap();

            @Override
            public V putIfAbsent(K key, V value)
            {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races with cache invalidation");
            }

            @Override
            public boolean remove(Object key, Object value)
            {
                @SuppressWarnings("SuspiciousMethodCalls") // Object passed to map as key K
                Token<K> token = tokens.get(key);
                if (token != null) {
                    return dataCacheMap.remove(token, value);
                }
                return false;
            }

            @Override
            public boolean replace(K key, V oldValue, V newValue)
            {
                Token<K> token = tokens.get(key);
                if (token != null) {
                    return dataCacheMap.replace(token, oldValue, newValue);
                }
                return false;
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
                return tokens.containsKey(key);
            }

            @Override
            public boolean containsValue(Object value)
            {
                return values().contains(value);
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
                Token<K> token = tokens.remove(key);
                if (token != null) {
                    return dataCacheMap.remove(token);
                }
                return null;
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
                tokens.clear();
            }

            @Override
            public Set<K> keySet()
            {
                return tokens.keySet();
            }

            @Override
            public Collection<V> values()
            {
                return dataCacheMap.values();
            }

            @Override
            public Set<Map.Entry<K, V>> entrySet()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    // instance-based equality
    static final class Token<K>
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
            return format("CacheToken(%s; %s)", Integer.toHexString(hashCode()), key);
        }
    }

    private static class TokenCacheLoader<K, V>
            extends CacheLoader<Token<K>, V>
    {
        private final CacheLoader<? super K, V> delegate;

        public TokenCacheLoader(CacheLoader<? super K, V> delegate)
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
            Map<? super K, V> values = delegate.loadAll(keys);

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
