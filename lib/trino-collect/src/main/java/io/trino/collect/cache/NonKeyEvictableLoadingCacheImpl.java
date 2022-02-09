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

import com.google.common.cache.ForwardingLoadingCache;
import com.google.common.cache.LoadingCache;

import static java.util.Objects.requireNonNull;

// package-private. The interface provides deprecation and javadoc to help at call sites
class NonKeyEvictableLoadingCacheImpl<K, V>
        extends ForwardingLoadingCache<K, V>
        implements NonKeyEvictableLoadingCache<K, V>
{
    private final LoadingCache<K, V> delegate;

    NonKeyEvictableLoadingCacheImpl(LoadingCache<K, V> delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected LoadingCache<K, V> delegate()
    {
        return delegate;
    }

    @Override
    public void invalidate(Object key)
    {
        throw new UnsupportedOperationException("invalidate(key) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. " +
                "Use EvictableLoadingCache if you need invalidation");
    }

    @Override
    public void invalidateAll(Iterable<?> keys)
    {
        throw new UnsupportedOperationException("invalidateAll(keys) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. " +
                "Use EvictableLoadingCache if you need invalidation");
    }
}
