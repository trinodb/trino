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

import com.google.common.cache.Cache;
import com.google.common.cache.ForwardingCache;

import static java.util.Objects.requireNonNull;

// package-private. The interface provides deprecation and javadoc to help at call sites
class NonKeyEvictableCacheImpl<K, V>
        extends ForwardingCache<K, V>
        implements NonKeyEvictableCache<K, V>
{
    private final Cache<K, V> delegate;

    NonKeyEvictableCacheImpl(Cache<K, V> delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected Cache<K, V> delegate()
    {
        return delegate;
    }

    @Override
    public void invalidate(Object key)
    {
        throw new UnsupportedOperationException("invalidate(key) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. " +
                "Use EvictableCache if you need invalidation");
    }

    @Override
    public void invalidateAll(Iterable<?> keys)
    {
        throw new UnsupportedOperationException("invalidateAll(keys) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. " +
                "Use EvictableCache if you need invalidation");
    }
}
