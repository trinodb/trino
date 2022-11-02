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

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ForwardingConcurrentMap;

import java.util.concurrent.ConcurrentMap;

// package-private. The interface provides deprecation and javadoc to help at call sites
final class NonEvictableLoadingCacheImpl<K, V>
        extends NonKeyEvictableLoadingCacheImpl<K, V>
        implements NonEvictableLoadingCache<K, V>
{
    NonEvictableLoadingCacheImpl(LoadingCache<K, V> delegate)
    {
        super(delegate);
    }

    @Override
    public void invalidateAll()
    {
        throw new UnsupportedOperationException("invalidateAll does not invalidate ongoing loads, so a stale value may remain in the cache for ever. " +
                "Use EvictableCache if you need invalidation, or use SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll() " +
                "if invalidateAll is not required for correctness");
    }

    @Override
    public ConcurrentMap<K, V> asMap()
    {
        ConcurrentMap<K, V> map = super.asMap();
        return new ForwardingConcurrentMap<K, V>()
        {
            @Override
            protected ConcurrentMap<K, V> delegate()
            {
                return map;
            }

            @Override
            public void clear()
            {
                throw new UnsupportedOperationException("clear() does not invalidate ongoing loads, so a stale value may remain in the cache for ever. " +
                        "Use EvictableCacheBuilder if you need invalidation");
            }
        };
    }
}
