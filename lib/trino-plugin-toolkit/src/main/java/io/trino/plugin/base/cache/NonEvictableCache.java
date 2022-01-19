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

/**
 * A {@link com.google.common.cache.Cache} that does not support eviction.
 */
public interface NonEvictableCache<K, V>
        extends Cache<K, V>
{
    /**
     * @deprecated Not supported. Use {@link EvictableCache} cache implementation instead.
     */
    @Deprecated
    @Override
    void invalidate(Object key);

    /**
     * @deprecated Not supported. Use {@link EvictableCache} cache implementation instead.
     */
    @Deprecated
    @Override
    void invalidateAll(Iterable<?> keys);

    /**
     * @deprecated Not supported. Use {@link EvictableCache} cache implementation instead.
     */
    @Deprecated
    @Override
    void invalidateAll();
}
