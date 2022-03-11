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

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public final class CacheUtils
{
    private CacheUtils() {}

    public static <K, V> V uncheckedCacheGet(Cache<K, V> cache, K key, Supplier<V> loader)
    {
        try {
            return cache.get(key, loader::get);
        }
        catch (ExecutionException e) {
            // this can not happen because a supplier can not throw a checked exception
            throw new RuntimeException("Unexpected checked exception from cache load", e);
        }
    }
}
