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
package io.trino.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertSame;

public class TestSafeCaches
{
    @Test
    public void testNonEvictableCache()
            throws Exception
    {
        NonEvictableCache<Object, Object> cache = buildNonEvictableCache(CacheBuilder.newBuilder());
        verifyKeyInvalidationIsImpossible(cache);
        verifyClearIsImpossible(cache);
        verifyLoadingIsPossible(cache);
    }

    @Test
    public void testNonEvictableCacheWithWeakInvalidateAll()
            throws Exception
    {
        NonKeyEvictableCache<Object, Object> cache = buildNonEvictableCacheWithWeakInvalidateAll(CacheBuilder.newBuilder());
        verifyKeyInvalidationIsImpossible(cache);
        verifyClearIsPossible(cache);
        verifyLoadingIsPossible(cache);
    }

    @Test
    public void testNonEvictableLoadingCache()
            throws Exception
    {
        NonEvictableLoadingCache<Object, Object> cache = buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(key -> key));
        verifyKeyInvalidationIsImpossible(cache);
        verifyClearIsImpossible(cache);
        verifyLoadingIsPossible(cache);
    }

    @Test
    public void testNonEvictableLoadingCacheWithWeakInvalidateAll()
            throws Exception
    {
        NonKeyEvictableLoadingCache<Object, Object> cache = buildNonEvictableCacheWithWeakInvalidateAll(CacheBuilder.newBuilder(), CacheLoader.from(key -> key));
        verifyKeyInvalidationIsImpossible(cache);
        verifyClearIsPossible(cache);
        verifyLoadingIsPossible(cache);
    }

    private static void verifyLoadingIsPossible(Cache<Object, Object> cache)
            throws Exception
    {
        Object key = new Object();
        Object value = new Object();
        // Verify the previous load was inserted into the cache
        assertSame(cache.get(key, () -> value), value);
    }

    private static void verifyKeyInvalidationIsImpossible(Cache<Object, Object> cache)
    {
        assertThatThrownBy(() -> cache.invalidate(new Object()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("invalidate(key) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCache if you need invalidation");
        assertThatThrownBy(() -> cache.invalidateAll(ImmutableList.of()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("invalidateAll(keys) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCache if you need invalidation");
        Object object = new Object();
        assertThatThrownBy(() -> cache.asMap().remove(object))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("remove(key) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCacheBuilder if you need invalidation");
        assertThatThrownBy(() -> cache.asMap().remove(object, object))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("remove(key, value) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCacheBuilder if you need invalidation");
        assertThatThrownBy(() -> cache.asMap().replace(object, object))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("replace(key, value) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCacheBuilder if you need invalidation");
        assertThatThrownBy(() -> cache.asMap().replace(object, object, object))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("replace(key, oldValue, newValue) does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCacheBuilder if you need invalidation");
    }

    private static void verifyClearIsImpossible(Cache<Object, Object> cache)
    {
        assertThatThrownBy(cache::invalidateAll)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("invalidateAll does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCache if you need invalidation, or use SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll() if invalidateAll is not required for correctness");
        Map<Object, Object> map = cache.asMap();
        assertThatThrownBy(map::clear)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("clear() does not invalidate ongoing loads, so a stale value may remain in the cache for ever. Use EvictableCacheBuilder if you need invalidation");
    }

    private static void verifyClearIsPossible(Cache<Object, Object> cache)
            throws Exception
    {
        Object key = new Object();
        Object firstValue = new Object();
        cache.get(key, () -> firstValue);
        assertSame(cache.getIfPresent(key), firstValue);

        cache.invalidateAll();
        assertThat(cache.getIfPresent(key)).isNull();

        Object secondValue = new Object();
        cache.get(key, () -> secondValue);
        assertSame(cache.getIfPresent(key), secondValue);
        cache.asMap().clear();
        assertThat(cache.getIfPresent(key)).isNull();
    }
}
