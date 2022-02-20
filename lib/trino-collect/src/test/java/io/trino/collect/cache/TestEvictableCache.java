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

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.collect.cache.CacheStatsAssertions.assertCacheStats;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestEvictableCache
{
    private static final int TEST_TIMEOUT_MILLIS = 10_000;

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testLoad()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();
        assertEquals(cache.get(42, () -> "abc"), "abc");
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testEvictBySize()
            throws Exception
    {
        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10)
                .build();

        for (int i = 0; i < 10_000; i++) {
            int value = i * 10;
            assertEquals((Object) cache.get(i, () -> value), value);
        }
        cache.cleanUp();
        assertEquals(cache.size(), 10);
        assertEquals(((EvictableCache<?, ?>) cache).tokensCount(), 10);
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testEvictByWeight()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(20)
                .weigher((Integer key, String value) -> value.length())
                .build();

        for (int i = 0; i < 10; i++) {
            String value = Strings.repeat("a", i);
            assertEquals((Object) cache.get(i, () -> value), value);
        }
        cache.cleanUp();
        // It's not deterministic which entries get evicted
        int cacheSize = toIntExact(cache.size());
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(cacheSize);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(cacheSize);
        assertThat(cache.asMap().keySet().stream().mapToInt(i -> i).sum()).as("key sum").isLessThanOrEqualTo(20);
        assertThat(cache.asMap().values()).as("values").hasSize(cacheSize);
        assertThat(cache.asMap().values().stream().mapToInt(String::length).sum()).as("values length sum").isLessThanOrEqualTo(20);
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testReplace()
            throws Exception
    {
        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10)
                .build();

        int key = 10;
        int initialValue = 20;
        int replacedValue = 21;
        cache.get(key, () -> initialValue);
        assertTrue(cache.asMap().replace(key, initialValue, replacedValue));
        assertEquals((Object) cache.getIfPresent(key), replacedValue);

        // already replaced, current value is different
        assertFalse(cache.asMap().replace(key, initialValue, replacedValue));
        assertEquals((Object) cache.getIfPresent(key), replacedValue);

        // non-existent key
        assertFalse(cache.asMap().replace(100000, replacedValue, 22));
        assertEquals(cache.asMap().keySet(), ImmutableSet.of(key));
        assertEquals((Object) cache.getIfPresent(key), replacedValue);

        int anotherKey = 13;
        int anotherInitialValue = 14;
        cache.get(anotherKey, () -> anotherInitialValue);
        cache.invalidate(anotherKey);
        // after eviction
        assertFalse(cache.asMap().replace(anotherKey, anotherInitialValue, 15));
        assertEquals(cache.asMap().keySet(), ImmutableSet.of(key));
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testDisabledCache()
            throws Exception
    {
        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(0)
                .build();

        for (int i = 0; i < 10; i++) {
            int value = i * 10;
            assertEquals((Object) cache.get(i, () -> value), value);
        }
        cache.cleanUp();
        assertEquals(cache.size(), 0);
        assertThat(cache.asMap().keySet()).as("keySet").isEmpty();
        assertThat(cache.asMap().values()).as("values").isEmpty();
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testLoadStats()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build();

        assertEquals(cache.stats(), new CacheStats(0, 0, 0, 0, 0, 0));

        String value = assertCacheStats(cache)
                .misses(1)
                .loads(1)
                .calling(() -> cache.get(42, () -> "abc"));
        assertEquals(value, "abc");

        value = assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(42, () -> "xyz"));
        assertEquals(value, "abc");

        // with equal, but not the same key
        value = assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(newInteger(42), () -> "xyz"));
        assertEquals(value, "abc");
    }

    @SuppressModernizer
    private static Integer newInteger(int value)
    {
        Integer integer = value;
        @SuppressWarnings({"UnnecessaryBoxing", "deprecation", "BoxedPrimitiveConstructor"})
        Integer newInteger = new Integer(value);
        assertNotSame(integer, newInteger);
        return newInteger;
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS, dataProviderClass = Invalidation.class, dataProvider = "invalidations")
    public void testInvalidateOngoingLoad(Invalidation invalidation)
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();
        Integer key = 42;

        CountDownLatch loadOngoing = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);
        CountDownLatch getReturned = new CountDownLatch(1);
        ExecutorService executor = newFixedThreadPool(2);
        try {
            // thread A
            Future<String> threadA = executor.submit(() -> {
                String value = cache.get(key, () -> {
                    loadOngoing.countDown(); // 1
                    assertTrue(invalidated.await(10, SECONDS)); // 2
                    return "stale value";
                });
                getReturned.countDown(); // 3
                return value;
            });

            // thread B
            Future<String> threadB = executor.submit(() -> {
                assertTrue(loadOngoing.await(10, SECONDS)); // 1

                switch (invalidation) {
                    case INVALIDATE_KEY:
                        cache.invalidate(key);
                        break;
                    case INVALIDATE_PREDEFINED_KEYS:
                        cache.invalidateAll(ImmutableList.of(key));
                        break;
                    case INVALIDATE_SELECTED_KEYS:
                        Set<Integer> keys = cache.asMap().keySet().stream()
                                .filter(foundKey -> (int) foundKey == key)
                                .collect(toImmutableSet());
                        cache.invalidateAll(keys);
                        break;
                    case INVALIDATE_ALL:
                        cache.invalidateAll();
                        break;
                }

                invalidated.countDown(); // 2
                // Cache may persist value after loader returned, but before `cache.get(...)` returned. Ensure the latter completed.
                assertTrue(getReturned.await(10, SECONDS)); // 3

                return cache.get(key, () -> "fresh value");
            });

            assertEquals(threadA.get(), "stale value");
            assertEquals(threadB.get(), "fresh value");
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test(invocationCount = 10, timeOut = TEST_TIMEOUT_MILLIS, dataProviderClass = Invalidation.class, dataProvider = "invalidations")
    public void testInvalidateAndLoadConcurrently(Invalidation invalidation)
            throws Exception
    {
        int[] primes = {2, 3, 5, 7};
        AtomicLong remoteState = new AtomicLong(1);

        Cache<Integer, Long> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();
        Integer key = 42;
        int threads = 4;

        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            List<Future<Void>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        // prime the cache
                        assertEquals((long) cache.get(key, remoteState::get), 1L);
                        int prime = primes[threadNumber];

                        barrier.await(10, SECONDS);

                        // modify underlying state
                        remoteState.updateAndGet(current -> current * prime);

                        // invalidate
                        switch (invalidation) {
                            case INVALIDATE_KEY:
                                cache.invalidate(key);
                                break;
                            case INVALIDATE_PREDEFINED_KEYS:
                                cache.invalidateAll(ImmutableList.of(key));
                                break;
                            case INVALIDATE_SELECTED_KEYS:
                                Set<Integer> keys = cache.asMap().keySet().stream()
                                        .filter(foundKey -> (int) foundKey == key)
                                        .collect(toImmutableSet());
                                cache.invalidateAll(keys);
                                break;
                            case INVALIDATE_ALL:
                                cache.invalidateAll();
                                break;
                        }

                        // read through cache
                        long current = cache.get(key, remoteState::get);
                        if (current % prime != 0) {
                            fail(format("The value read through cache (%s) in thread (%s) is not divisable by (%s)", current, threadNumber, prime));
                        }

                        return (Void) null;
                    }))
                    .collect(toImmutableList());

            futures.forEach(MoreFutures::getFutureValue);

            assertEquals(remoteState.get(), 2 * 3 * 5 * 7);
            assertEquals((long) cache.get(key, remoteState::get), remoteState.get());
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
