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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.collect.cache.CacheStatsAssertions.assertCacheStats;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestEvictableLoadingCache
{
    private static final int TEST_TIMEOUT_MILLIS = 10_000;

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testLoad()
            throws Exception
    {
        LoadingCache<Integer, String> cache = EvictableLoadingCache.build(
                OptionalLong.empty(),
                OptionalLong.empty(),
                10_000,
                true,
                CacheLoader.from((Integer ignored) -> "abc"));

        assertEquals(cache.get(42), "abc");
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testLoadStats()
            throws Exception
    {
        LoadingCache<Integer, String> cache = EvictableLoadingCache.build(
                OptionalLong.empty(),
                OptionalLong.empty(),
                10_000,
                true,
                CacheLoader.from((Integer ignored) -> "abc"));

        assertEquals(cache.stats(), new CacheStats(0, 0, 0, 0, 0, 0));

        String value = assertCacheStats(cache)
                .misses(1)
                .loads(1)
                .calling(() -> cache.get(42));
        assertEquals(value, "abc");

        value = assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(42));
        assertEquals(value, "abc");

        // with equal, but not the same key
        value = assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(newInteger(42)));
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
     * Verity that implementation of {@link LoadingCache#getAll(Iterable)} returns same keys as provided, not equal ones.
     * This is necessary for the case where the cache key can be equal but still distinguishable.
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testGetAllMaintainsKeyIdentity()
            throws Exception
    {
        LoadingCache<String, Integer> cache = EvictableLoadingCache.build(
                OptionalLong.empty(),
                OptionalLong.empty(),
                10_000,
                true,
                CacheLoader.from(String::length));

        String first = "abc";
        String second = new String(first);
        assertNotSame(first, second);

        // prime the cache
        assertEquals((int) cache.get(first), 3);

        Map<String, Integer> values = cache.getAll(ImmutableList.of(second));
        assertThat(values).hasSize(1);
        Entry<String, Integer> entry = getOnlyElement(values.entrySet());
        assertEquals((int) entry.getValue(), 3);
        assertEquals(entry.getKey(), first);
        assertEquals(entry.getKey(), second);
        assertNotSame(entry.getKey(), first);
        assertSame(entry.getKey(), second);
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS, dataProviderClass = Invalidation.class, dataProvider = "invalidations")
    public void testInvalidateOngoingLoad(Invalidation invalidation)
            throws Exception
    {
        ConcurrentMap<Integer, String> remoteState = new ConcurrentHashMap<>();
        Integer key = 42;
        remoteState.put(key, "stale value");

        CountDownLatch loadOngoing = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);
        CountDownLatch getReturned = new CountDownLatch(1);

        LoadingCache<Integer, String> cache = EvictableLoadingCache.build(
                OptionalLong.empty(),
                OptionalLong.empty(),
                10_000,
                false,
                new CacheLoader<Integer, String>()
                {
                    @Override
                    public String load(Integer key)
                            throws Exception
                    {
                        String value = remoteState.get(key);
                        loadOngoing.countDown(); // 1
                        assertTrue(invalidated.await(10, SECONDS)); // 2
                        return value;
                    }
                });

        ExecutorService executor = newFixedThreadPool(2);
        try {
            // thread A
            Future<String> threadA = executor.submit(() -> {
                String value = cache.get(key);
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

                remoteState.put(key, "fresh value");
                invalidated.countDown(); // 2
                // Cache may persist value after loader returned, but before `cache.get(...)` returned. Ensure the latter completed.
                assertTrue(getReturned.await(10, SECONDS)); // 3

                return cache.get(key);
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

        Integer key = 42;
        Map<Integer, AtomicLong> remoteState = new ConcurrentHashMap<>();
        remoteState.put(key, new AtomicLong(1));

        LoadingCache<Integer, Long> cache = EvictableLoadingCache.build(
                OptionalLong.empty(),
                OptionalLong.empty(),
                10_000,
                false,
                CacheLoader.from(i -> remoteState.get(i).get()));

        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            List<Future<Void>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        // prime the cache
                        assertEquals((long) cache.get(key), 1L);
                        int prime = primes[threadNumber];

                        barrier.await(10, SECONDS);

                        // modify underlying state
                        remoteState.get(key).updateAndGet(current -> current * prime);

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
                        long current = cache.get(key);
                        if (current % prime != 0) {
                            fail(format("The value read through cache (%s) in thread (%s) is not divisable by (%s)", current, threadNumber, prime));
                        }

                        return (Void) null;
                    }))
                    .collect(toImmutableList());

            futures.forEach(MoreFutures::getFutureValue);

            assertEquals(remoteState.keySet(), ImmutableSet.of(key));
            assertEquals(remoteState.get(key).get(), 2 * 3 * 5 * 7);
            assertEquals((long) cache.get(key), 2 * 3 * 5 * 7);
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
