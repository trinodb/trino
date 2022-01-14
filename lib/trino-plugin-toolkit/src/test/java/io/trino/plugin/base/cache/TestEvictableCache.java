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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import io.airlift.concurrent.MoreFutures;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestEvictableCache
{
    private static final int TEST_TIMEOUT_MILLIS = 10_000;

    private enum Invalidation
    {
        INVALIDATE_KEY,
        INVALIDATE_PREDEFINED_KEYS,
        INVALIDATE_SELECTED_KEYS,
        INVALIDATE_ALL,
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testLoad()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCache.buildWith(
                CacheBuilder.newBuilder());

        assertEquals(cache.stats(), new CacheStats(0, 0, 0, 0, 0, 0));
        String value = cache.get(42, () -> "abc");
        assertEquals(cache.stats(), new CacheStats(0, 1, 1, 0, cache.stats().totalLoadTime(), 0));
        assertEquals(value, "abc");
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS, dataProvider = "invalidations")
    public void testInvalidateOngoingLoad(Invalidation invalidation)
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());
        Integer key = 42;

        CountDownLatch loadOngoing = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);
        ExecutorService executor = newFixedThreadPool(2);
        try {
            // thread A
            Future<String> threadA = executor.submit(() -> {
                return cache.get(key, () -> {
                    loadOngoing.countDown(); // 1
                    assertTrue(invalidated.await(10, SECONDS)); // 2
                    return "stale value";
                });
            });

            // thread B
            Future<String> threadB = executor.submit(() -> {
                assertTrue(loadOngoing.await(10, SECONDS)); // 1

                switch (invalidation) {
                    case INVALIDATE_KEY:
                        cache.invalidate(key);
                        break;
                    case INVALIDATE_PREDEFINED_KEYS:
                        cache.invalidateAll(List.of(key));
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
    @Test(invocationCount = 10, timeOut = TEST_TIMEOUT_MILLIS, dataProvider = "invalidations")
    public void testInvalidateAndLoadConcurrently(Invalidation invalidation)
            throws Exception
    {
        int[] primes = {2, 3, 5, 7};
        AtomicLong remoteState = new AtomicLong(1);

        Cache<Integer, Long> cache = EvictableCache.buildWith(CacheBuilder.newBuilder());
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
                                cache.invalidateAll(List.of(key));
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

    @DataProvider
    public static Object[][] invalidations()
    {
        return Stream.of(Invalidation.values())
                .map(invalidation -> new Object[] {invalidation})
                .toArray(Object[][]::new);
    }
}
