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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TestingTicker;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.collect.cache.CacheStatsAssertions.assertCacheStats;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
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
        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build(CacheLoader.from((Integer ignored) -> "abc"));

        assertEquals(cache.get(42), "abc");
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testEvictBySize()
            throws Exception
    {
        int maximumSize = 10;
        AtomicInteger loads = new AtomicInteger();
        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .build(CacheLoader.from(key -> {
                    loads.incrementAndGet();
                    return "abc" + key;
                }));

        for (int i = 0; i < 10_000; i++) {
            assertEquals((Object) cache.get(i), "abc" + i);
        }
        cache.cleanUp();
        assertEquals(cache.size(), maximumSize);
        assertEquals(((EvictableCache<?, ?>) cache).tokensCount(), maximumSize);
        assertEquals(loads.get(), 10_000);

        // Ensure cache is effective, i.e. no new load
        int lastKey = 10_000 - 1;
        assertEquals((Object) cache.get(lastKey), "abc" + lastKey);
        assertEquals(loads.get(), 10_000);
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testEvictByWeight()
            throws Exception
    {
        AtomicInteger loads = new AtomicInteger();
        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(20)
                .weigher((Integer key, String value) -> value.length())
                .build(CacheLoader.from(key -> {
                    loads.incrementAndGet();
                    return Strings.repeat("a", key);
                }));

        for (int i = 0; i < 10; i++) {
            assertEquals((Object) cache.get(i), Strings.repeat("a", i));
        }
        cache.cleanUp();
        // It's not deterministic which entries get evicted
        int cacheSize = toIntExact(cache.size());
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(cacheSize);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(cacheSize);
        assertThat(cache.asMap().keySet().stream().mapToInt(i -> i).sum()).as("key sum").isLessThanOrEqualTo(20);
        assertThat(cache.asMap().values()).as("values").hasSize(cacheSize);
        assertThat(cache.asMap().values().stream().mapToInt(String::length).sum()).as("values length sum").isLessThanOrEqualTo(20);
        assertEquals(loads.get(), 10);

        // Ensure cache is effective, i.e. no new load
        int lastKey = 10 - 1;
        assertEquals((Object) cache.get(lastKey), Strings.repeat("a", lastKey));
        assertEquals(loads.get(), 10);
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testEvictByTime()
    {
        TestingTicker ticker = new TestingTicker();
        int ttl = 100;
        AtomicInteger loads = new AtomicInteger();
        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                .build(CacheLoader.from(k -> {
                    loads.incrementAndGet();
                    return k + " ala ma kota";
                }));

        assertEquals(cache.getUnchecked(1), "1 ala ma kota");
        ticker.increment(ttl, MILLISECONDS);
        assertEquals(cache.getUnchecked(2), "2 ala ma kota");
        cache.cleanUp();

        // First entry should be expired and its token removed
        int cacheSize = toIntExact(cache.size());
        assertThat(cacheSize).as("cacheSize").isEqualTo(1);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(cacheSize);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(cacheSize);
        assertThat(cache.asMap().values()).as("values").hasSize(cacheSize);
        assertEquals(loads.get(), 2);
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testPreserveValueLoadedAfterTimeExpiration()
    {
        TestingTicker ticker = new TestingTicker();
        int ttl = 100;
        AtomicInteger loads = new AtomicInteger();
        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                .build(CacheLoader.from(k -> {
                    loads.incrementAndGet();
                    return k + " ala ma kota";
                }));
        int key = 11;

        assertEquals(cache.getUnchecked(key), "11 ala ma kota");
        assertThat(loads.get()).as("initial load count").isEqualTo(1);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        // Should be served from the cache
        assertEquals(cache.getUnchecked(key), "11 ala ma kota");
        assertThat(loads.get()).as("loads count should not change before value expires").isEqualTo(1);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        ticker.increment(ttl, MILLISECONDS);
        // Should be reloaded
        assertEquals(cache.getUnchecked(key), "11 ala ma kota");
        assertThat(loads.get()).as("loads count should reflect reloading of value after expiration").isEqualTo(2);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        // Should be served from the cache
        assertEquals(cache.getUnchecked(key), "11 ala ma kota");
        assertThat(loads.get()).as("loads count should not change before value expires again").isEqualTo(2);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        assertThat(cache.size()).as("cacheSize").isEqualTo(1);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(1);
        assertThat(cache.asMap().values()).as("values").hasSize(1);
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS, dataProvider = "testDisabledCacheDataProvider")
    public void testDisabledCache(String behavior)
            throws Exception
    {
        CacheLoader<Integer, Integer> loader = CacheLoader.from(key -> key * 10);
        EvictableCacheBuilder<Object, Object> builder = EvictableCacheBuilder.newBuilder()
                .maximumSize(0);

        switch (behavior) {
            case "share-nothing":
                builder.shareNothingWhenDisabled();
                break;
            case "guava":
                builder.shareResultsAndFailuresEvenIfDisabled();
                break;
            case "none":
                assertThatThrownBy(() -> builder.build(loader))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Even when cache is disabled, the loads are synchronized and both load results and failures are shared between threads. " +
                                "This is rarely desired, thus builder caller is expected to either opt-in into this behavior with shareResultsAndFailuresEvenIfDisabled(), " +
                                "or choose not to share results (and failures) between concurrent invocations with shareNothingWhenDisabled().");
                return;
            default:
                throw new UnsupportedOperationException("Unsupported: " + behavior);
        }

        LoadingCache<Integer, Integer> cache = builder.build(loader);

        for (int i = 0; i < 10; i++) {
            assertEquals((Object) cache.get(i), i * 10);
        }
        cache.cleanUp();
        assertEquals(cache.size(), 0);
        assertThat(cache.asMap().keySet()).as("keySet").isEmpty();
        assertThat(cache.asMap().values()).as("values").isEmpty();
    }

    @DataProvider
    public static Object[][] testDisabledCacheDataProvider()
    {
        return new Object[][] {
                {"share-nothing"},
                {"guava"},
                {"none"},
        };
    }

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testLoadStats()
            throws Exception
    {
        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build(CacheLoader.from((Integer ignored) -> "abc"));

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
        LoadingCache<String, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build(CacheLoader.from(String::length));

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
     * Test that they keys provided to {@link LoadingCache#get(Object)} are not necessarily the ones provided to
     * {@link CacheLoader#load(Object)}. While guarantying this would be obviously desirable (as in
     * {@link #testGetAllMaintainsKeyIdentityForBulkLoader}), it seems not feasible to do this while
     * also maintain load sharing (see {@link #testConcurrentGetShareLoad()}).
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testGetDoesNotMaintainKeyIdentityForLoader()
            throws Exception
    {
        AtomicInteger loadCounter = new AtomicInteger();
        int firstAdditionalField = 1;
        int secondAdditionalField = 123456789;

        LoadingCache<ClassWithPartialEquals, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build(CacheLoader.from((ClassWithPartialEquals key) -> {
                    loadCounter.incrementAndGet();
                    assertEquals(key.getAdditionalField(), firstAdditionalField); // not secondAdditionalField because get() reuses existing token
                    return key.getValue();
                }));

        ClassWithPartialEquals keyA = new ClassWithPartialEquals(42, firstAdditionalField);
        ClassWithPartialEquals keyB = new ClassWithPartialEquals(42, secondAdditionalField);
        // sanity check: objects are equal despite having different observed state
        assertEquals(keyA, keyB);
        assertNotEquals(keyA.getAdditionalField(), keyB.getAdditionalField());

        // Populate the cache
        assertEquals((int) cache.get(keyA, () -> 317), 317);
        assertEquals(loadCounter.get(), 0);

        // invalidate dataCache but keep tokens -- simulate concurrent implicit or explicit eviction
        ((EvictableCache<?, ?>) cache).clearDataCacheOnly();
        assertEquals((int) cache.get(keyB), 42);
        assertEquals(loadCounter.get(), 1);
    }

    /**
     * Test that they keys provided to {@link LoadingCache#getAll(Iterable)} are the ones provided to {@link CacheLoader#loadAll(Iterable)}.
     * It is possible that {@link CacheLoader#loadAll(Iterable)} requires keys to have some special characteristics and some
     * other, equal keys, derived from {@code EvictableCache.tokens}, may not have that characteristics.
     * This can happen only when cache keys are not fully value-based. While discouraged, this situation is possible.
     * Guava Cache also exhibits the behavior tested here.
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testGetAllMaintainsKeyIdentityForBulkLoader()
            throws Exception
    {
        AtomicInteger loadAllCounter = new AtomicInteger();
        int expectedAdditionalField = 123456789;

        LoadingCache<ClassWithPartialEquals, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build(new CacheLoader<ClassWithPartialEquals, Integer>()
                {
                    @Override
                    public Integer load(ClassWithPartialEquals key)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Map<ClassWithPartialEquals, Integer> loadAll(Iterable<? extends ClassWithPartialEquals> keys)
                    {
                        loadAllCounter.incrementAndGet();
                        // For the sake of simplicity, the test currently leverages that getAll() with singleton list will
                        // end up calling loadAll() even though load() could be used.
                        ClassWithPartialEquals key = getOnlyElement(keys);
                        assertEquals(key.getAdditionalField(), expectedAdditionalField);
                        return ImmutableMap.of(key, key.getValue());
                    }
                });

        ClassWithPartialEquals keyA = new ClassWithPartialEquals(42, 1);
        ClassWithPartialEquals keyB = new ClassWithPartialEquals(42, expectedAdditionalField);
        // sanity check: objects are equal despite having different observed state
        assertEquals(keyA, keyB);
        assertNotEquals(keyA.getAdditionalField(), keyB.getAdditionalField());

        // Populate the cache
        assertEquals((int) cache.get(keyA, () -> 317), 317);
        assertEquals(loadAllCounter.get(), 0);

        // invalidate dataCache but keep tokens -- simulate concurrent implicit or explicit eviction
        ((EvictableCache<?, ?>) cache).clearDataCacheOnly();
        Map<ClassWithPartialEquals, Integer> map = cache.getAll(ImmutableList.of(keyB));
        assertThat(map).hasSize(1);
        assertSame(getOnlyElement(map.keySet()), keyB);
        assertEquals((int) getOnlyElement(map.values()), 42);
        assertEquals(loadAllCounter.get(), 1);
    }

    /**
     * Test that the loader is invoked only once for concurrent invocations of {{@link LoadingCache#get(Object, Callable)} with equal keys.
     * This is a behavior of Guava Cache as well. While this is necessarily desirable behavior (see
     * <a href="https://github.com/trinodb/trino/issues/11067">https://github.com/trinodb/trino/issues/11067</a>),
     * the test exists primarily to document current state and support discussion, should the current state change.
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testConcurrentGetWithCallableShareLoad()
            throws Exception
    {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger concurrentInvocations = new AtomicInteger();

        LoadingCache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build(CacheLoader.from(() -> {
                    throw new UnsupportedOperationException();
                }));

        int threads = 2;
        int invocationsPerThread = 100;
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    for (int invocation = 0; invocation < invocationsPerThread; invocation++) {
                        int key = invocation;
                        barrier.await(10, SECONDS);
                        int value = cache.get(key, () -> {
                            loads.incrementAndGet();
                            int invocations = concurrentInvocations.incrementAndGet();
                            checkState(invocations == 1, "There should be no concurrent invocations, cache should do load sharing when get() invoked for same key");
                            Thread.sleep(1);
                            concurrentInvocations.decrementAndGet();
                            return -key;
                        });
                        assertEquals(value, -invocation);
                    }
                    return null;
                }));
            }

            for (Future<?> future : futures) {
                future.get(10, SECONDS);
            }
            assertThat(loads).as("loads")
                    .hasValueBetween(invocationsPerThread, threads * invocationsPerThread - 1 /* inclusive */);
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }

    /**
     * Test that the loader is invoked only once for concurrent invocations of {{@link LoadingCache#get(Object)} with equal keys.
     */
    @Test(timeOut = TEST_TIMEOUT_MILLIS)
    public void testConcurrentGetShareLoad()
            throws Exception
    {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger concurrentInvocations = new AtomicInteger();

        LoadingCache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build(new CacheLoader<Integer, Integer>()
                {
                    @Override
                    public Integer load(Integer key)
                            throws Exception
                    {
                        loads.incrementAndGet();
                        int invocations = concurrentInvocations.incrementAndGet();
                        checkState(invocations == 1, "There should be no concurrent invocations, cache should do load sharing when get() invoked for same key");
                        Thread.sleep(1);
                        concurrentInvocations.decrementAndGet();
                        return -key;
                    }
                });

        int threads = 2;
        int invocationsPerThread = 100;
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    for (int invocation = 0; invocation < invocationsPerThread; invocation++) {
                        barrier.await(10, SECONDS);
                        assertEquals((int) cache.get(invocation), -invocation);
                    }
                    return null;
                }));
            }

            for (Future<?> future : futures) {
                future.get(10, SECONDS);
            }
            assertThat(loads).as("loads")
                    .hasValueBetween(invocationsPerThread, threads * invocationsPerThread - 1 /* inclusive */);
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
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

        LoadingCache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build(new CacheLoader<Integer, String>()
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

        LoadingCache<Integer, Long> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build(CacheLoader.from(i -> remoteState.get(i).get()));

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
                            fail(format("The value read through cache (%s) in thread (%s) is not divisible by (%s)", current, threadNumber, prime));
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

    @Test(dataProvider = "disabledCacheImplementations", dataProviderClass = TestEvictableCache.class)
    public void testPutOnEmptyCacheImplementation(EvictableCacheBuilder.DisabledCacheImplementation disabledCacheImplementation)
    {
        LoadingCache<Object, Object> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(0)
                .disabledCacheImplementation(disabledCacheImplementation)
                .build(CacheLoader.from(key -> key));
        Map<Object, Object> cacheMap = cache.asMap();

        int key = 0;
        int value = 1;
        assertThat(cacheMap.put(key, value)).isNull();
        assertThat(cacheMap.put(key, value)).isNull();
        assertThat(cacheMap.putIfAbsent(key, value)).isNull();
        assertThat(cacheMap.putIfAbsent(key, value)).isNull();
    }

    @Test
    public void testPutOnNonEmptyCacheImplementation()
    {
        LoadingCache<Object, Object> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10)
                .build(CacheLoader.from(key -> key));
        Map<Object, Object> cacheMap = cache.asMap();

        int key = 0;
        int value = 1;
        assertThatThrownBy(() -> cacheMap.put(key, value))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("The operation is not supported, as in inherently races with cache invalidation. Use get(key, callable) instead.");
        assertThatThrownBy(() -> cacheMap.putIfAbsent(key, value))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("The operation is not supported, as in inherently races with cache invalidation");
    }

    /**
     * A class implementing value-based equality taking into account some fields, but not all.
     * This is definitely discouraged, but still may happen in practice.
     */
    private static class ClassWithPartialEquals
    {
        private final int value;
        private final int additionalField; // not part of equals

        public ClassWithPartialEquals(int value, int additionalField)
        {
            this.value = value;
            this.additionalField = additionalField;
        }

        public int getValue()
        {
            return value;
        }

        public int getAdditionalField()
        {
            return additionalField;
        }

        @Override
        public boolean equals(Object other)
        {
            return other != null &&
                    this.getClass() == other.getClass() &&
                    this.value == ((ClassWithPartialEquals) other).value;
        }

        @Override
        public int hashCode()
        {
            return value;
        }
    }
}
