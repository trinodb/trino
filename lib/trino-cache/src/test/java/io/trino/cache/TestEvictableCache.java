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
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TestingTicker;
import io.trino.cache.EvictableCacheBuilder.DisabledCacheImplementation;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cache.CacheStatsAssertions.assertCacheStats;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEvictableCache
{
    private static final int TEST_TIMEOUT_SECONDS = 10;

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoad()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();
        assertThat(cache.get(42, () -> "abc")).isEqualTo("abc");
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testEvictBySize()
            throws Exception
    {
        int maximumSize = 10;
        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .build();

        for (int i = 0; i < 10_000; i++) {
            int value = i * 10;
            assertThat((Object) cache.get(i, () -> value)).isEqualTo(value);
        }
        cache.cleanUp();
        assertThat(cache.size()).isEqualTo(maximumSize);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).isEqualTo(maximumSize);

        // Ensure cache is effective, i.e. some entries preserved
        int lastKey = 10_000 - 1;
        assertThat((Object) cache.get(lastKey, () -> {
            throw new UnsupportedOperationException();
        })).isEqualTo(lastKey * 10);
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testEvictByWeight()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(20)
                .weigher((Integer key, String value) -> value.length())
                .build();

        for (int i = 0; i < 10; i++) {
            String value = "a".repeat(i);
            assertThat((Object) cache.get(i, () -> value)).isEqualTo(value);
        }
        cache.cleanUp();
        // It's not deterministic which entries get evicted
        int cacheSize = toIntExact(cache.size());
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(cacheSize);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(cacheSize);
        assertThat(cache.asMap().keySet().stream().mapToInt(i -> i).sum()).as("key sum").isLessThanOrEqualTo(20);
        assertThat(cache.asMap().values()).as("values").hasSize(cacheSize);
        assertThat(cache.asMap().values().stream().mapToInt(String::length).sum()).as("values length sum").isLessThanOrEqualTo(20);

        // Ensure cache is effective, i.e. some entries preserved
        int lastKey = 10 - 1;
        assertThat(cache.get(lastKey, () -> {
            throw new UnsupportedOperationException();
        })).isEqualTo("a".repeat(lastKey));
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testEvictByTime()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        int ttl = 100;
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                .build();

        assertThat(cache.get(1, () -> "1 ala ma kota")).isEqualTo("1 ala ma kota");
        ticker.increment(ttl, MILLISECONDS);
        assertThat(cache.get(2, () -> "2 ala ma kota")).isEqualTo("2 ala ma kota");
        cache.cleanUp();

        // First entry should be expired and its token removed
        int cacheSize = toIntExact(cache.size());
        assertThat(cacheSize).as("cacheSize").isEqualTo(1);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(cacheSize);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(cacheSize);
        assertThat(cache.asMap().values()).as("values").hasSize(cacheSize);
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testPreserveValueLoadedAfterTimeExpiration()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        int ttl = 100;
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                .build();
        int key = 11;

        assertThat(cache.get(key, () -> "11 ala ma kota")).isEqualTo("11 ala ma kota");
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        // Should be served from the cache
        assertThat(cache.get(key, () -> "something else")).isEqualTo("11 ala ma kota");
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        ticker.increment(ttl, MILLISECONDS);
        // Should be reloaded
        assertThat(cache.get(key, () -> "new value")).isEqualTo("new value");
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        // Should be served from the cache
        assertThat(cache.get(key, () -> "something yet different")).isEqualTo("new value");
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);

        assertThat(cache.size()).as("cacheSize").isEqualTo(1);
        assertThat(((EvictableCache<?, ?>) cache).tokensCount()).as("tokensCount").isEqualTo(1);
        assertThat(cache.asMap().keySet()).as("keySet").hasSize(1);
        assertThat(cache.asMap().values()).as("values").hasSize(1);
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
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
        assertThat(cache.asMap().replace(key, initialValue, replacedValue)).isTrue();
        assertThat((Object) cache.getIfPresent(key)).isEqualTo(replacedValue);

        // already replaced, current value is different
        assertThat(cache.asMap().replace(key, initialValue, replacedValue)).isFalse();
        assertThat((Object) cache.getIfPresent(key)).isEqualTo(replacedValue);

        // non-existent key
        assertThat(cache.asMap().replace(100000, replacedValue, 22)).isFalse();
        assertThat(cache.asMap().keySet()).isEqualTo(ImmutableSet.of(key));
        assertThat((Object) cache.getIfPresent(key)).isEqualTo(replacedValue);

        int anotherKey = 13;
        int anotherInitialValue = 14;
        cache.get(anotherKey, () -> anotherInitialValue);
        cache.invalidate(anotherKey);
        // after eviction
        assertThat(cache.asMap().replace(anotherKey, anotherInitialValue, 15)).isFalse();
        assertThat(cache.asMap().keySet()).isEqualTo(ImmutableSet.of(key));
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testDisabledCache()
            throws Exception
    {
        assertThatThrownBy(() -> EvictableCacheBuilder.newBuilder()
                .maximumSize(0)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Even when cache is disabled, the loads are synchronized and both load results and failures are shared between threads. " +
                        "This is rarely desired, thus builder caller is expected to either opt-in into this behavior with shareResultsAndFailuresEvenIfDisabled(), " +
                        "or choose not to share results (and failures) between concurrent invocations with shareNothingWhenDisabled().");

        testDisabledCache(
                EvictableCacheBuilder.newBuilder()
                        .maximumSize(0)
                        .shareNothingWhenDisabled()
                        .build());

        testDisabledCache(
                EvictableCacheBuilder.newBuilder()
                        .maximumSize(0)
                        .shareResultsAndFailuresEvenIfDisabled()
                        .build());
    }

    private void testDisabledCache(Cache<Integer, Integer> cache)
            throws Exception
    {
        for (int i = 0; i < 10; i++) {
            int value = i * 10;
            assertThat((Object) cache.get(i, () -> value)).isEqualTo(value);
        }
        cache.cleanUp();
        assertThat(cache.size()).isEqualTo(0);
        assertThat(cache.asMap().keySet()).as("keySet").isEmpty();
        assertThat(cache.asMap().values()).as("values").isEmpty();
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoadStats()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build();

        assertThat(cache.stats()).isEqualTo(new CacheStats(0, 0, 0, 0, 0, 0));

        String value = assertCacheStats(cache)
                .misses(1)
                .loads(1)
                .calling(() -> cache.get(42, () -> "abc"));
        assertThat(value).isEqualTo("abc");

        value = assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(42, () -> "xyz"));
        assertThat(value).isEqualTo("abc");

        // with equal, but not the same key
        value = assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(newInteger(42), () -> "xyz"));
        assertThat(value).isEqualTo("abc");
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoadFailure()
            throws Exception
    {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(0)
                .expireAfterWrite(0, DAYS)
                .shareResultsAndFailuresEvenIfDisabled()
                .build();
        int key = 10;

        ExecutorService executor = newFixedThreadPool(2);
        try {
            Exchanger<Thread> exchanger = new Exchanger<>();
            CountDownLatch secondUnblocked = new CountDownLatch(1);

            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                boolean first = i == 0;
                futures.add(executor.submit(() -> {
                    if (!first) {
                        // Wait for the first one to start the call
                        exchanger.exchange(Thread.currentThread(), 10, SECONDS);
                        // Prove that we are back in RUNNABLE state.
                        secondUnblocked.countDown();
                    }
                    return cache.get(key, () -> {
                        if (first) {
                            Thread secondThread = exchanger.exchange(null, 10, SECONDS);
                            assertThat(secondUnblocked.await(10, SECONDS)).isTrue();
                            // Wait for the second one to hang inside the cache.get call.
                            assertEventually(() -> assertThat(secondThread.getState()).isNotEqualTo(Thread.State.RUNNABLE));
                            throw new RuntimeException("first attempt is poised to fail");
                        }
                        return "success";
                    });
                }));
            }

            List<String> results = new ArrayList<>();
            for (Future<String> future : futures) {
                try {
                    results.add(future.get());
                }
                catch (ExecutionException e) {
                    results.add(e.getCause().toString());
                }
            }

            // Note: if this starts to fail, that suggests that Guava implementation changed and NoopCache may be redundant now.
            assertThat(results).containsExactly(
                    "com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: first attempt is poised to fail",
                    "com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: first attempt is poised to fail");
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @SuppressModernizer
    private static Integer newInteger(int value)
    {
        Integer integer = value;
        @SuppressWarnings({"UnnecessaryBoxing", "BoxedPrimitiveConstructor", "CachedNumberConstructorCall", "removal"})
        Integer newInteger = new Integer(value);
        assertThat(integer).isNotSameAs(newInteger);
        return newInteger;
    }

    /**
     * Test that the loader is invoked only once for concurrent invocations of {{@link LoadingCache#get(Object, Callable)} with equal keys.
     * This is a behavior of Guava Cache as well. While this is not necessarily desirable behavior (see
     * <a href="https://github.com/trinodb/trino/issues/11067">https://github.com/trinodb/trino/issues/11067</a>),
     * the test exists primarily to document current state and support discussion, should the current state change.
     */
    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testConcurrentGetWithCallableShareLoad()
            throws Exception
    {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger concurrentInvocations = new AtomicInteger();

        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();

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
                        assertThat(value).isEqualTo(-invocation);
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
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testInvalidateOngoingLoad()
            throws Exception
    {
        for (Invalidation invalidation : Invalidation.values()) {
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
                        assertThat(invalidated.await(10, SECONDS)).isTrue(); // 2
                        return "stale value";
                    });
                    getReturned.countDown(); // 3
                    return value;
                });

                // thread B
                Future<String> threadB = executor.submit(() -> {
                    assertThat(loadOngoing.await(10, SECONDS)).isTrue(); // 1

                    switch (invalidation) {
                        case INVALIDATE_KEY -> cache.invalidate(key);
                        case INVALIDATE_PREDEFINED_KEYS -> cache.invalidateAll(ImmutableList.of(key));
                        case INVALIDATE_SELECTED_KEYS -> {
                            Set<Integer> keys = cache.asMap().keySet().stream()
                                    .filter(foundKey -> (int) foundKey == key)
                                    .collect(toImmutableSet());
                            cache.invalidateAll(keys);
                        }
                        case INVALIDATE_ALL -> cache.invalidateAll();
                    }

                    invalidated.countDown(); // 2
                    // Cache may persist value after loader returned, but before `cache.get(...)` returned. Ensure the latter completed.
                    assertThat(getReturned.await(10, SECONDS)).isTrue(); // 3

                    return cache.get(key, () -> "fresh value");
                });

                assertThat(threadA.get()).isEqualTo("stale value");
                assertThat(threadB.get()).isEqualTo("fresh value");
            }
            catch (AssertionError e) {
                throw new AssertionError("Error for invalidation=%s: %s".formatted(invalidation, e.getMessage()), e);
            }
            finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
            }
        }
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testInvalidateAndLoadConcurrently()
            throws Exception
    {
        for (Invalidation invalidation : Invalidation.values()) {
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
                            assertThat((long) cache.get(key, remoteState::get)).isEqualTo(1L);
                            int prime = primes[threadNumber];

                            barrier.await(10, SECONDS);

                            // modify underlying state
                            remoteState.updateAndGet(current -> current * prime);

                            // invalidate
                            switch (invalidation) {
                                case INVALIDATE_KEY -> cache.invalidate(key);
                                case INVALIDATE_PREDEFINED_KEYS -> cache.invalidateAll(ImmutableList.of(key));
                                case INVALIDATE_SELECTED_KEYS -> {
                                    Set<Integer> keys = cache.asMap().keySet().stream()
                                            .filter(foundKey -> (int) foundKey == key)
                                            .collect(toImmutableSet());
                                    cache.invalidateAll(keys);
                                }
                                case INVALIDATE_ALL -> cache.invalidateAll();
                            }

                            // read through cache
                            long current = cache.get(key, remoteState::get);
                            if (current % prime != 0) {
                                throw new AssertionError(format("The value read through cache (%s) in thread (%s) is not divisible by (%s)", current, threadNumber, prime));
                            }

                            return (Void) null;
                        }))
                        .collect(toImmutableList());

                futures.forEach(MoreFutures::getFutureValue);

                assertThat(remoteState.get()).isEqualTo(2 * 3 * 5 * 7);
                assertThat((long) cache.get(key, remoteState::get)).isEqualTo(remoteState.get());
            }
            catch (AssertionError e) {
                throw new AssertionError("Error for invalidation=%s: %s".formatted(invalidation, e.getMessage()), e);
            }
            finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
            }
        }
    }

    @Test
    public void testPutOnEmptyCacheImplementation()
    {
        for (DisabledCacheImplementation disabledCacheImplementation : DisabledCacheImplementation.values()) {
            Cache<Object, Object> cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(0)
                    .disabledCacheImplementation(disabledCacheImplementation)
                    .build();
            Map<Object, Object> cacheMap = cache.asMap();

            int key = 0;
            int value = 1;
            assertThat(cacheMap.put(key, value)).isNull();
            assertThat(cacheMap.put(key, value)).isNull();
            assertThat(cacheMap.putIfAbsent(key, value)).isNull();
            assertThat(cacheMap.putIfAbsent(key, value)).isNull();
        }
    }

    @Test
    public void testPutOnNonEmptyCacheImplementation()
    {
        Cache<Object, Object> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10)
                .build();
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

    @RepeatedTest(1_000)
    public void testParallelLoadingCacheEntries()
    {
        Cache<String, String> cache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(60))
                .maximumSize(10)
                .build();
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Runnable cacheLoader = () -> {
                try {
                    String value = cache.get("key", () -> "value");
                    assertThat(value).isEqualTo("value");
                }
                catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            };
            executor.submit(cacheLoader);
            executor.submit(cacheLoader);
        }
        assertThat(cache.getIfPresent("key")).isNotNull();
    }
}
