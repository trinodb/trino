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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 700, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 700, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkEvictableCache
{
    @Benchmark
    public void cacheBenchmark(CacheBenchmarkState state)
            throws InterruptedException, ExecutionException
    {
        Cache<String, String> cache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(60))
                .maximumSize(1000)
                .build();

        try (ExecutorService executor = Executors.newFixedThreadPool(state.competingThreadsCount)) {
            for (int iteration = 0; iteration < state.iterations; iteration++) {
                CountDownLatch startLatch = new CountDownLatch(state.competingThreadsCount);
                CountDownLatch endLatch = new CountDownLatch(state.competingThreadsCount);
                for (int threadIndex = 0; threadIndex < state.competingThreadsCount; threadIndex++) {
                    executor.submit(() -> {
                        try {
                            startLatch.countDown();

                            startLatch.await();
                            cache.get("key", state.expensiveCacheLoaderSupplier.apply(state.cacheValueLoadTimeMillis));
                            endLatch.countDown();
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                endLatch.await();
                // final cache retrieval after race (to see if value is still in cache)
                cache.get("key", state.expensiveCacheLoaderSupplier.apply(state.cacheValueLoadTimeMillis));
            }
        }
    }

    @State(Scope.Benchmark)
    public static class CacheBenchmarkState
    {
        public final Function<Integer, Callable<String>> expensiveCacheLoaderSupplier = cacheValueLoadTime -> () -> {
            if (cacheValueLoadTime > 0) {
                // simulate value loading takes some time, so each cache miss is expensive
                Thread.sleep(cacheValueLoadTime);
            }
            return "key_nano:" + System.nanoTime();
        };

        @Param({"2", "5", "15", "50"})
        public int competingThreadsCount;

        @Param({"1", "2", "5"})
        public int iterations;

        @Param({"0", "500", "2000"})
        public int cacheValueLoadTimeMillis;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkEvictableCache.class).run();
    }
}
