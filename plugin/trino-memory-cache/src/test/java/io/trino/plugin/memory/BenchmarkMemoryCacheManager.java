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
package io.trino.plugin.memory;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 6, time = 2)
@Measurement(iterations = 6, time = 2)
@BenchmarkMode(Throughput)
public class BenchmarkMemoryCacheManager
{
    @State(Scope.Benchmark)
    public static class Context
    {
        @Param({"false", "true"})
        private boolean polluteCache;
        @Param({"false", "true"})
        private boolean changeSignatures;

        private final MemoryCacheManager memoryCacheManager = new MemoryCacheManager(bytes -> bytes <= 4_000_000_000L, true);
        private final ConcurrentCacheManager concurrentCacheManager = new ConcurrentCacheManager(
                new CacheManagerContext()
                {
                    @Override
                    public MemoryAllocator revocableMemoryAllocator()
                    {
                        return bytes -> bytes <= 4_000_000_000L;
                    }

                    @Override
                    public BlockEncodingSerde blockEncodingSerde()
                    {
                        return new TestingBlockEncodingSerde();
                    }
                },
                true);
        private final CacheSplitId splitId = new CacheSplitId("split");
        private final List<CacheColumnId> columnIds = IntStream.range(0, 64)
                .mapToObj(i -> new CacheColumnId("column" + i))
                .collect(toImmutableList());

        private final List<Type> columnTypes = columnIds.stream()
                .map(column -> INTEGER)
                .collect(toImmutableList());
        private final PlanSignature[] signatures = IntStream.range(0, 200)
                .mapToObj(i -> new PlanSignature(
                        new SignatureKey("key" + i),
                        Optional.empty(),
                        columnIds,
                        columnTypes,
                        TupleDomain.all(),
                        TupleDomain.all()))
                .toArray(PlanSignature[]::new);
        private final AtomicLong nextSignature = new AtomicLong();

        private final Page page = new Page(nCopies(
                columnIds.size(),
                new IntArrayBlock(4, Optional.empty(), new int[] {0, 1, 2, 3}))
                .toArray(new Block[0]));

        @Setup
        public void setup()
                throws IOException
        {
            if (polluteCache) {
                for (int i = 0; i < 100; i++) {
                    storeCachedData(memoryCacheManager);
                    storeCachedData(concurrentCacheManager);
                }
            }
            storeCachedData(memoryCacheManager);
            storeCachedData(concurrentCacheManager);
        }

        public MemoryCacheManager memoryCacheManager()
        {
            return memoryCacheManager;
        }

        public ConcurrentCacheManager concurrentCacheManager()
        {
            return concurrentCacheManager;
        }

        public Optional<ConnectorPageSource> loadCachedData(CacheManager cacheManager)
                throws IOException
        {
            try (CacheManager.SplitCache splitCache = cacheManager.getSplitCache(getSignature())) {
                return splitCache.loadPages(splitId);
            }
        }

        public void storeCachedData(CacheManager cacheManager)
                throws IOException
        {
            try (CacheManager.SplitCache splitCache = cacheManager.getSplitCache(getSignature())) {
                ConnectorPageSink sink = splitCache.storePages(splitId).orElseThrow();
                sink.appendPage(page);
                sink.finish();
            }
        }

        private PlanSignature getSignature()
        {
            if (changeSignatures) {
                return signatures[(int) (nextSignature.getAndIncrement() % signatures.length)];
            }
            return signatures[0];
        }
    }

    @Threads(10)
    @Benchmark
    public Optional<ConnectorPageSource> benchmarkMemoryLoadPages(Context context)
            throws IOException
    {
        return context.loadCachedData(context.memoryCacheManager());
    }

    @Threads(10)
    @Benchmark
    public void benchmarkMemoryStorePages(Context context)
            throws IOException
    {
        context.storeCachedData(context.memoryCacheManager());
    }

    @Threads(10)
    @Benchmark
    public Optional<ConnectorPageSource> benchmarkConcurrentLoadPages(Context context)
            throws IOException
    {
        return context.loadCachedData(context.concurrentCacheManager());
    }

    @Threads(10)
    @Benchmark
    public void benchmarkConcurrentStorePages(Context context)
            throws IOException
    {
        context.storeCachedData(context.concurrentCacheManager());
    }

    @Test
    public void testBenchmarkMemoryLoadPages()
            throws IOException
    {
        Context context = new Context();
        context.setup();
        assertThat(benchmarkMemoryLoadPages(context)).isPresent();
    }

    @Test
    public void testBenchmarkMemoryStorePages()
            throws IOException
    {
        Context context = new Context();
        context.setup();
        benchmarkMemoryStorePages(context);
    }

    @Test
    public void testBenchmarkConcurrentLoadPages()
            throws IOException
    {
        Context context = new Context();
        context.setup();
        assertThat(benchmarkConcurrentLoadPages(context)).isPresent();
    }

    @Test
    public void testBenchmarkConcurrentStorePages()
            throws IOException
    {
        Context context = new Context();
        context.setup();
        benchmarkConcurrentStorePages(context);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkMemoryCacheManager.class).run();
    }
}
