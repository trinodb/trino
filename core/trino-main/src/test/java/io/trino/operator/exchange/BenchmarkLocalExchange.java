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
package io.trino.operator.exchange;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.Session;
import io.trino.jmh.Benchmarks;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.TaskContext;
import io.trino.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.PartitionFunctionProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLocalExchange
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final int POSITIONS_PER_WRITER = 1_000_000;
    private static final Session SESSION = testSessionBuilder().build();
    private static final DataSize MAX_BUFFERED_BYTES = DataSize.of(128, MEGABYTE);
    private static final DataSize WRITER_SCALING_MIN_DATA_PROCESSED = DataSize.of(32, MEGABYTE);
    private static final NullSafeHashCompiler HASH_COMPILER = new NullSafeHashCompiler(new TypeOperators());

    @Benchmark
    public long exchange(BenchmarkData data)
            throws Exception
    {
        LocalExchange localExchange = data.createLocalExchange();
        LocalExchangeSinkFactory sinkFactory = localExchange.createSinkFactory();
        sinkFactory.noMoreSinkFactories();
        List<LocalExchangeSink> sinks = range(0, data.concurrency)
                .mapToObj(_ -> sinkFactory.createSink())
                .collect(toImmutableList());
        sinkFactory.close();
        List<LocalExchangeSource> sources = range(0, localExchange.getBufferCount())
                .mapToObj(_ -> localExchange.getNextSource())
                .collect(toImmutableList());

        ImmutableList.Builder<Future<Long>> futures = ImmutableList.builder();
        for (LocalExchangeSink sink : sinks) {
            futures.add(data.executor.submit(() -> write(sink, data.pages)));
        }
        for (LocalExchangeSource source : sources) {
            futures.add(data.executor.submit(() -> read(source)));
        }
        long positions = 0;
        for (Future<Long> future : futures.build()) {
            positions += future.get();
        }
        return positions;
    }

    private static long write(LocalExchangeSink sink, List<Page> pages)
    {
        for (Page page : pages) {
            getFutureValue(sink.waitForWriting());
            sink.addPage(page);
        }
        sink.finish();
        return 0;
    }

    private static long read(LocalExchangeSource source)
    {
        long positions = 0;
        while (!source.isFinished()) {
            Page page = source.removePage();
            if (page == null) {
                getFutureValue(source.waitForReading());
            }
            else {
                positions += page.getPositionCount();
            }
        }
        return positions;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"FIXED_HASH_DISTRIBUTION", "FIXED_ARBITRARY_DISTRIBUTION", "FIXED_PASSTHROUGH_DISTRIBUTION"})
        private String partitioning = "FIXED_HASH_DISTRIBUTION";

        @Param({"128", "8192"})
        private int positionsPerPage = 128;

        @Param("4")
        private int concurrency = 4;

        private final PartitionFunctionProvider partitionFunctionProvider = new PartitionFunctionProvider(HASH_COMPILER, _ -> {
            throw new UnsupportedOperationException();
        });
        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private TaskContext taskContext;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(BenchmarkLocalExchange.class.getSimpleName() + "-%s"));
            scheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed(BenchmarkLocalExchange.class.getSimpleName() + "-scheduler-%s"));
            taskContext = TestingTaskContext.builder(executor, scheduledExecutor, SESSION)
                    .setMemoryPoolSize(DataSize.of(1, GIGABYTE))
                    .build();
            pages = range(0, POSITIONS_PER_WRITER / positionsPerPage)
                    .mapToObj(page -> SequencePageBuilder.createSequencePage(TYPES, positionsPerPage, page * positionsPerPage))
                    .collect(toImmutableList());
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private LocalExchange createLocalExchange()
        {
            PartitioningHandle partitioningHandle = partitioningHandle();
            List<Integer> partitionChannels = ImmutableList.of();
            List<Type> partitionChannelTypes = ImmutableList.of();
            if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                partitionChannels = ImmutableList.of(0);
                partitionChannelTypes = TYPES;
            }
            return new LocalExchange(
                    partitionFunctionProvider,
                    SESSION,
                    concurrency,
                    partitioningHandle,
                    OptionalInt.empty(),
                    partitionChannels,
                    partitionChannelTypes,
                    MAX_BUFFERED_BYTES,
                    taskContext.aggregateUserMemoryContext(),
                    HASH_COMPILER,
                    WRITER_SCALING_MIN_DATA_PROCESSED,
                    () -> 0L);
        }

        private PartitioningHandle partitioningHandle()
        {
            return switch (partitioning) {
                case "FIXED_HASH_DISTRIBUTION" -> FIXED_HASH_DISTRIBUTION;
                case "FIXED_ARBITRARY_DISTRIBUTION" -> FIXED_ARBITRARY_DISTRIBUTION;
                case "FIXED_PASSTHROUGH_DISTRIBUTION" -> FIXED_PASSTHROUGH_DISTRIBUTION;
                default -> throw new IllegalArgumentException("Unsupported partitioning: " + partitioning);
            };
        }
    }

    @Test
    public void verifyExchange()
            throws Exception
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        try {
            assertThat(exchange(data)).isEqualTo((long) data.concurrency * data.pages.stream().mapToInt(Page::getPositionCount).sum());
        }
        finally {
            data.tearDown();
        }
    }

    static void main()
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkLocalExchange.class).run();
    }
}
