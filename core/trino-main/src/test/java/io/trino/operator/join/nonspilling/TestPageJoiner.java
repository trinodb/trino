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
package io.trino.operator.join.nonspilling;

import com.google.common.collect.ImmutableList;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.PagesIndex;
import io.trino.operator.WorkProcessor;
import io.trino.operator.join.JoinStatisticsCounter;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.nonspilling.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.join.JoinType.INNER;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPageJoiner
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> OUTPUT_TYPES = ImmutableList.of(BIGINT, BIGINT);
    private static final NullSafeHashCompiler HASH_COMPILER = new NullSafeHashCompiler(new TypeOperators());

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-page-joiner-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-page-joiner-scheduled-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSingleMatchCapability(boolean singleBigintLookupSource)
            throws IOException
    {
        try (LookupSource unique = lookupSource(singleBigintLookupSource, new Page(createLongsBlock(1L, 2L, null, null)), Optional.empty()).get();
                LookupSource duplicates = lookupSource(singleBigintLookupSource, new Page(createLongsBlock(1L, 2L, 1L)), Optional.empty()).get();
                LookupSource filtered = lookupSource(singleBigintLookupSource, new Page(createLongsBlock(1L, 2L)), Optional.of((_, _, _) -> (_, _, _) -> false)).get()) {
            assertThat(unique.isSingleMatchWithoutFilter()).isTrue();
            assertThat(duplicates.isSingleMatchWithoutFilter()).isFalse();
            assertThat(filtered.isSingleMatchWithoutFilter()).isFalse();
        }
    }

    @Test
    public void testPartitionedSingleMatchCapability()
            throws IOException
    {
        Supplier<LookupSource> unique = lookupSource(true, new Page(createLongsBlock(1L, 2L)), Optional.empty());
        Supplier<LookupSource> duplicates = lookupSource(true, new Page(createLongsBlock(3L, 3L)), Optional.empty());
        Supplier<LookupSource> filtered = lookupSource(true, new Page(createLongsBlock(3L)), Optional.of((_, _, _) -> (_, _, _) -> true));
        for (Supplier<LookupSource> other : ImmutableList.of(unique, duplicates, filtered)) {
            try (LookupSource source = PartitionedLookupSource.createPartitionedLookupSourceSupplier(ImmutableList.of(unique, other), TYPES, false, HASH_COMPILER).getLookupSource()) {
                assertThat(source.isSingleMatchWithoutFilter()).isEqualTo(other == unique);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSingleMatchYield(boolean singleBigintLookupSource)
            throws IOException
    {
        DriverContext driverContext = driverContext();
        DriverYieldSignal yieldSignal = driverContext.getYieldSignal();
        JoinStatisticsCounter statistics = new JoinStatisticsCounter(INNER);
        TestingLookupSource source = new TestingLookupSource(
                lookupSource(singleBigintLookupSource, new Page(createLongsBlock(10, 20, 30)), Optional.empty()).get(),
                yieldSignal::forceYieldForTesting);
        try (PageJoiner joiner = pageJoiner(driverContext, source, statistics)) {
            WorkProcessor<Page> output = WorkProcessor.fromIterable(ImmutableList.of(new Page(createLongsBlock(null, 10L, 99L, 20L, 30L))))
                    .transform(joiner);
            yieldSignal.forceYieldForTesting();
            assertThat(output.process()).isFalse();
            assertThat(source.appended).isZero();

            for (int matches = 1; matches <= 3; matches++) {
                yieldSignal.resetYieldForTesting();
                assertThat(output.process()).isFalse();
                assertThat(source.appended).isEqualTo(matches);
            }
            yieldSignal.resetYieldForTesting();
            assertThat(output.process()).isTrue();
            assertThat(output.isFinished()).isFalse();
            assertThat(toMaterializedResult(OUTPUT_TYPES, ImmutableList.of(output.getResult())))
                    .containsExactlyElementsOf(MaterializedResult.resultBuilder(OUTPUT_TYPES).row(10L, 10L).row(20L, 20L).row(30L, 30L).build());
            assertThat(output.process()).isTrue();
            assertThat(output.isFinished()).isTrue();
            assertThat(statistics.get().getLogHistogramProbes()).containsExactly(2, 3, 0, 0, 0, 0, 0, 0);
            assertThat(statistics.get().getLogHistogramOutput()).containsExactly(0, 3, 0, 0, 0, 0, 0, 0);
        }
        assertThat(source.closed).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSingleMatchOutputPageBoundaries(boolean singleBigintLookupSource)
            throws IOException
    {
        int positions = 20_000;
        Page build = createSequencePage(TYPES, positions, 0);
        TestingLookupSource source = new TestingLookupSource(lookupSource(singleBigintLookupSource, build, Optional.empty()).get(), () -> {});
        JoinStatisticsCounter statistics = new JoinStatisticsCounter(INNER);
        List<Page> input = ImmutableList.of(build, new Page(createLongsBlock(new int[0])), new Page(createLongsBlock(-1, -2)), new Page(createLongsBlock(2, 1)));
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(OUTPUT_TYPES);
        for (long position = 0; position < positions; position++) {
            expected.row(position, position);
        }
        expected.row(2L, 2L).row(1L, 1L);

        try (PageJoiner joiner = pageJoiner(driverContext(), source, statistics)) {
            List<Page> output = drain(WorkProcessor.fromIterable(input).transform(joiner));
            assertThat(output.size()).isGreaterThan(2);
            assertThat(toMaterializedResult(OUTPUT_TYPES, output)).containsExactlyElementsOf(expected.build());
            assertThat(source.appended).isEqualTo(positions + 2);
            assertThat(statistics.get().getLogHistogramProbes()).containsExactly(2, positions + 2, 0, 0, 0, 0, 0, 0);
            assertThat(statistics.get().getLogHistogramOutput()).containsExactly(0, positions + 2, 0, 0, 0, 0, 0, 0);
            assertThat(statistics.get().getTotalProbes()).isEqualTo(input.size());
        }
    }

    @Test
    public void testSingleMatchAndRlePages()
            throws IOException
    {
        LookupSource source = lookupSource(true, new Page(createLongsBlock(1, 2, 3)), Optional.empty()).get();
        JoinStatisticsCounter statistics = new JoinStatisticsCounter(INNER);
        List<Page> input = ImmutableList.of(
                new Page(createLongsBlock(1L, null, 2L)),
                new Page(RunLengthEncodedBlock.create(BIGINT, 3L, 2)),
                new Page(createLongsBlock(2, 1)),
                new Page(RunLengthEncodedBlock.create(BIGINT, null, 2)),
                new Page(createLongsBlock(3, -1)));
        try (PageJoiner joiner = pageJoiner(driverContext(), source, statistics)) {
            assertThat(toMaterializedResult(OUTPUT_TYPES, drain(WorkProcessor.fromIterable(input).transform(joiner))))
                    .containsExactlyElementsOf(MaterializedResult.resultBuilder(OUTPUT_TYPES)
                            .row(1L, 1L).row(2L, 2L).row(3L, 3L).row(3L, 3L).row(2L, 2L).row(1L, 1L).row(3L, 3L).build());
            assertThat(statistics.get().getRleProbes()).isEqualTo(2);
        }
    }

    @Test
    public void testCloseWhileYielded()
            throws IOException
    {
        DriverContext driverContext = driverContext();
        TestingLookupSource source = new TestingLookupSource(
                lookupSource(true, new Page(createLongsBlock(1, 2)), Optional.empty()).get(),
                driverContext.getYieldSignal()::forceYieldForTesting);
        try (PageJoiner joiner = pageJoiner(driverContext, source, new JoinStatisticsCounter(INNER))) {
            WorkProcessor<Page> output = WorkProcessor.fromIterable(ImmutableList.of(new Page(createLongsBlock(1, 2)))).transform(joiner);
            assertThat(output.process()).isFalse();
            assertThat(source.appended).isEqualTo(1);
        }
        assertThat(source.closed).isTrue();
    }

    private DriverContext driverContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static PageJoiner pageJoiner(DriverContext driverContext, LookupSource source, JoinStatisticsCounter statistics)
    {
        return new PageJoiner(
                driverContext.addOperatorContext(0, new PlanNodeId("test"), "test"),
                TYPES,
                INNER,
                false,
                new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false),
                immediateFuture(source),
                statistics);
    }

    private static Supplier<LookupSource> lookupSource(boolean singleBigintLookupSource, Page build, Optional<JoinFilterFunctionFactory> filter)
    {
        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false, singleBigintLookupSource).newPagesIndex(TYPES, build.getPositionCount());
        pagesIndex.addPage(build);
        return pagesIndex.createLookupSourceSupplier(TEST_SESSION, ImmutableList.of(0), filter, OptionalInt.empty(), ImmutableList.of());
    }

    private static List<Page> drain(WorkProcessor<Page> output)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (int calls = 0; calls < 100; calls++) {
            assertThat(output.process()).isTrue();
            if (output.isFinished()) {
                return pages.build();
            }
            pages.add(output.getResult());
        }
        throw new AssertionError("Join did not finish");
    }

    private static class TestingLookupSource
            implements LookupSource
    {
        private final LookupSource delegate;
        private final Runnable onAppend;
        private int appended;
        private boolean closed;

        public TestingLookupSource(LookupSource delegate, Runnable onAppend)
        {
            this.delegate = delegate;
            this.onAppend = onAppend;
            assertThat(delegate.isSingleMatchWithoutFilter()).isTrue();
        }

        @Override
        public boolean isSingleMatchWithoutFilter()
        {
            return true;
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return delegate.getInMemorySizeInBytes();
        }

        @Override
        public long getJoinPositionCount()
        {
            return delegate.getJoinPositionCount();
        }

        @Override
        public long joinPositionWithinPartition(long position)
        {
            return delegate.joinPositionWithinPartition(position);
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
        {
            return delegate.getJoinPosition(position, hashChannelsPage, allChannelsPage, rawHash);
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            return delegate.getJoinPosition(position, hashChannelsPage, allChannelsPage);
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new AssertionError("Single-match lookup must not traverse position links");
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            delegate.appendTo(position, pageBuilder, outputChannelOffset);
            appended++;
            onAppend.run();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new AssertionError("Unfiltered lookup must not evaluate a join filter");
        }

        @Override
        public boolean isEmpty()
        {
            return delegate.isEmpty();
        }

        @Override
        public void close()
        {
            delegate.close();
            closed = true;
        }
    }
}
