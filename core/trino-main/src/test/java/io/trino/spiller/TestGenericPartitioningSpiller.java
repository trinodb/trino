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
package io.trino.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.trino.FeaturesConfig;
import io.trino.RowPagesBuilder;
import io.trino.SequencePageBuilder;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.PartitionFunction;
import io.trino.operator.SpillContext;
import io.trino.operator.TestingOperatorContext;
import io.trino.spi.Page;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpiller.PartitioningSpillResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.IntPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGenericPartitioningSpiller
{
    private static final int FIRST_PARTITION_START = -10;
    private static final int SECOND_PARTITION_START = 0;
    private static final int THIRD_PARTITION_START = 10;
    private static final int FOURTH_PARTITION_START = 20;

    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);

    private Path tempDirectory;
    private GenericPartitioningSpillerFactory factory;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        tempDirectory = createTempDirectory(getClass().getSimpleName());
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(ImmutableList.of(tempDirectory.toString()));
        featuresConfig.setSpillerThreads("8");
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        SingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                new TestingBlockEncodingSerde(),
                new SpillerStats(),
                featuresConfig,
                new NodeSpillConfig());
        factory = new GenericPartitioningSpillerFactory(singleStreamSpillerFactory);
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> scheduledExecutor.shutdownNow());
            closer.register(() -> deleteRecursively(tempDirectory, ALLOW_INSECURE));
        }
    }

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (PartitioningSpiller spiller = factory.create(
                TYPES,
                new FourFixedPartitionsPartitionFunction(0),
                mockSpillContext(),
                mockMemoryContext(scheduledExecutor))) {
            RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);
            builder.addSequencePage(10, FIRST_PARTITION_START, -5, 0, 5);

            List<Page> firstSpill = builder.build();

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);
            builder.addSequencePage(10, FOURTH_PARTITION_START, 25, 30, 35);

            List<Page> secondSpill = builder.build();

            IntPredicate spillPartitionMask = ImmutableSet.of(1, 2)::contains;
            PartitioningSpillResult result = spiller.partitionAndSpill(firstSpill.get(0), spillPartitionMask);
            result.getSpillingFuture().get();
            assertThat(result.getRetained().getPositionCount()).isEqualTo(0);

            result = spiller.partitionAndSpill(firstSpill.get(1), spillPartitionMask);
            result.getSpillingFuture().get();
            assertThat(result.getRetained().getPositionCount()).isEqualTo(10);

            result = spiller.partitionAndSpill(secondSpill.get(0), spillPartitionMask);
            result.getSpillingFuture().get();
            assertThat(result.getRetained().getPositionCount()).isEqualTo(0);

            result = spiller.partitionAndSpill(secondSpill.get(1), spillPartitionMask);
            result.getSpillingFuture().get();
            assertThat(result.getRetained().getPositionCount()).isEqualTo(10);

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);

            List<Page> secondPartition = builder.build();

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);

            List<Page> thirdPartition = builder.build();

            assertSpilledPages(
                    TYPES,
                    spiller,
                    ImmutableList.of(ImmutableList.of(), secondPartition, thirdPartition, ImmutableList.of()));
        }
    }

    @Test
    public void testCloseDuringReading()
            throws Exception
    {
        Iterator<Page> readingInProgress;
        try (PartitioningSpiller spiller = factory.create(
                TYPES,
                new ModuloPartitionFunction(0, 4),
                mockSpillContext(),
                mockMemoryContext(scheduledExecutor))) {
            Page page = SequencePageBuilder.createSequencePage(TYPES, 10, FIRST_PARTITION_START, 5, 10, 15);
            PartitioningSpillResult spillResult = spiller.partitionAndSpill(page, partition -> true);
            assertThat(spillResult.getRetained().getPositionCount()).isEqualTo(0);
            getFutureValue(spillResult.getSpillingFuture());

            // We get the iterator but we do not exhaust it, so that close happens during reading
            readingInProgress = spiller.getSpilledPages(0);
        }

        assertThatThrownBy(readingInProgress::hasNext)
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(ClosedChannelException.class);
    }

    @Test
    public void testWriteManyPartitions()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        int partitionCount = 4;
        AggregatedMemoryContext memoryContext = mockMemoryContext(scheduledExecutor);

        try (GenericPartitioningSpiller spiller = (GenericPartitioningSpiller) factory.create(
                types,
                new ModuloPartitionFunction(0, partitionCount),
                mockSpillContext(),
                memoryContext)) {
            for (int i = 0; i < 50_000; i++) {
                Page page = SequencePageBuilder.createSequencePage(types, partitionCount, 0);
                PartitioningSpillResult spillResult = spiller.partitionAndSpill(page, partition -> true);
                assertThat(spillResult.getRetained().getPositionCount()).isEqualTo(0);
                getFutureValue(spillResult.getSpillingFuture());
                getFutureValue(spiller.flush());
            }
        }
        assertThat(memoryContext.getBytes())
                .describedAs("Reserved bytes should be zeroed after spiller is closed")
                .isEqualTo(0);
    }

    private void assertSpilledPages(
            List<Type> types,
            PartitioningSpiller spiller,
            List<List<Page>> expectedPartitions)
    {
        for (int partition = 0; partition < expectedPartitions.size(); partition++) {
            List<Page> actualSpill = ImmutableList.copyOf(spiller.getSpilledPages(partition));
            List<Page> expectedSpill = expectedPartitions.get(partition);

            assertThat(actualSpill).hasSize(expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
    }

    private static AggregatedMemoryContext mockMemoryContext(ScheduledExecutorService scheduledExecutor)
    {
        // It's important to use OperatorContext's memory context, because it does additional bookkeeping.
        return TestingOperatorContext.create(scheduledExecutor).newAggregateUserMemoryContext();
    }

    private static SpillContext mockSpillContext()
    {
        return bytes -> {};
    }

    private static class FourFixedPartitionsPartitionFunction
            implements PartitionFunction
    {
        private final int valueChannel;

        FourFixedPartitionsPartitionFunction(int valueChannel)
        {
            this.valueChannel = valueChannel;
        }

        @Override
        public int partitionCount()
        {
            return 4;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = BIGINT.getLong(page.getBlock(valueChannel), position);
            if (value >= FOURTH_PARTITION_START) {
                return 3;
            }
            if (value >= THIRD_PARTITION_START) {
                return 2;
            }
            if (value >= SECOND_PARTITION_START) {
                return 1;
            }
            return 0;
        }
    }

    private static class ModuloPartitionFunction
            implements PartitionFunction
    {
        private final int valueChannel;
        private final int partitionCount;

        ModuloPartitionFunction(int valueChannel, int partitionCount)
        {
            this.valueChannel = valueChannel;
            checkArgument(partitionCount > 0);
            this.partitionCount = partitionCount;
        }

        @Override
        public int partitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = BIGINT.getLong(page.getBlock(valueChannel), position);
            return toIntExact(Math.abs(value) % partitionCount);
        }
    }
}
