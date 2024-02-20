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
package io.trino.operator.output;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OutputFactory;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestPagePartitioner
{
    private static final DataSize MAX_MEMORY = DataSize.of(50, MEGABYTE);
    private static final DataSize PARTITION_MAX_MEMORY = DataSize.of(5, MEGABYTE);

    private static final int POSITIONS_PER_PAGE = 8;
    private static final int PARTITION_COUNT = 2;

    private static final PagesSerdeFactory PAGES_SERDE_FACTORY = new PagesSerdeFactory(new TestingBlockEncodingSerde(), NONE);
    private static final PageDeserializer PAGE_DESERIALIZER = PAGES_SERDE_FACTORY.createDeserializer(Optional.empty());

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-executor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    public void tearDownClass()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testOutputForEmptyPage()
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        Page page = new Page(createLongsBlock(ImmutableList.of()));

        pagePartitioner.partitionPage(page, operatorContext());
        pagePartitioner.close();

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).isEmpty();
    }

    private OperatorContext operatorContext()
    {
        return new DriverContextBuilder(executor, scheduledExecutor).buildDriverContext()
                .addOperatorContext(0, new PlanNodeId("plan-node-0"), PartitionedOutputOperator.class.getSimpleName());
    }

    @Test
    public void testOutputEqualsInput()
    {
        testOutputEqualsInput(PartitioningMode.ROW_WISE);
        testOutputEqualsInput(PartitioningMode.COLUMNAR);
    }

    private void testOutputEqualsInput(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();

        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        Page page = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        List<Object> expected = readLongs(Stream.of(page), 0);

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test
    public void testOutputForPageWithNoBlockPartitionFunction()
    {
        testOutputForPageWithNoBlockPartitionFunction(PartitioningMode.ROW_WISE);
        testOutputForPageWithNoBlockPartitionFunction(PartitioningMode.COLUMNAR);
    }

    private void testOutputForPageWithNoBlockPartitionFunction(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();

        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT)
                .withPartitionFunction(new BucketPartitionFunction(
                        ROUND_ROBIN.createBucketFunction(null, false, PARTITION_COUNT, null),
                        IntStream.range(0, PARTITION_COUNT).toArray()))
                .withPartitionChannels(ImmutableList.of())
                .build();
        Page page = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(0L, 2L, 4L, 6L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(1L, 3L, 5L, 7L);
    }

    @Test
    public void testOutputForMultipleSimplePages()
    {
        testOutputForMultipleSimplePages(PartitioningMode.ROW_WISE);
        testOutputForMultipleSimplePages(PartitioningMode.COLUMNAR);
    }

    private void testOutputForMultipleSimplePages(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();

        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        Page page1 = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        Page page2 = new Page(createLongSequenceBlock(1, POSITIONS_PER_PAGE));
        Page page3 = new Page(createLongSequenceBlock(2, POSITIONS_PER_PAGE));
        List<Object> expected = readLongs(Stream.of(page1, page2, page3), 0);

        processPages(pagePartitioner, partitioningMode, page1, page2, page3);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test
    public void testOutputForSimplePageWithReplication()
    {
        testOutputForSimplePageWithReplication(PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithReplication(PartitioningMode.COLUMNAR);
    }

    private void testOutputForSimplePageWithReplication(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).replicate().build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(0L, 2L, null);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(0L, 1L, 3L); // position 0 copied to all partitions
    }

    @Test
    public void testOutputForSimplePageWithNullChannel()
    {
        testOutputForSimplePageWithNullChannel(PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithNullChannel(PartitioningMode.COLUMNAR);
    }

    private void testOutputForSimplePageWithNullChannel(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).withNullChannel(0).build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyInAnyOrder(0L, 2L, null);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyInAnyOrder(1L, 3L, null); // null copied to all partitions
    }

    @Test
    public void testOutputForSimplePageWithPartitionConstant()
    {
        testOutputForSimplePageWithPartitionConstant(PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithPartitionConstant(PartitioningMode.COLUMNAR);
    }

    private void testOutputForSimplePageWithPartitionConstant(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT)
                .withPartitionConstants(ImmutableList.of(Optional.of(new NullableValue(BIGINT, 1L))))
                .withPartitionChannels(-1)
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));
        List<Object> allValues = readLongs(Stream.of(page), 0);

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).isEmpty();
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyElementsOf(allValues);
    }

    @Test
    public void testOutputForSimplePageWithPartitionConstantAndHashBlock()
    {
        testOutputForSimplePageWithPartitionConstantAndHashBlock(PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithPartitionConstantAndHashBlock(PartitioningMode.COLUMNAR);
    }

    private void testOutputForSimplePageWithPartitionConstantAndHashBlock(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT)
                .withPartitionConstants(ImmutableList.of(Optional.empty(), Optional.of(new NullableValue(BIGINT, 1L))))
                .withPartitionChannels(0, -1) // use first block and constant block at index 1 as input to partitionFunction
                .withHashChannels(0, 1) // use both channels to calculate partition (a+b) mod 2
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(1L, 3L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(0L, 2L);
    }

    @Test
    public void testPartitionPositionsWithRleNotNull()
    {
        testPartitionPositionsWithRleNotNull(PartitioningMode.ROW_WISE);
        testPartitionPositionsWithRleNotNull(PartitioningMode.COLUMNAR);
    }

    private void testPartitionPositionsWithRleNotNull(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT, BIGINT).build();
        Page page = new Page(createRepeatedValuesBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition0HashBlock = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0HashBlock).containsOnly(0L).hasSize(POSITIONS_PER_PAGE);
        assertThat(outputBuffer.getEnqueuedDeserialized(1)).isEmpty();
    }

    @Test
    public void testPartitionPositionsWithRleNotNullWithReplication()
    {
        testPartitionPositionsWithRleNotNullWithReplication(PartitioningMode.ROW_WISE);
        testPartitionPositionsWithRleNotNullWithReplication(PartitioningMode.COLUMNAR);
    }

    private void testPartitionPositionsWithRleNotNullWithReplication(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT, BIGINT).replicate().build();
        Page page = new Page(createRepeatedValuesBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 1);
        assertThat(partition1).containsExactly(0L); // position 0 copied to all partitions
    }

    @Test
    public void testPartitionPositionsWithRleNullWithNullChannel()
    {
        testPartitionPositionsWithRleNullWithNullChannel(PartitioningMode.ROW_WISE);
        testPartitionPositionsWithRleNullWithNullChannel(PartitioningMode.COLUMNAR);
    }

    private void testPartitionPositionsWithRleNullWithNullChannel(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT, BIGINT).withNullChannel(0).build();
        Page page = new Page(RunLengthEncodedBlock.create(createLongsBlock((Long) null), POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 1);
        assertThat(partition1).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
    }

    @Test
    public void testOutputForDictionaryBlock()
    {
        testOutputForDictionaryBlock(PartitioningMode.ROW_WISE);
        testOutputForDictionaryBlock(PartitioningMode.COLUMNAR);
    }

    private void testOutputForDictionaryBlock(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        Page page = new Page(createLongDictionaryBlock(0, 10)); // must have at least 10 position to have non-trivial dict

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyElementsOf(nCopies(5, 0L));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyElementsOf(nCopies(5, 1L));
    }

    @Test
    public void testOutputForOneValueDictionaryBlock()
    {
        testOutputForOneValueDictionaryBlock(PartitioningMode.ROW_WISE);
        testOutputForOneValueDictionaryBlock(PartitioningMode.COLUMNAR);
    }

    private void testOutputForOneValueDictionaryBlock(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        Page page = new Page(DictionaryBlock.create(4, createLongsBlock(0), new int[] {0, 0, 0, 0}));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyElementsOf(nCopies(4, 0L));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).isEmpty();
    }

    @Test
    public void testOutputForViewDictionaryBlock()
    {
        testOutputForViewDictionaryBlock(PartitioningMode.ROW_WISE);
        testOutputForViewDictionaryBlock(PartitioningMode.COLUMNAR);
    }

    private void testOutputForViewDictionaryBlock(PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        Page page = new Page(DictionaryBlock.create(4, createLongSequenceBlock(4, 8), new int[] {1, 0, 3, 2}));

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyInAnyOrder(4L, 6L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyInAnyOrder(5L, 7L);
    }

    @Test
    public void testOutputForSimplePageWithType()
    {
        testOutputForSimplePageWithType(BIGINT, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(BOOLEAN, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(INTEGER, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(createCharType(10), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(createUnboundedVarcharType(), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(DOUBLE, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(SMALLINT, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(TINYINT, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(UUID, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(VARBINARY, PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(createDecimalType(1), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(createDecimalType(Decimals.MAX_SHORT_PRECISION + 1), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(new ArrayType(BIGINT), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(TimestampType.createTimestampType(9), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(TimestampType.createTimestampType(3), PartitioningMode.ROW_WISE);
        testOutputForSimplePageWithType(IPADDRESS, PartitioningMode.ROW_WISE);

        testOutputForSimplePageWithType(BIGINT, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(BOOLEAN, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(INTEGER, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(createCharType(10), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(createUnboundedVarcharType(), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(DOUBLE, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(SMALLINT, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(TINYINT, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(UUID, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(VARBINARY, PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(createDecimalType(1), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(createDecimalType(Decimals.MAX_SHORT_PRECISION + 1), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(new ArrayType(BIGINT), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(TimestampType.createTimestampType(9), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(TimestampType.createTimestampType(3), PartitioningMode.COLUMNAR);
        testOutputForSimplePageWithType(IPADDRESS, PartitioningMode.COLUMNAR);
    }

    private void testOutputForSimplePageWithType(Type type, PartitioningMode partitioningMode)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT, type).build();
        Page page = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE), // partition block
                createBlockForType(type, POSITIONS_PER_PAGE));
        List<Object> expected = readChannel(Stream.of(page), 1, type);

        processPages(pagePartitioner, partitioningMode, page);

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test
    public void testOutputWithMixedRowWiseAndColumnarPartitioning()
    {
        testOutputEqualsInput(BIGINT, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(BOOLEAN, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(INTEGER, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(createCharType(10), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(createUnboundedVarcharType(), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(DOUBLE, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(SMALLINT, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(TINYINT, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(UUID, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(VARBINARY, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(createDecimalType(1), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(createDecimalType(Decimals.MAX_SHORT_PRECISION + 1), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(new ArrayType(BIGINT), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(TimestampType.createTimestampType(9), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(TimestampType.createTimestampType(3), PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);
        testOutputEqualsInput(IPADDRESS, PartitioningMode.COLUMNAR, PartitioningMode.ROW_WISE);

        testOutputEqualsInput(BIGINT, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(BOOLEAN, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(INTEGER, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(createCharType(10), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(createUnboundedVarcharType(), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(DOUBLE, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(SMALLINT, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(TINYINT, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(UUID, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(VARBINARY, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(createDecimalType(1), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(createDecimalType(Decimals.MAX_SHORT_PRECISION + 1), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(new ArrayType(BIGINT), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(TimestampType.createTimestampType(9), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(TimestampType.createTimestampType(3), PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
        testOutputEqualsInput(IPADDRESS, PartitioningMode.ROW_WISE, PartitioningMode.COLUMNAR);
    }

    @Test
    public void testOutputBytesWhenReused()
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).build();
        OperatorContext operatorContext = operatorContext();

        Page page = new Page(createLongsBlock(1, 1, 1, 1, 1, 1));

        pagePartitioner.partitionPage(page, operatorContext);
        assertThat(operatorContext.getOutputDataSize().getTotalCount()).isEqualTo(0);
        pagePartitioner.prepareForRelease(operatorContext);
        assertThat(operatorContext.getOutputDataSize().getTotalCount()).isEqualTo(page.getSizeInBytes());
        // release again with no additional input, size should not change
        pagePartitioner.prepareForRelease(operatorContext);
        assertThat(operatorContext.getOutputDataSize().getTotalCount()).isEqualTo(page.getSizeInBytes());

        pagePartitioner.partitionPage(page, operatorContext);
        pagePartitioner.prepareForRelease(operatorContext);
        assertThat(operatorContext.getOutputDataSize().getTotalCount()).isEqualTo(page.getSizeInBytes() * 2);

        pagePartitioner.close();
        List<Slice> output = outputBuffer.getEnqueued();
        // only a single page was flushed after the partitioner is closed, all output bytes were reported eagerly on release
        assertThat(output.size()).isEqualTo(1);
    }

    @Test
    public void testMemoryReleased()
    {
        testMemoryReleased(PartitioningMode.ROW_WISE);
        testMemoryReleased(PartitioningMode.COLUMNAR);
    }

    private void testMemoryReleased(PartitioningMode partitioningMode)
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).withMemoryContext(memoryContext).build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(pagePartitioner, partitioningMode, page);

        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testMemoryReleasedOnFailure()
    {
        testMemoryReleasedOnFailure(PartitioningMode.ROW_WISE);
        testMemoryReleasedOnFailure(PartitioningMode.COLUMNAR);
    }

    private void testMemoryReleasedOnFailure(PartitioningMode partitioningMode)
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        RuntimeException exception = new RuntimeException();
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        outputBuffer.throwOnEnqueue(exception);
        PagePartitioner pagePartitioner = pagePartitioner(outputBuffer, BIGINT).withMemoryContext(memoryContext).build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        partitioningMode.partitionPage(pagePartitioner, page);

        assertThatThrownBy(pagePartitioner::close).isEqualTo(exception);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    private void testOutputEqualsInput(Type type, PartitioningMode mode1, PartitioningMode mode2)
    {
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        PagePartitionerBuilder pagePartitionerBuilder = pagePartitioner(outputBuffer, BIGINT, type, type);
        PagePartitioner pagePartitioner = pagePartitionerBuilder.build();
        Page input = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE), // partition block
                createBlockForType(type, POSITIONS_PER_PAGE),
                createBlockForType(type, POSITIONS_PER_PAGE));

        List<Object> expected = readChannel(Stream.of(input, input), 1, type);

        mode1.partitionPage(pagePartitioner, input);
        mode2.partitionPage(pagePartitioner, input);

        pagePartitioner.close();

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // output of the PagePartitioner can be reordered
        outputBuffer.clear();
    }

    private static Block createBlockForType(Type type, int positionsPerPage)
    {
        return createRandomBlockForType(type, positionsPerPage, 0.2F);
    }

    private static void processPages(PagePartitioner pagePartitioner, PartitioningMode partitioningMode, Page... pages)
    {
        for (Page page : pages) {
            partitioningMode.partitionPage(pagePartitioner, page);
        }
        pagePartitioner.close();
    }

    private static List<Object> readLongs(Stream<Page> pages, int channel)
    {
        return readChannel(pages, channel, BIGINT);
    }

    private static List<Object> readChannel(Stream<Page> pages, int channel, Type type)
    {
        List<Object> result = new ArrayList<>();

        pages.forEach(page -> {
            Block block = page.getBlock(channel);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    result.add(null);
                }
                else {
                    result.add(type.getObjectValue(null, block, i));
                }
            }
        });
        return unmodifiableList(result);
    }

    private PagePartitionerBuilder pagePartitioner(TestOutputBuffer outputBuffer, Type... types)
    {
        return pagePartitioner(ImmutableList.copyOf(types), outputBuffer);
    }

    private PagePartitionerBuilder pagePartitioner(List<Type> types, TestOutputBuffer outputBuffer)
    {
        return pagePartitioner(outputBuffer).withTypes(types);
    }

    private PagePartitionerBuilder pagePartitioner(TestOutputBuffer outputBuffer)
    {
        return new PagePartitionerBuilder(executor, scheduledExecutor, outputBuffer);
    }

    private enum PartitioningMode
    {
        ROW_WISE {
            @Override
            public void partitionPage(PagePartitioner pagePartitioner, Page page)
            {
                pagePartitioner.partitionPageByRow(page);
            }
        },
        COLUMNAR {
            @Override
            public void partitionPage(PagePartitioner pagePartitioner, Page page)
            {
                pagePartitioner.partitionPageByColumn(page);
            }
        };

        public abstract void partitionPage(PagePartitioner pagePartitioner, Page page);
    }

    public static class PagePartitionerBuilder
    {
        public static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory(new BlockTypeOperators());
        private final OutputBuffer outputBuffer;
        private final DriverContextBuilder driverContextBuilder;

        private ImmutableList<Integer> partitionChannels = ImmutableList.of(0);
        private List<Optional<NullableValue>> partitionConstants = ImmutableList.of();
        private PartitionFunction partitionFunction = new SumModuloPartitionFunction(PARTITION_COUNT, 0);
        private boolean shouldReplicate;
        private OptionalInt nullChannel = OptionalInt.empty();
        private List<Type> types;
        private AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();

        PagePartitionerBuilder(ExecutorService executor, ScheduledExecutorService scheduledExecutor, OutputBuffer outputBuffer)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.driverContextBuilder = new DriverContextBuilder(executor, scheduledExecutor);
        }

        public PagePartitionerBuilder withPartitionChannels(Integer... partitionChannels)
        {
            return withPartitionChannels(ImmutableList.copyOf(partitionChannels));
        }

        public PagePartitionerBuilder withPartitionChannels(ImmutableList<Integer> partitionChannels)
        {
            this.partitionChannels = partitionChannels;
            return this;
        }

        public PagePartitionerBuilder withPartitionConstants(List<Optional<NullableValue>> partitionConstants)
        {
            this.partitionConstants = partitionConstants;
            return this;
        }

        public PagePartitionerBuilder withHashChannels(int... hashChannels)
        {
            return withPartitionFunction(new SumModuloPartitionFunction(PARTITION_COUNT, hashChannels));
        }

        public PagePartitionerBuilder withPartitionFunction(PartitionFunction partitionFunction)
        {
            this.partitionFunction = partitionFunction;
            return this;
        }

        public PagePartitionerBuilder replicate()
        {
            return withShouldReplicate(true);
        }

        public PagePartitionerBuilder withShouldReplicate(boolean shouldReplicate)
        {
            this.shouldReplicate = shouldReplicate;
            return this;
        }

        public PagePartitionerBuilder withNullChannel(int nullChannel)
        {
            return withNullChannel(OptionalInt.of(nullChannel));
        }

        public PagePartitionerBuilder withNullChannel(OptionalInt nullChannel)
        {
            this.nullChannel = nullChannel;
            return this;
        }

        public PagePartitionerBuilder withTypes(Type... types)
        {
            return withTypes(ImmutableList.copyOf(types));
        }

        public PagePartitionerBuilder withTypes(List<Type> types)
        {
            this.types = types;
            return this;
        }

        public PagePartitionerBuilder withMemoryContext(AggregatedMemoryContext memoryContext)
        {
            this.memoryContext = memoryContext;
            return this;
        }

        public PartitionedOutputOperator buildPartitionedOutputOperator()
        {
            DriverContext driverContext = driverContextBuilder.buildDriverContext();

            OutputFactory operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    shouldReplicate,
                    nullChannel,
                    outputBuffer,
                    PARTITION_MAX_MEMORY,
                    POSITIONS_APPENDER_FACTORY,
                    Optional.empty(),
                    memoryContext,
                    1,
                    Optional.empty());
            OperatorFactory factory = operatorFactory.createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), PAGES_SERDE_FACTORY);
            PartitionedOutputOperator operator = (PartitionedOutputOperator) factory
                    .createOperator(driverContext);
            factory.noMoreOperators();
            return operator;
        }

        public PagePartitioner build()
        {
            return new PagePartitioner(
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    shouldReplicate,
                    nullChannel,
                    outputBuffer,
                    PAGES_SERDE_FACTORY,
                    types,
                    PARTITION_MAX_MEMORY,
                    POSITIONS_APPENDER_FACTORY,
                    Optional.empty(),
                    memoryContext,
                    true);
        }
    }

    public static class DriverContextBuilder
    {
        private final ExecutorService executor;
        private final ScheduledExecutorService scheduledExecutor;

        DriverContextBuilder(ExecutorService executor, ScheduledExecutorService scheduledExecutor)
        {
            this.executor = requireNonNull(executor, "executor is null");
            this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        }

        public DriverContext buildDriverContext()
        {
            return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }
    }

    public static class TestOutputBuffer
            implements OutputBuffer
    {
        private final Multimap<Integer, Slice> enqueued = ArrayListMultimap.create();
        private RuntimeException throwOnEnqueue;

        public Stream<Page> getEnqueuedDeserialized()
        {
            return getEnqueued().stream().map(PAGE_DESERIALIZER::deserialize);
        }

        public List<Slice> getEnqueued()
        {
            return ImmutableList.copyOf(enqueued.values());
        }

        public void clear()
        {
            enqueued.clear();
        }

        public Stream<Page> getEnqueuedDeserialized(int partition)
        {
            return getEnqueued(partition).stream().map(PAGE_DESERIALIZER::deserialize);
        }

        public List<Slice> getEnqueued(int partition)
        {
            Collection<Slice> serializedPages = enqueued.get(partition);
            return ImmutableList.copyOf(serializedPages);
        }

        public void throwOnEnqueue(RuntimeException throwOnEnqueue)
        {
            this.throwOnEnqueue = throwOnEnqueue;
        }

        @Override
        public void enqueue(int partition, List<Slice> pages)
        {
            if (throwOnEnqueue != null) {
                throw throwOnEnqueue;
            }
            enqueued.putAll(partition, pages);
        }

        @Override
        public OutputBufferInfo getInfo()
        {
            return null;
        }

        @Override
        public BufferState getState()
        {
            return BufferState.NO_MORE_BUFFERS;
        }

        @Override
        public double getUtilization()
        {
            return 0;
        }

        @Override
        public OutputBufferStatus getStatus()
        {
            return OutputBufferStatus.initial();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
        {
        }

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers)
        {
        }

        @Override
        public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
        {
            return null;
        }

        @Override
        public void acknowledge(OutputBufferId bufferId, long token)
        {
        }

        @Override
        public void destroy(OutputBufferId bufferId)
        {
        }

        @Override
        public ListenableFuture<Void> isFull()
        {
            return null;
        }

        @Override
        public void enqueue(List<Slice> pages)
        {
        }

        @Override
        public void setNoMorePages()
        {
        }

        @Override
        public void destroy()
        {
        }

        @Override
        public void abort()
        {
        }

        @Override
        public long getPeakMemoryUsage()
        {
            return 0;
        }

        @Override
        public Optional<Throwable> getFailureCause()
        {
            return Optional.empty();
        }
    }

    private record SumModuloPartitionFunction(int partitionCount, int... hashChannels)
            implements PartitionFunction
    {
        private SumModuloPartitionFunction
        {
            checkArgument(partitionCount > 0);
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = 0;
            for (int hashChannel : hashChannels) {
                value += BIGINT.getLong(page.getBlock(hashChannel), position);
            }

            return toIntExact(Math.abs(value) % partitionCount);
        }
    }
}
