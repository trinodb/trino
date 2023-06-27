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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.spiller.SpillSpaceTracker;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertBetweenInclusive;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.finishOperator;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class GroupByHashYieldAssertion
{
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("GroupByHashYieldAssertion-%s"));
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newScheduledThreadPool(2, daemonThreadsNamed("GroupByHashYieldAssertion-scheduledExecutor-%s"));

    private GroupByHashYieldAssertion() {}

    public static List<Page> createPagesWithDistinctHashKeys(Type type, int pageCount, int positionCountPerPage)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(true, ImmutableList.of(0), type);
        for (int i = 0; i < pageCount; i++) {
            rowPagesBuilder.addSequencePage(positionCountPerPage, positionCountPerPage * i);
        }
        return rowPagesBuilder.build();
    }

    /**
     * @param operatorFactory creates an Operator that should directly or indirectly contain GroupByHash
     * @param getHashCapacity returns the hash table capacity for the input operator
     * @param additionalMemoryInBytes the memory used in addition to the GroupByHash in the operator (e.g., aggregator)
     */
    public static GroupByHashYieldResult finishOperatorWithYieldingGroupByHash(List<Page> input, Type hashKeyType, OperatorFactory operatorFactory, Function<Operator, Integer> getHashCapacity, long additionalMemoryInBytes)
    {
        assertLessThan(additionalMemoryInBytes, 1L << 21, "additionalMemoryInBytes should be a relatively small number");
        List<Page> result = new LinkedList<>();

        // mock an adjustable memory pool
        QueryId queryId = new QueryId("test_query");
        TaskId anotherTaskId = new TaskId(new StageId("another_query", 0), 0, 0);
        MemoryPool memoryPool = new MemoryPool(DataSize.of(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(
                queryId,
                DataSize.of(512, MEGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                EXECUTOR,
                SCHEDULED_EXECUTOR,
                DataSize.of(512, MEGABYTE),
                new SpillSpaceTracker(DataSize.of(512, MEGABYTE)));

        DriverContext driverContext = createTaskContext(queryContext, EXECUTOR, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        Operator operator = operatorFactory.createOperator(driverContext);

        byte[] pointer = new byte[VariableWidthData.POINTER_SIZE];
        VariableWidthData variableWidthData = new VariableWidthData();

        // run operator
        int yieldCount = 0;
        long maxReservedBytes = 0;
        for (Page page : input) {
            // compute the memory reserved by the variable width data allocator for this page
            long pageVariableWidthSize = 0;
            if (hashKeyType == VARCHAR) {
                long oldVariableWidthSize = variableWidthData.getRetainedSizeBytes();
                for (int position = 0; position < page.getPositionCount(); position++) {
                    variableWidthData.allocate(pointer, 0, page.getBlock(0).getSliceLength(position));
                }
                pageVariableWidthSize = variableWidthData.getRetainedSizeBytes() - oldVariableWidthSize;
            }

            // unblocked
            assertTrue(operator.needsInput());

            // reserve the most of the memory pool, except for the space necessary for the variable with data
            // a small bit of memory is left unallocated for the aggregators
            memoryPool.reserve(anotherTaskId, "test", memoryPool.getFreeBytes() - additionalMemoryInBytes - pageVariableWidthSize);

            long oldMemoryUsage = operator.getOperatorContext().getDriverContext().getMemoryUsage();
            int oldCapacity = getHashCapacity.apply(operator);

            // add a page and verify different behaviors
            operator.addInput(page);

            // get output to consume the input
            Page output = operator.getOutput();
            if (output != null) {
                result.add(output);
            }

            long newMemoryUsage = operator.getOperatorContext().getDriverContext().getMemoryUsage();
            maxReservedBytes = max(maxReservedBytes, newMemoryUsage);

            // Skip if the memory usage is not large enough since we cannot distinguish
            // between rehash and memory used by aggregator
            if (newMemoryUsage < DataSize.of(4, MEGABYTE).toBytes()) {
                // free the pool for the next iteration
                memoryPool.free(anotherTaskId, "test", memoryPool.getTaskMemoryReservations().get(anotherTaskId));
                // this required in case input is blocked
                output = operator.getOutput();
                if (output != null) {
                    result.add(output);
                }
                continue;
            }

            long actualHashIncreased = newMemoryUsage - oldMemoryUsage - pageVariableWidthSize;

            if (operator.needsInput()) {
                // The page processing completed

                // Assert we are not blocked
                assertTrue(operator.getOperatorContext().isWaitingForMemory().isDone());

                // assert the hash capacity is not changed; otherwise, we should have yielded
                assertEquals((int) getHashCapacity.apply(operator), oldCapacity);

                // We are not going to rehash; therefore, assert the memory increase only comes from the aggregator
                assertLessThan(actualHashIncreased, additionalMemoryInBytes);

                // free the pool for the next iteration
                memoryPool.free(anotherTaskId, "test", memoryPool.getTaskMemoryReservations().get(anotherTaskId));
            }
            else {
                // Page processing is not completed
                yieldCount++;

                // Assert we are blocked waiting for memory
                assertFalse(operator.getOperatorContext().isWaitingForMemory().isDone());

                // Hash table capacity should not have changed, because memory must be allocated first
                assertEquals(oldCapacity, (long) getHashCapacity.apply(operator));

                // The increase in hash memory should be twice the current capacity.
                // The additional memory for the entire new hash table is reserved before rehashing because the new and old hash tables coexist during rehashing.
                long expectedHashBytes = getHashTableSizeInBytes(hashKeyType, oldCapacity * 2);
                assertBetweenInclusive(actualHashIncreased, expectedHashBytes, expectedHashBytes + additionalMemoryInBytes);

                // Output should be blocked as well
                assertNull(operator.getOutput());

                // Free the pool to unblock
                memoryPool.free(anotherTaskId, "test", memoryPool.getTaskMemoryReservations().get(anotherTaskId));

                // Trigger a process through getOutput() or needsInput()
                output = operator.getOutput();
                if (output != null) {
                    result.add(output);
                }
                assertTrue(operator.needsInput());

                // Hash table capacity has increased
                assertGreaterThan(getHashCapacity.apply(operator), oldCapacity);

                // Assert the estimated reserved memory after rehash is lower than the one before rehash (extra memory allocation has been released)
                long rehashedMemoryUsage = operator.getOperatorContext().getDriverContext().getMemoryUsage();
                long previousHashTableSizeInBytes = getHashTableSizeInBytes(hashKeyType, oldCapacity);
                long expectedMemoryUsageAfterRehash = newMemoryUsage - previousHashTableSizeInBytes;
                double memoryUsageErrorUpperBound = 1.01;
                double memoryUsageError = rehashedMemoryUsage * 1.0 / expectedMemoryUsageAfterRehash;
                if (memoryUsageError > memoryUsageErrorUpperBound) {
                    // Usually the error is < 1%, but since MultiChannelGroupByHash.getEstimatedSize
                    // accounts for changes in completedPagesMemorySize, which is increased if new page is
                    // added by addNewGroup (an even that cannot be predicted as it depends on the number of unique groups
                    // in the current page being processed), the difference includes the size of the added new page.
                    // Lower bound is 1% lower than normal because "additionalMemoryInBytes" includes also aggregator state.
                    assertBetweenInclusive(rehashedMemoryUsage * 1.0 / (expectedMemoryUsageAfterRehash + additionalMemoryInBytes), 0.97, memoryUsageErrorUpperBound,
                            "rehashedMemoryUsage " + rehashedMemoryUsage + ", expectedMemoryUsageAfterRehash: " + expectedMemoryUsageAfterRehash);
                }
                else {
                    assertBetweenInclusive(memoryUsageError, 0.99, memoryUsageErrorUpperBound);
                }

                // unblocked
                assertTrue(operator.needsInput());
                assertTrue(operator.getOperatorContext().isWaitingForMemory().isDone());
            }
        }

        result.addAll(finishOperator(operator));
        return new GroupByHashYieldResult(yieldCount, maxReservedBytes, result);
    }

    private static long getHashTableSizeInBytes(Type hashKeyType, int capacity)
    {
        if (hashKeyType == BIGINT) {
            // groupIds and values double by hashCapacity; while valuesByGroupId double by maxFill = hashCapacity / 0.75
            return capacity * (long) (Long.BYTES * 1.75 + Integer.BYTES);
        }

        @SuppressWarnings("OverlyComplexArithmeticExpression")
        int sizePerEntry = Byte.BYTES + // control byte
                Integer.BYTES + // groupId to hashPosition
                VariableWidthData.POINTER_SIZE + // variable width pointer
                Integer.BYTES + // groupId
                Long.BYTES + // rawHash (optional, but present in this test)
                Byte.BYTES + // field null
                Integer.BYTES + // field variable length
                Long.BYTES + // field first 8 bytes
                Integer.BYTES; // field variable offset (or 4 more field bytes)
        return (long) capacity * sizePerEntry;
    }

    public static final class GroupByHashYieldResult
    {
        private final int yieldCount;
        private final long maxReservedBytes;
        private final List<Page> output;

        public GroupByHashYieldResult(int yieldCount, long maxReservedBytes, List<Page> output)
        {
            this.yieldCount = yieldCount;
            this.maxReservedBytes = maxReservedBytes;
            this.output = requireNonNull(output, "output is null");
        }

        public int getYieldCount()
        {
            return yieldCount;
        }

        public long getMaxReservedBytes()
        {
            return maxReservedBytes;
        }

        public List<Page> getOutput()
        {
            return output;
        }
    }
}
