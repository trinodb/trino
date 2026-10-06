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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.output.TestPagePartitioner.PagePartitionerBuilder;
import io.trino.operator.output.TestPagePartitioner.TestOutputBuffer;
import io.trino.spi.Page;
import io.trino.spi.block.DictionaryBlock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPartitionedOutputOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUpClass()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterAll
    public void tearDownClass()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testOperatorContextStats()
            throws Exception
    {
        try (PartitionedOutputOperator partitionedOutputOperator = new PagePartitionerBuilder(executor, scheduledExecutor, new TestOutputBuffer())
                .withTypes(BIGINT).buildPartitionedOutputOperator()) {
            Page page = new Page(createLongSequenceBlock(0, 8));

            partitionedOutputOperator.addInput(page);

            OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
            assertThat(operatorContext.getOutputDataSize()).isEqualTo(0);
            assertThat(operatorContext.getOutputPositions()).isEqualTo(page.getPositionCount());

            partitionedOutputOperator.finish();
            assertThat(operatorContext.getOutputDataSize()).isEqualTo(page.getSizeInBytes());
        }
    }

    @Test
    void testMemoryReleasedOnFinishFailure()
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        RuntimeException exception = new RuntimeException();
        TestOutputBuffer outputBuffer = new TestOutputBuffer();
        outputBuffer.throwOnEnqueue(exception);
        PartitionedOutputOperator partitionedOutputOperator = new PagePartitionerBuilder(executor, scheduledExecutor, outputBuffer)
                .withTypes(BIGINT)
                .withMemoryContext(memoryContext)
                .buildPartitionedOutputOperator();
        // repeated dictionary ids make finish flush the buffered page
        partitionedOutputOperator.addInput(new Page(DictionaryBlock.create(4, createLongsBlock(0, 1), new int[] {0, 1, 0, 1})));
        assertThat(memoryContext.getBytes()).isPositive();

        assertThatThrownBy(partitionedOutputOperator::finish).isSameAs(exception);
        assertThat(memoryContext.getBytes()).isZero();
    }
}
