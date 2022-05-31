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

import io.trino.operator.OperatorContext;
import io.trino.operator.output.TestPagePartitioner.PagePartitionerBuilder;
import io.trino.operator.output.TestPagePartitioner.TestOutputBuffer;
import io.trino.spi.Page;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPartitionedOutputOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private TestOutputBuffer outputBuffer;

    @BeforeClass
    public void setUpClass()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @BeforeMethod
    public void setUp()
    {
        outputBuffer = new TestOutputBuffer();
    }

    @Test
    public void testOperatorContextStats()
    {
        PartitionedOutputOperator partitionedOutputOperator = new PagePartitionerBuilder(executor, scheduledExecutor, outputBuffer)
                .withTypes(BIGINT).buildPartitionedOutputOperator();
        Page page = new Page(createLongSequenceBlock(0, 8));

        partitionedOutputOperator.addInput(page);
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), page.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), page.getPositionCount());
    }
}
