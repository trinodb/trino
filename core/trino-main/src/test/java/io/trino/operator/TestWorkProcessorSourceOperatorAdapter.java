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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.operator.WorkProcessorSourceOperatorAdapter.AdapterWorkProcessorSourceOperatorFactory;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWorkProcessorSourceOperatorAdapter
{
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMetrics()
    {
        DriverContext driverContext = TestingTaskContext.builder(MoreExecutors.directExecutor(), scheduledExecutor, TEST_SESSION)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        TestWorkProcessorOperatorFactory workProcessorFactory = new TestWorkProcessorOperatorFactory();
        OperatorContext context = driverContext.addOperatorContext(
                workProcessorFactory.getOperatorId(),
                workProcessorFactory.getPlanNodeId(),
                workProcessorFactory.getOperatorType());
        Operator operator = new WorkProcessorSourceOperatorAdapter(context, workProcessorFactory);

        operator.getOutput();
        assertThat(operator.isFinished()).isFalse();
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getMetrics().getMetrics())
                .hasSize(5)
                .containsEntry("testOperatorMetric", new LongCount(1));
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of(
                "testConnectorMetric", new LongCount(2)));
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getPhysicalInputReadTime())
                .isEqualTo(new Duration(7, NANOSECONDS));

        operator.getOutput();
        assertThat(operator.isFinished()).isTrue();
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getMetrics().getMetrics())
                .hasSize(5)
                .containsEntry("testOperatorMetric", new LongCount(2));
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of(
                "testConnectorMetric", new LongCount(3)));
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getPhysicalInputReadTime())
                .isEqualTo(new Duration(7, NANOSECONDS));
    }

    private static class TestWorkProcessorOperatorFactory
            implements AdapterWorkProcessorSourceOperatorFactory
    {
        @Override
        public WorkProcessorSourceOperator create(
                OperatorContext operatorContext,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> splits)
        {
            return new TestWorkProcessorOperator();
        }

        @Override
        public int getOperatorId()
        {
            return 0;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return new PlanNodeId("test");
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return new PlanNodeId("test");
        }

        @Override
        public String getOperatorType()
        {
            return "test";
        }
    }

    private static class TestWorkProcessorOperator
            implements WorkProcessorSourceOperator
    {
        private long count;

        @Override
        public Metrics getMetrics()
        {
            return new Metrics(ImmutableMap.of("testOperatorMetric", new LongCount(count)));
        }

        @Override
        public Metrics getConnectorMetrics()
        {
            return new Metrics(ImmutableMap.of("testConnectorMetric", new LongCount(count + 1)));
        }

        @Override
        public Duration getReadTime()
        {
            return new Duration(7, NANOSECONDS);
        }

        @Override
        public WorkProcessor<Page> getOutputPages()
        {
            return WorkProcessor.of(new Page(0))
                    .withProcessEntryMonitor(() -> count++);
        }
    }
}
