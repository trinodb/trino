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
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperatorFactory;
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
import static io.trino.operator.WorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWorkProcessorOperatorAdapter
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
        OperatorFactory factory = createAdapterOperatorFactory(new TestWorkProcessorOperatorFactory());
        Operator operator = factory.createOperator(driverContext);

        OperatorContext context = operator.getOperatorContext();

        operator.getOutput();
        assertThat(operator.isFinished()).isFalse();
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getMetrics().getMetrics()).isEqualTo(ImmutableMap.of("testOperatorMetric", new LongCount(1)));

        operator.getOutput();
        assertThat(operator.isFinished()).isTrue();
        assertThat(getOnlyElement(context.getNestedOperatorStats()).getMetrics().getMetrics()).isEqualTo(ImmutableMap.of("testOperatorMetric", new LongCount(2)));
    }

    private static class TestWorkProcessorOperatorFactory
            implements AdapterWorkProcessorOperatorFactory
    {
        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AdapterWorkProcessorOperator createAdapterOperator(ProcessorContext processorContext)
        {
            return new TestWorkProcessorOperator();
        }

        @Override
        public AdapterWorkProcessorOperatorFactory duplicate()
        {
            return new TestWorkProcessorOperatorFactory();
        }

        @Override
        public int getOperatorId()
        {
            return 0;
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
            implements AdapterWorkProcessorOperator
    {
        private long count;

        @Override
        public Metrics getMetrics()
        {
            return new Metrics(ImmutableMap.of("testOperatorMetric", new LongCount(count)));
        }

        @Override
        public WorkProcessor<Page> getOutputPages()
        {
            return WorkProcessor.of(new Page(0))
                    .withProcessEntryMonitor(() -> count++);
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void finish()
        {
        }
    }
}
