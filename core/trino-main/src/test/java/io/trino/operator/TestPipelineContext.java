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

import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPipelineContext
{
    @Test
    public void testAlternativeOperatorsNotMerged()
    {
        MockScheduledExecutorService scheduledExecutor = new MockScheduledExecutorService();
        PipelineContext pipelineContext = TestingOperatorContext.createDriverContext(scheduledExecutor).getPipelineContext();
        DriverContext alternative0DriverContext = pipelineContext.addDriverContext();
        alternative0DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 0);
        alternative0DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        DriverContext alternative1DriverContext = pipelineContext.addDriverContext();
        alternative1DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 1);
        alternative1DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        pipelineContext.driverFinished(alternative0DriverContext);
        pipelineContext.driverFinished(alternative1DriverContext);

        PipelineStats pipelineStats = pipelineContext.getPipelineStats();

        List<OperatorStats> operatorSummaries = pipelineStats.getOperatorSummaries();
        assertThat(operatorSummaries).hasSize(2);
        assertThat(operatorSummaries.get(0).getOperatorId()).isEqualTo(0);
        assertThat(operatorSummaries.get(1).getOperatorId()).isEqualTo(0);
        assertThat(operatorSummaries.get(0).getAlternativeId()).isNotEqualTo(operatorSummaries.get(1).getAlternativeId());
    }

    @Test
    public void testOperatorsMerged()
    {
        MockScheduledExecutorService scheduledExecutor = new MockScheduledExecutorService();
        PipelineContext pipelineContext = TestingOperatorContext.createDriverContext(scheduledExecutor).getPipelineContext();
        DriverContext alternative0DriverContext = pipelineContext.addDriverContext();
        alternative0DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 3);
        alternative0DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        DriverContext alternative1DriverContext = pipelineContext.addDriverContext();
        alternative1DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 3);
        alternative1DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        pipelineContext.driverFinished(alternative0DriverContext);
        pipelineContext.driverFinished(alternative1DriverContext);

        PipelineStats pipelineStats = pipelineContext.getPipelineStats();

        List<OperatorStats> operatorSummaries = pipelineStats.getOperatorSummaries();
        assertThat(operatorSummaries).hasSize(1);
        assertThat(operatorSummaries.get(0).getOperatorId()).isEqualTo(0);
        assertThat(operatorSummaries.get(0).getAlternativeId()).isEqualTo(3);
    }

    private static class MockScheduledExecutorService
            extends AbstractExecutorService
            implements ScheduledExecutorService
    {
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTerminated()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command)
        {
            throw new UnsupportedOperationException();
        }
    }
}
