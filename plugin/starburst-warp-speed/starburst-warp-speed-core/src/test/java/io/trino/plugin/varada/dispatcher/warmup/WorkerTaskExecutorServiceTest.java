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
package io.trino.plugin.varada.dispatcher.warmup;

import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsWorkerTaskExecutorService;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.util.UUID;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService.WORKER_TASK_EXECUTOR_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerTaskExecutorServiceTest
{
    private WorkerTaskExecutorService taskExecutorService;
    private VaradaStatsWorkerTaskExecutorService statsWorkerTaskExecutorService;

    @BeforeEach
    public void before()
    {
        this.statsWorkerTaskExecutorService = new VaradaStatsWorkerTaskExecutorService(WORKER_TASK_EXECUTOR_STAT_GROUP);
        MetricsManager metricManager = mock(MetricsManager.class);
        when(metricManager.registerMetric(any())).thenReturn(statsWorkerTaskExecutorService);
        this.taskExecutorService = new WorkerTaskExecutorService(
                new WarmupDemoterConfiguration(),
                new NativeConfiguration(),
                metricManager);
    }

    @Test
    public void testAddPendingTask()
    {
        RowGroupKey key = new RowGroupKey("s", "t", "fp", 0, 0, 0, "", "");
        TestSubmittableTask task1 = new TestSubmittableTask(key, 1);
        taskExecutorService.submitTask(task1, true);
        assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(1);
        TestSubmittableTask task2 = new TestSubmittableTask(key, 1);
        taskExecutorService.submitTask(task2, true);
        assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(1);
        assertThat(this.statsWorkerTaskExecutorService.gettask_pended()).isEqualTo(1);

        taskExecutorService.taskFinished(key); // task1 finished
        assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(2);
        assertThat(this.statsWorkerTaskExecutorService.gettask_finished()).isEqualTo(1);

        taskExecutorService.taskFinished(key); // task2 finished
        assertThat(this.statsWorkerTaskExecutorService.gettask_finished()).isEqualTo(2);
        assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(2);
        assertThat(this.statsWorkerTaskExecutorService.gettask_pended()).isEqualTo(1);
    }

    @Test
    public void testDifferentKeysShouldScheduled()
    {
        RowGroupKey key1 = new RowGroupKey("s", "t", "fp", 0, 0, 0, "", "");
        RowGroupKey key2 = new RowGroupKey("s", "t", "fp1", 0, 0, 0, "", "");
        TestSubmittableTask task1 = new TestSubmittableTask(key1, 1);
        TestSubmittableTask task2 = new TestSubmittableTask(key2, 1);
        taskExecutorService.submitTask(task1, true);
        taskExecutorService.submitTask(task2, true);
        assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(2);
    }

    @Test
    public void testMultiplePending()
    {
        RowGroupKey key1 = new RowGroupKey("s", "t", "fp", 0, 0, 0, "", "");
        TestSubmittableTask task1 = new TestSubmittableTask(key1, 1);
        TestSubmittableTask task2 = new TestSubmittableTask(key1, 1);
        TestSubmittableTask task3 = new TestSubmittableTask(key1, 1);
        TestSubmittableTask task4 = new TestSubmittableTask(key1, 1);
        TestSubmittableTask task5 = new TestSubmittableTask(key1, 1);
        taskExecutorService.submitTask(task1, true);
        taskExecutorService.submitTask(task2, true);
        taskExecutorService.submitTask(task3, true);
        taskExecutorService.submitTask(task4, true);
        taskExecutorService.submitTask(task5, true);
        taskExecutorService.taskFinished(key1);
        taskExecutorService.taskFinished(key1);
        taskExecutorService.taskFinished(key1);
        taskExecutorService.taskFinished(key1);
        taskExecutorService.taskFinished(key1);
        assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(5);
    }

    @Test
    public void testDelayedTask()
    {
        RowGroupKey key1 = new RowGroupKey("s", "t", "fp", 0, 0, 0, "", "");
        TestSubmittableTask task1 = new TestSubmittableTask(key1, 1);
        taskExecutorService.delaySubmit(3, task1, this::handleConflict);
        Failsafe.with(new RetryPolicy<>()
                        .handle(AssertionFailedError.class)
                        .withMaxRetries(10)
                        .withDelay(Duration.ofSeconds(1)))
                .run(() -> {
                    assertThat(this.statsWorkerTaskExecutorService.gettask_scheduled()).isEqualTo(1);
                    assertThat(this.statsWorkerTaskExecutorService.gettask_delayed()).isEqualTo(1);
                });
    }

    private void handleConflict(WorkerSubmittableTask task)
    {
        assertThat(task).isNotNull(); // do nothing
    }

    private static class TestSubmittableTask
            implements WorkerSubmittableTask
    {
        private final RowGroupKey key;
        private final int priority;
        private final UUID id;

        public TestSubmittableTask(RowGroupKey key, int priority)
        {
            this.priority = priority;
            this.key = key;
            this.id = UUID.randomUUID();
        }

        @Override
        public UUID getId()
        {
            return id;
        }

        @Override
        public int getPriority()
        {
            return priority;
        }

        @Override
        public RowGroupKey getRowGroupKey()
        {
            return key;
        }

        @Override
        public void taskScheduled()
        {
        }

        @Override
        public void run()
        {
        }
    }
}
