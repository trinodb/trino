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
package io.trino.server.remotetask;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.operator.RetryPolicy;
import io.trino.operator.TaskStats;
import io.trino.server.DynamicFilterService;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.execution.TaskStatus.STARTING_VERSION;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskInfoFetcherInstanceMismatch
{
    private static final TaskId TASK_ID = new TaskId(new StageId("test", 1), 0, 0);
    private static final URI TASK_URI = URI.create("http://fake.invalid/task/node/test.1.0.0");

    @Test
    @Timeout(10)
    public void testInstanceMismatchFinalizesTaskInfo()
            throws Exception
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            long originalInstanceId = 42L;
            TaskInfo initialTaskInfo = createTaskInfo(originalInstanceId, STARTING_VERSION, RUNNING);

            ContinuousTaskStatusFetcher statusFetcher = createStatusFetcher(
                    initialTaskInfo.taskStatus(), executor);
            TaskInfoFetcher fetcher = createFetcher(initialTaskInfo, statusFetcher, executor);

            // Simulate coordinator force-failing the status (as failLocallyImmediately does)
            TaskStatus failedStatus = TaskStatus.failWith(
                    statusFetcher.getTaskStatus(), FAILED, ImmutableList.of());
            statusFetcher.updateTaskStatus(failedStatus);

            CompletableFuture<TaskInfo> finalTaskInfoFuture = new CompletableFuture<>();
            fetcher.addFinalTaskInfoListener(finalTaskInfoFuture::complete);

            // Feed a response with a different instance id and low version
            fetcher.updateTaskInfo(createTaskInfo(999L, 1, RUNNING));

            // finalTaskInfo must be set promptly using the authoritative local FAILED status
            TaskInfo finalTaskInfo = finalTaskInfoFuture.get(5, SECONDS);
            assertThat(finalTaskInfo.taskStatus().state()).isEqualTo(FAILED);
            assertThat(finalTaskInfo.taskStatus().taskInstanceId()).isEqualTo(originalInstanceId);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testMismatchWithLowVersionStillFinalizes()
            throws Exception
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            long originalInstanceId = 42L;
            long highVersion = 50L;
            TaskInfo initialTaskInfo = createTaskInfo(originalInstanceId, highVersion, RUNNING);

            ContinuousTaskStatusFetcher statusFetcher = createStatusFetcher(
                    initialTaskInfo.taskStatus(), executor);
            TaskInfoFetcher fetcher = createFetcher(initialTaskInfo, statusFetcher, executor);

            // Force status to FAILED
            TaskStatus failedStatus = TaskStatus.failWith(
                    statusFetcher.getTaskStatus(), FAILED, ImmutableList.of());
            statusFetcher.updateTaskStatus(failedStatus);

            CompletableFuture<TaskInfo> finalTaskInfoFuture = new CompletableFuture<>();
            fetcher.addFinalTaskInfoListener(finalTaskInfoFuture::complete);

            // Mismatched instance with version=1, much lower than held version=50
            // Without the fix, updateTaskInfo silently rejects this (version too low)
            // and finalTaskInfo stays empty.
            fetcher.updateTaskInfo(createTaskInfo(999L, 1, RUNNING));

            TaskInfo finalTaskInfo = finalTaskInfoFuture.get(5, SECONDS);
            assertThat(finalTaskInfo.taskStatus().state()).isEqualTo(FAILED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testNormalResponseIsNotMismatch()
            throws Exception
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            long originalInstanceId = 42L;
            TaskInfo initialTaskInfo = createTaskInfo(originalInstanceId, STARTING_VERSION, RUNNING);

            ContinuousTaskStatusFetcher statusFetcher = createStatusFetcher(
                    initialTaskInfo.taskStatus(), executor);
            TaskInfoFetcher fetcher = createFetcher(initialTaskInfo, statusFetcher, executor);

            CompletableFuture<TaskInfo> finalTaskInfoFuture = new CompletableFuture<>();
            fetcher.addFinalTaskInfoListener(finalTaskInfoFuture::complete);

            // Normal done response from the same instance — must finalize
            fetcher.updateTaskInfo(createTaskInfo(originalInstanceId, STARTING_VERSION + 1, TaskState.FINISHED));

            TaskInfo finalTaskInfo = finalTaskInfoFuture.get(5, SECONDS);
            assertThat(finalTaskInfo.taskStatus().state()).isEqualTo(TaskState.FINISHED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static TaskInfo createTaskInfo(long taskInstanceId, long version, TaskState state)
    {
        TaskStatus status = new TaskStatus(
                TASK_ID,
                taskInstanceId,
                version,
                state,
                TASK_URI,
                "node-id",
                false,
                ImmutableList.of(),
                0,
                0,
                OutputBufferStatus.initial(),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                OptionalInt.empty(),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                0,
                new Duration(0, MILLISECONDS),
                0,
                0,
                0);

        return new TaskInfo(
                status,
                Instant.now(),
                new OutputBufferInfo(
                        "UNINITIALIZED",
                        BufferState.OPEN,
                        true,
                        true,
                        0,
                        0,
                        0,
                        0,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                ImmutableSet.of(),
                new TaskStats(Instant.now(), null),
                Optional.empty(),
                false);
    }

    private static ContinuousTaskStatusFetcher createStatusFetcher(
            TaskStatus initialStatus,
            ScheduledExecutorService executor)
    {
        DynamicFiltersFetcher dynamicFiltersFetcher = new DynamicFiltersFetcher(
                _ -> {},
                TASK_ID,
                TASK_URI,
                new Duration(10, SECONDS),
                JsonCodec.jsonCodec(VersionedDynamicFilterDomains.class),
                executor,
                new TestingHttpClient(_ -> { throw new UnsupportedOperationException(); }),
                () -> noopTracer().spanBuilder("test"),
                new Duration(10, SECONDS),
                executor,
                new RemoteTaskStats(),
                new DynamicFilterService(
                        createTestingMetadataManager(),
                        PLANNER_CONTEXT.getFunctionManager(),
                        new TypeOperators(),
                        new DynamicFilterConfig()));

        return new ContinuousTaskStatusFetcher(
                _ -> {},
                initialStatus,
                new Duration(10, SECONDS),
                JsonCodec.jsonCodec(TaskStatus.class),
                dynamicFiltersFetcher,
                executor,
                new TestingHttpClient(_ -> { throw new UnsupportedOperationException(); }),
                () -> noopTracer().spanBuilder("test"),
                new Duration(10, SECONDS),
                executor,
                new RemoteTaskStats());
    }

    private static TaskInfoFetcher createFetcher(
            TaskInfo initialTask,
            ContinuousTaskStatusFetcher statusFetcher,
            ScheduledExecutorService executor)
    {
        return new TaskInfoFetcher(
                _ -> {},
                statusFetcher,
                initialTask,
                new TestingHttpClient(_ -> { throw new UnsupportedOperationException(); }),
                () -> noopTracer().spanBuilder("test"),
                new Duration(10, SECONDS),
                JsonCodec.jsonCodec(TaskInfo.class),
                new Duration(10, SECONDS),
                false,
                executor,
                executor,
                executor,
                new RemoteTaskStats(),
                Optional.empty(),
                RetryPolicy.NONE);
    }
}
