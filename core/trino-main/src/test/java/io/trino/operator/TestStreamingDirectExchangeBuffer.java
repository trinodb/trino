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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStreamingDirectExchangeBuffer
{
    private static final StageId STAGE_ID = new StageId(new QueryId("query"), 0);
    private static final TaskId TASK_0 = new TaskId(STAGE_ID, 0, 0);
    private static final TaskId TASK_1 = new TaskId(STAGE_ID, 1, 0);
    private static final Slice PAGE_0 = utf8Slice("page0");
    private static final Slice PAGE_1 = utf8Slice("page-1");
    private static final Slice PAGE_2 = utf8Slice("page-_2");

    @Test
    public void testHappyPath()
    {
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertThat(buffer.isFinished()).isFalse();
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertThat(blocked.isDone()).isFalse();
            assertThat(buffer.pollPage()).isNull();

            buffer.addTask(TASK_0);
            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isFalse();
            assertThat(buffer.pollPage()).isNull();

            buffer.addTask(TASK_1);
            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isFalse();
            assertThat(buffer.pollPage()).isNull();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isFalse();
            assertThat(buffer.pollPage()).isNull();

            buffer.addPages(TASK_0, ImmutableList.of(PAGE_0));
            assertThat(buffer.getBufferedPageCount()).isEqualTo(1);
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(PAGE_0.getRetainedSize());
            assertThat(buffer.getMaxRetainedSizeInBytes()).isEqualTo(PAGE_0.getRetainedSize());
            assertThat(buffer.getRemainingCapacityInBytes()).isEqualTo(DataSize.of(1, KILOBYTE).toBytes() - PAGE_0.getRetainedSize());
            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isTrue();
            assertThat(buffer.pollPage()).isEqualTo(PAGE_0);
            blocked = buffer.isBlocked();
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(0);
            assertThat(buffer.getMaxRetainedSizeInBytes()).isEqualTo(PAGE_0.getRetainedSize());
            assertThat(buffer.getRemainingCapacityInBytes()).isEqualTo(DataSize.of(1, KILOBYTE).toBytes());
            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isFalse();

            buffer.taskFinished(TASK_0);
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isBlocked().isDone()).isFalse();

            buffer.addPages(TASK_1, ImmutableList.of(PAGE_1, PAGE_2));
            assertThat(buffer.getBufferedPageCount()).isEqualTo(2);
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(PAGE_1.getRetainedSize() + PAGE_2.getRetainedSize());
            assertThat(buffer.getMaxRetainedSizeInBytes()).isEqualTo(PAGE_1.getRetainedSize() + PAGE_2.getRetainedSize());
            assertThat(buffer.getRemainingCapacityInBytes()).isEqualTo(DataSize.of(1, KILOBYTE).toBytes() - PAGE_1.getRetainedSize() - PAGE_2.getRetainedSize());
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isBlocked().isDone()).isTrue();
            assertThat(buffer.pollPage()).isEqualTo(PAGE_1);
            assertThat(buffer.pollPage()).isEqualTo(PAGE_2);
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isBlocked().isDone()).isFalse();
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(0);
            assertThat(buffer.getMaxRetainedSizeInBytes()).isEqualTo(PAGE_1.getRetainedSize() + PAGE_2.getRetainedSize());
            assertThat(buffer.getRemainingCapacityInBytes()).isEqualTo(DataSize.of(1, KILOBYTE).toBytes());

            buffer.taskFinished(TASK_1);
            assertThat(buffer.isFinished()).isTrue();
            assertThat(blocked.isDone()).isTrue();
        }
    }

    @Test
    public void testClose()
    {
        StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE));
        buffer.addTask(TASK_0);
        buffer.addTask(TASK_1);

        assertThat(buffer.isFinished()).isFalse();
        assertThat(buffer.isBlocked().isDone()).isFalse();
        assertThat(buffer.pollPage()).isNull();

        buffer.close();

        assertThat(buffer.isFinished()).isTrue();
        assertThat(buffer.isBlocked().isDone()).isTrue();
        assertThat(buffer.pollPage()).isNull();
    }

    @Test
    public void testIsFinished()
    {
        // 0 tasks
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertThat(buffer.isFinished()).isFalse();
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertThat(blocked.isDone()).isFalse();

            buffer.noMoreTasks();

            assertThat(buffer.isFinished()).isTrue();
            assertThat(blocked.isDone()).isTrue();
        }

        // single task
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertThat(buffer.isFinished()).isFalse();
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertThat(blocked.isDone()).isFalse();

            buffer.addTask(TASK_0);
            buffer.noMoreTasks();

            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isFalse();

            buffer.taskFinished(TASK_0);

            assertThat(buffer.isFinished()).isTrue();
            assertThat(blocked.isDone()).isTrue();
        }

        // single failed task
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertThat(buffer.isFinished()).isFalse();
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertThat(blocked.isDone()).isFalse();

            buffer.addTask(TASK_0);

            assertThat(buffer.isFinished()).isFalse();
            assertThat(blocked.isDone()).isFalse();

            RuntimeException error = new RuntimeException();
            buffer.taskFailed(TASK_0, error);

            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isTrue();
            assertThat(blocked.isDone()).isTrue();
            assertThatThrownBy(buffer::pollPage).isEqualTo(error);
        }
    }

    @Test
    public void testFutureCancellationDoesNotAffectOtherFutures()
    {
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertThat(buffer.isFinished()).isFalse();

            ListenableFuture<Void> blocked1 = buffer.isBlocked();
            ListenableFuture<Void> blocked2 = buffer.isBlocked();
            ListenableFuture<Void> blocked3 = buffer.isBlocked();

            assertThat(blocked1.isDone()).isFalse();
            assertThat(blocked2.isDone()).isFalse();
            assertThat(blocked3.isDone()).isFalse();

            blocked3.cancel(true);
            assertThat(blocked1.isDone()).isFalse();
            assertThat(blocked2.isDone()).isFalse();

            buffer.noMoreTasks();

            assertThat(buffer.isFinished()).isTrue();
            assertThat(blocked1.isDone()).isTrue();
            assertThat(blocked2.isDone()).isTrue();
        }
    }

    @Test
    public void testRemoteTaskFailedError()
    {
        // fail before noMoreTasks
        try (DirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            buffer.addTask(TASK_0);
            buffer.taskFailed(TASK_0, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));
            buffer.noMoreTasks();

            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isFalse();
            assertThat(buffer.pollPage()).isNull();
        }

        // fail after noMoreTasks
        try (DirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            buffer.addTask(TASK_0);
            buffer.noMoreTasks();
            buffer.taskFailed(TASK_0, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));

            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isFalse();
            assertThat(buffer.pollPage()).isNull();
        }
    }

    @Test
    public void testSingleWakeUp()
    {
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertThat(buffer.isFinished()).isFalse();
            ListenableFuture<Void> blocked1 = buffer.isBlocked();
            ListenableFuture<Void> blocked2 = buffer.isBlocked();
            assertThat(blocked1.isDone()).isFalse();
            assertThat(blocked2.isDone()).isFalse();

            buffer.addTask(TASK_0);

            buffer.addPages(TASK_0, ImmutableList.of(PAGE_0));
            buffer.pollPage();

            assertThat(blocked1.isDone()).isTrue();
            assertThat(blocked2.isDone()).isFalse();

            buffer.addPages(TASK_0, ImmutableList.of(PAGE_0));
            buffer.pollPage();

            assertThat(blocked2.isDone()).isTrue();
        }
    }
}
