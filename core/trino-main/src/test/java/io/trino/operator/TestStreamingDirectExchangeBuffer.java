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
import org.testng.annotations.Test;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.addTask(TASK_0);
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.addTask(TASK_1);
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertNull(buffer.pollPage());

            buffer.addPages(TASK_0, ImmutableList.of(PAGE_0));
            assertEquals(buffer.getBufferedPageCount(), 1);
            assertEquals(buffer.getRetainedSizeInBytes(), PAGE_0.getRetainedSize());
            assertEquals(buffer.getMaxRetainedSizeInBytes(), PAGE_0.getRetainedSize());
            assertEquals(buffer.getRemainingCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes() - PAGE_0.getRetainedSize());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
            assertEquals(buffer.pollPage(), PAGE_0);
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getMaxRetainedSizeInBytes(), PAGE_0.getRetainedSize());
            assertEquals(buffer.getRemainingCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes());
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.taskFinished(TASK_0);
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.addPages(TASK_1, ImmutableList.of(PAGE_1, PAGE_2));
            assertEquals(buffer.getBufferedPageCount(), 2);
            assertEquals(buffer.getRetainedSizeInBytes(), PAGE_1.getRetainedSize() + PAGE_2.getRetainedSize());
            assertEquals(buffer.getMaxRetainedSizeInBytes(), PAGE_1.getRetainedSize() + PAGE_2.getRetainedSize());
            assertEquals(buffer.getRemainingCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes() - PAGE_1.getRetainedSize() - PAGE_2.getRetainedSize());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
            assertEquals(buffer.pollPage(), PAGE_1);
            assertEquals(buffer.pollPage(), PAGE_2);
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getMaxRetainedSizeInBytes(), PAGE_1.getRetainedSize() + PAGE_2.getRetainedSize());
            assertEquals(buffer.getRemainingCapacityInBytes(), DataSize.of(1, KILOBYTE).toBytes());

            buffer.taskFinished(TASK_1);
            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
        }
    }

    @Test
    public void testClose()
    {
        StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE));
        buffer.addTask(TASK_0);
        buffer.addTask(TASK_1);

        assertFalse(buffer.isFinished());
        assertFalse(buffer.isBlocked().isDone());
        assertNull(buffer.pollPage());

        buffer.close();

        assertTrue(buffer.isFinished());
        assertTrue(buffer.isBlocked().isDone());
        assertNull(buffer.pollPage());
    }

    @Test
    public void testIsFinished()
    {
        // 0 tasks
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.noMoreTasks();

            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
        }

        // single task
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.addTask(TASK_0);
            buffer.noMoreTasks();

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.taskFinished(TASK_0);

            assertTrue(buffer.isFinished());
            assertTrue(buffer.isBlocked().isDone());
        }

        // single failed task
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            buffer.addTask(TASK_0);

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isBlocked().isDone());

            RuntimeException error = new RuntimeException();
            buffer.taskFailed(TASK_0, error);

            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
            assertTrue(buffer.isBlocked().isDone());
            assertThatThrownBy(buffer::pollPage).isEqualTo(error);
        }
    }

    @Test
    public void testFutureCancellationDoesNotAffectOtherFutures()
    {
        try (StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            assertFalse(buffer.isFinished());

            ListenableFuture<Void> blocked1 = buffer.isBlocked();
            ListenableFuture<Void> blocked2 = buffer.isBlocked();
            ListenableFuture<Void> blocked3 = buffer.isBlocked();

            assertFalse(blocked1.isDone());
            assertFalse(blocked2.isDone());
            assertFalse(blocked3.isDone());

            blocked3.cancel(true);
            assertFalse(blocked1.isDone());
            assertFalse(blocked2.isDone());

            buffer.noMoreTasks();

            assertTrue(buffer.isFinished());
            assertTrue(blocked1.isDone());
            assertTrue(blocked2.isDone());
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

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isFailed());
            assertNull(buffer.pollPage());
        }

        // fail after noMoreTasks
        try (DirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(directExecutor(), DataSize.of(1, KILOBYTE))) {
            buffer.addTask(TASK_0);
            buffer.noMoreTasks();
            buffer.taskFailed(TASK_0, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isFailed());
            assertNull(buffer.pollPage());
        }
    }
}
