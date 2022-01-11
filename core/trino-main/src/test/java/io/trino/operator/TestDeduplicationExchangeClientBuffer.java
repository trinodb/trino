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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDeduplicationExchangeClientBuffer
{
    private static final DataSize ONE_KB = DataSize.of(1, KILOBYTE);

    @Test
    public void testIsBlocked()
    {
        // immediate close
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);
            buffer.close();
            assertNotBlocked(blocked);
        }

        // empty set of tasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);
            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task finishes before noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.taskFinished(taskId);
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task finishes after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertBlocked(blocked);

            buffer.taskFinished(taskId);
            assertNotBlocked(blocked);
        }

        // single task fails before noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.taskFailed(taskId, new RuntimeException());
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task fails after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertBlocked(blocked);

            buffer.taskFailed(taskId, new RuntimeException());
            assertNotBlocked(blocked);
        }

        // cancelled blocked future doesn't affect other blocked futures
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked1 = buffer.isBlocked();
            ListenableFuture<Void> blocked2 = buffer.isBlocked();
            assertBlocked(blocked1);
            assertBlocked(blocked2);

            blocked2.cancel(true);

            assertBlocked(blocked1);
            assertNotBlocked(blocked2);
        }
    }

    @Test
    public void testPollPage()
    {
        testPollPages(ImmutableListMultimap.of(), ImmutableMap.of(), ImmutableList.of());
        testPollPages(
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), utf8Slice("p0a0v0"))
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of("p0a0v0"));
        testPollPages(
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), utf8Slice("p0a0v0"))
                        .put(createTaskId(0, 1), utf8Slice("p0a1v0"))
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of("p0a1v0"));
        testPollPages(
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), utf8Slice("p0a0v0"))
                        .put(createTaskId(1, 0), utf8Slice("p1a0v0"))
                        .put(createTaskId(0, 1), utf8Slice("p0a1v0"))
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of("p0a1v0"));
        testPollPages(
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), utf8Slice("p0a0v0"))
                        .put(createTaskId(1, 0), utf8Slice("p1a0v0"))
                        .put(createTaskId(0, 1), utf8Slice("p0a1v0"))
                        .build(),
                ImmutableMap.of(
                        createTaskId(2, 0),
                        new RuntimeException("error")),
                ImmutableList.of("p0a1v0"));
        RuntimeException error = new RuntimeException("error");
        testPollPagesFailure(
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), utf8Slice("p0a0v0"))
                        .put(createTaskId(1, 0), utf8Slice("p1a0v0"))
                        .put(createTaskId(0, 1), utf8Slice("p0a1v0"))
                        .build(),
                ImmutableMap.of(
                        createTaskId(2, 2),
                        error),
                error);
        testPollPagesFailure(
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), utf8Slice("p0a0v0"))
                        .put(createTaskId(1, 0), utf8Slice("p1a0v0"))
                        .put(createTaskId(0, 1), utf8Slice("p0a1v0"))
                        .build(),
                ImmutableMap.of(
                        createTaskId(0, 1),
                        error),
                error);
    }

    private static void testPollPages(Multimap<TaskId, Slice> pages, Map<TaskId, RuntimeException> failures, List<String> expectedValues)
    {
        List<Slice> actualPages = pollPages(pages, failures);
        List<String> actualValues = actualPages.stream()
                .map(Slice::toStringUtf8)
                .collect(toImmutableList());
        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private static void testPollPagesFailure(Multimap<TaskId, Slice> pages, Map<TaskId, RuntimeException> failures, Throwable expectedFailure)
    {
        assertThatThrownBy(() -> pollPages(pages, failures)).isEqualTo(expectedFailure);
    }

    private static List<Slice> pollPages(Multimap<TaskId, Slice> pages, Map<TaskId, RuntimeException> failures)
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            for (TaskId taskId : Sets.union(pages.keySet(), failures.keySet())) {
                buffer.addTask(taskId);
            }
            for (Map.Entry<TaskId, Slice> page : pages.entries()) {
                buffer.addPages(page.getKey(), ImmutableList.of(page.getValue()));
            }
            for (Map.Entry<TaskId, RuntimeException> failure : failures.entrySet()) {
                buffer.taskFailed(failure.getKey(), failure.getValue());
            }
            for (TaskId taskId : Sets.difference(pages.keySet(), failures.keySet())) {
                buffer.taskFinished(taskId);
            }
            buffer.noMoreTasks();

            ImmutableList.Builder<Slice> result = ImmutableList.builder();
            while (true) {
                Slice page = buffer.pollPage();
                if (page == null) {
                    break;
                }
                result.add(page);
            }
            assertTrue(buffer.isFinished());
            return result.build();
        }
    }

    @Test
    public void testRemovePagesForPreviousAttempts()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertEquals(buffer.getRetainedSizeInBytes(), 0);

            TaskId partition0Attempt0 = createTaskId(0, 0);
            TaskId partition1Attempt0 = createTaskId(1, 0);
            TaskId partition0Attempt1 = createTaskId(0, 1);

            Slice page1 = utf8Slice("textofrandomlength");
            Slice page2 = utf8Slice("textwithdifferentlength");
            Slice page3 = utf8Slice("smalltext");

            buffer.addTask(partition0Attempt0);
            buffer.addPages(partition0Attempt0, ImmutableList.of(page1));
            buffer.addTask(partition1Attempt0);
            buffer.addPages(partition1Attempt0, ImmutableList.of(page2));

            assertThat(buffer.getRetainedSizeInBytes()).isGreaterThan(0);
            assertEquals(buffer.getRetainedSizeInBytes(), page1.getRetainedSize() + page2.getRetainedSize());

            buffer.addTask(partition0Attempt1);
            assertEquals(buffer.getRetainedSizeInBytes(), 0);

            buffer.addPages(partition0Attempt1, ImmutableList.of(page3));
            assertEquals(buffer.getRetainedSizeInBytes(), page3.getRetainedSize());
        }
    }

    @Test
    public void testBufferOverflow()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), DataSize.of(100, BYTE), RetryPolicy.QUERY)) {
            TaskId task = createTaskId(0, 0);

            Slice page1 = utf8Slice("1234");
            Slice page2 = utf8Slice("123456789");
            assertThat(page1.getRetainedSize()).isLessThanOrEqualTo(100);
            assertThat(page1.getRetainedSize() + page2.getRetainedSize()).isGreaterThan(100);

            buffer.addTask(task);
            buffer.addPages(task, ImmutableList.of(page1));

            assertFalse(buffer.isFinished());
            assertBlocked(buffer.isBlocked());
            assertEquals(buffer.getRetainedSizeInBytes(), page1.getRetainedSize());

            buffer.addPages(task, ImmutableList.of(page2));
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
            assertNotBlocked(buffer.isBlocked());
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getBufferedPageCount(), 0);

            assertThatThrownBy(buffer::pollPage)
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Retries for queries with large result set currently unsupported");
        }
    }

    @Test
    public void testIsFinished()
    {
        // close right away
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());
            buffer.close();
            assertTrue(buffer.isFinished());
        }

        // 0 tasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());
            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        // single task producing no results, finish before noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        // single task producing no results, finish after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertTrue(buffer.isFinished());
        }

        // single task producing no results, fail before noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
        }

        // single task producing no results, fail after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
        }

        // single task producing one page, fail after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(utf8Slice("page")));
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
        }

        // single task producing one page, finish after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(utf8Slice("page")));
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            assertNotNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }

        // single task producing one page, finish before noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(utf8Slice("page")));
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            assertNotNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }
    }

    @Test
    public void testRemainingBufferCapacity()
    {
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            Slice page = utf8Slice("page");
            buffer.addPages(taskId, ImmutableList.of(page));

            assertEquals(buffer.getRemainingCapacityInBytes(), ONE_KB.toBytes() - page.getRetainedSize());
        }
    }

    @Test
    public void testRemoteTaskFailedError()
    {
        // fail before noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.taskFailed(taskId, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));
            buffer.noMoreTasks();

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isFailed());
            assertBlocked(buffer.isBlocked());
            assertNull(buffer.pollPage());
        }

        // fail after noMoreTasks
        try (ExchangeClientBuffer buffer = new DeduplicationExchangeClientBuffer(directExecutor(), ONE_KB, RetryPolicy.QUERY)) {
            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.noMoreTasks();
            buffer.taskFailed(taskId, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isFailed());
            assertBlocked(buffer.isBlocked());
            assertNull(buffer.pollPage());
        }
    }

    private static TaskId createTaskId(int partition, int attempt)
    {
        return new TaskId(new StageId("query", 0), partition, attempt);
    }

    private static void assertNotBlocked(ListenableFuture<Void> blocked)
    {
        assertTrue(blocked.isDone());
    }

    private static void assertBlocked(ListenableFuture<Void> blocked)
    {
        assertFalse(blocked.isDone());
    }
}
