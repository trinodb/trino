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
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDeduplicatingDirectExchangeBuffer
{
    private static final DataSize DEFAULT_BUFFER_CAPACITY = DataSize.of(1, KILOBYTE);

    private ExchangeManagerRegistry exchangeManagerRegistry;

    @BeforeAll
    public void beforeClass()
    {
        exchangeManagerRegistry = new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of()));
        exchangeManagerRegistry.addExchangeManagerFactory(new FileSystemExchangeManagerFactory());
        exchangeManagerRegistry.loadExchangeManager("filesystem", ImmutableMap.of(
                "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
    }

    @AfterAll
    public void afterClass()
    {
        exchangeManagerRegistry = null;
    }

    @Test
    public void testIsBlocked()
    {
        // immediate close
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);
            buffer.close();
            assertNotBlocked(blocked);
        }

        // empty set of tasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            ListenableFuture<Void> blocked = buffer.isBlocked();
            assertBlocked(blocked);
            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task finishes before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
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
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
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
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
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
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
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
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
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
    public void testPollPagesQueryLevelRetry()
    {
        // 0 pages
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                DEFAULT_BUFFER_CAPACITY,
                0,
                ImmutableList.of());

        // single page, no spilling
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.of(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(10, BYTE))),
                ImmutableMap.of(),
                DataSize.of(1, KILOBYTE),
                0,
                ImmutableList.of(createPage("p0a0v0", DataSize.of(10, BYTE))));

        // single page, with spilling
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.of(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(2, KILOBYTE))),
                ImmutableMap.of(),
                DataSize.of(1, KILOBYTE),
                1,
                ImmutableList.of(createPage("p0a0v0", DataSize.of(2, KILOBYTE))));

        // multiple pages, no spilling
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(0, 0), new RuntimeException("error")),
                DataSize.of(5, KILOBYTE),
                0,
                ImmutableList.of(
                        createPage("p0a1v0", DataSize.of(1, KILOBYTE))));

        // two pages, one spilled
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(6, KILOBYTE)))
                        .put(createTaskId(0, 0), createPage("p0a0v1", DataSize.of(3, KILOBYTE)))
                        .build(),
                ImmutableMap.of(),
                DataSize.of(5, KILOBYTE),
                2,
                ImmutableList.of(
                        createPage("p0a0v0", DataSize.of(6, KILOBYTE)),
                        createPage("p0a0v1", DataSize.of(3, KILOBYTE))));

        // discard single spilled page
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(6, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(3, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(0, 0), new RuntimeException("error")),
                DataSize.of(5, KILOBYTE),
                2,
                ImmutableList.of(
                        createPage("p0a1v0", DataSize.of(3, KILOBYTE))));

        // discard one of the spilled page
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(2, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(4, KILOBYTE)))
                        .put(createTaskId(1, 1), createPage("p1a1v0", DataSize.of(4, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(0, 0), new RuntimeException("error")),
                DataSize.of(5, KILOBYTE),
                2,
                ImmutableList.of(
                        createPage("p0a1v0", DataSize.of(4, KILOBYTE)),
                        createPage("p1a1v0", DataSize.of(4, KILOBYTE))));

        // discard pages from successful tasks, no spilling
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(2, 0), new RuntimeException("error")),
                DataSize.of(5, KILOBYTE),
                0,
                ImmutableList.of(
                        createPage("p0a1v0", DataSize.of(1, KILOBYTE))));

        // discard pages from successful tasks, with spilling
        testPollPages(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(2, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(3, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(2, 0), new RuntimeException("error")),
                DataSize.of(4, KILOBYTE),
                3,
                ImmutableList.of(
                        createPage("p0a1v0", DataSize.of(1, KILOBYTE))));

        RuntimeException error = new RuntimeException("error");

        // failure in a task that produced no pages, no spilling
        testPollPagesFailure(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(2, 2), error),
                DataSize.of(4, KILOBYTE),
                0,
                error);

        // failure in a task that produced some pages, no spilling
        testPollPagesFailure(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(0, 1), error),
                DataSize.of(4, KILOBYTE),
                0,
                error);

        // failure in a task that produced no pages, with spilling
        testPollPagesFailure(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(3, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(2, 2), error),
                DataSize.of(3, KILOBYTE),
                3,
                error);

        // failure in a task that produced some pages, with spilling
        testPollPagesFailure(
                RetryPolicy.QUERY,
                ImmutableListMultimap.<TaskId, Slice>builder()
                        .put(createTaskId(0, 0), createPage("p0a0v0", DataSize.of(1, KILOBYTE)))
                        .put(createTaskId(1, 0), createPage("p1a0v0", DataSize.of(3, KILOBYTE)))
                        .put(createTaskId(0, 1), createPage("p0a1v0", DataSize.of(1, KILOBYTE)))
                        .build(),
                ImmutableMap.of(createTaskId(0, 1), error),
                DataSize.of(3, KILOBYTE),
                3,
                error);
    }

    private void testPollPages(
            RetryPolicy retryPolicy,
            Multimap<TaskId, Slice> pages,
            Map<TaskId, RuntimeException> failures,
            DataSize bufferCapacity,
            int expectedSpilledPageCount,
            List<Slice> expectedOutput)
    {
        List<Slice> actualOutput = pollPages(retryPolicy, pages, failures, bufferCapacity, expectedSpilledPageCount);
        assertThat(actualOutput).isEqualTo(expectedOutput);
    }

    private void testPollPagesFailure(
            RetryPolicy retryPolicy,
            Multimap<TaskId, Slice> pages,
            Map<TaskId, RuntimeException> failures,
            DataSize bufferCapacity,
            int expectedSpilledPageCount,
            Throwable expectedFailure)
    {
        assertThatThrownBy(() -> pollPages(retryPolicy, pages, failures, bufferCapacity, expectedSpilledPageCount)).isEqualTo(expectedFailure);
    }

    private List<Slice> pollPages(
            RetryPolicy retryPolicy,
            Multimap<TaskId, Slice> pages,
            Map<TaskId, RuntimeException> failures,
            DataSize bufferCapacity,
            int expectedSpilledPageCount)
    {
        Set<TaskId> addedTasks = new HashSet<>();
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(bufferCapacity, retryPolicy)) {
            for (Map.Entry<TaskId, Slice> page : pages.entries()) {
                if (addedTasks.add(page.getKey())) {
                    buffer.addTask(page.getKey());
                }
                buffer.addPages(page.getKey(), ImmutableList.of(page.getValue()));
            }
            for (Map.Entry<TaskId, RuntimeException> failure : failures.entrySet()) {
                if (addedTasks.add(failure.getKey())) {
                    buffer.addTask(failure.getKey());
                }
                buffer.taskFailed(failure.getKey(), failure.getValue());
            }
            for (TaskId taskId : Sets.difference(pages.keySet(), failures.keySet())) {
                buffer.taskFinished(taskId);
            }
            buffer.noMoreTasks();

            ImmutableList.Builder<Slice> result = ImmutableList.builder();
            while (!buffer.isFinished()) {
                // wait for blocked
                getUnchecked(buffer.isBlocked());
                Slice page = buffer.pollPage();
                if (page == null) {
                    continue;
                }
                result.add(page);
            }
            assertThat(buffer.isFinished()).isTrue();
            assertThat(buffer.getSpilledPageCount()).isEqualTo(expectedSpilledPageCount);
            return result.build();
        }
    }

    @Test
    public void testRemovePagesForPreviousAttempts()
    {
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DataSize.of(1, KILOBYTE), RetryPolicy.QUERY)) {
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(0);

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
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(page1.getRetainedSize() + page2.getRetainedSize());

            buffer.addTask(partition0Attempt1);
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(0);

            buffer.addPages(partition0Attempt1, ImmutableList.of(page3));
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(page3.getRetainedSize());
        }
    }

    @Test
    public void testExchangeManagerNotConfigured()
    {
        // no overflow
        try (DirectExchangeBuffer buffer = new DeduplicatingDirectExchangeBuffer(
                directExecutor(),
                DataSize.of(100, BYTE),
                RetryPolicy.QUERY,
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of())),
                new QueryId("query"),
                Span.getInvalid(),
                createRandomExchangeId())) {
            TaskId task = createTaskId(0, 0);
            Slice page = createPage("1234", DataSize.of(10, BYTE));

            buffer.addTask(task);
            buffer.addPages(task, ImmutableList.of(page));
            buffer.taskFinished(task);
            buffer.noMoreTasks();

            assertThat(buffer.isFinished()).isFalse();
            assertNotBlocked(buffer.isBlocked());
            assertThat(buffer.pollPage()).isEqualTo(page);
            assertThat(buffer.pollPage()).isNull();
            assertThat(buffer.isFinished()).isTrue();
        }

        // overflow
        try (DirectExchangeBuffer buffer = new DeduplicatingDirectExchangeBuffer(
                directExecutor(),
                DataSize.of(100, BYTE),
                RetryPolicy.QUERY,
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of())),
                new QueryId("query"),
                Span.getInvalid(),
                createRandomExchangeId())) {
            TaskId task = createTaskId(0, 0);

            Slice page1 = utf8Slice("1234");
            Slice page2 = utf8Slice("123456789");
            assertThat(page1.getRetainedSize()).isLessThanOrEqualTo(100);
            assertThat(page1.getRetainedSize() + page2.getRetainedSize()).isGreaterThan(100);

            buffer.addTask(task);
            buffer.addPages(task, ImmutableList.of(page1));

            assertThat(buffer.isFinished()).isFalse();
            assertBlocked(buffer.isBlocked());
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(page1.getRetainedSize());

            buffer.addPages(task, ImmutableList.of(page2));
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isTrue();
            assertNotBlocked(buffer.isBlocked());
            assertThat(buffer.getRetainedSizeInBytes()).isEqualTo(0);
            assertThat(buffer.getBufferedPageCount()).isEqualTo(0);

            assertThatThrownBy(buffer::pollPage)
                    .isInstanceOf(TrinoException.class);
        }
    }

    @Test
    public void testIsFinished()
    {
        // close right away
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();
            buffer.close();
            assertThat(buffer.isFinished()).isTrue();
        }

        // 0 tasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();
            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isTrue();
        }

        // single task producing no results, finish before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFinished(taskId);
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isTrue();
        }

        // single task producing no results, finish after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFinished(taskId);
            assertThat(buffer.isFinished()).isTrue();
        }

        // single task producing no results, fail before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFailed(taskId, new RuntimeException());
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isTrue();
        }

        // single task producing no results, fail after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFailed(taskId, new RuntimeException());
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isTrue();
        }

        // single task producing one page, fail after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(utf8Slice("page")));
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFailed(taskId, new RuntimeException());
            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isTrue();
        }

        // single task producing one page, finish after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(utf8Slice("page")));
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFinished(taskId);
            assertThat(buffer.isFinished()).isFalse();

            assertThat(buffer.pollPage()).isNotNull();
            assertThat(buffer.isFinished()).isTrue();
        }

        // single task producing one page, finish before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(utf8Slice("page")));
            assertThat(buffer.isFinished()).isFalse();

            buffer.taskFinished(taskId);
            assertThat(buffer.isFinished()).isFalse();

            buffer.noMoreTasks();
            assertThat(buffer.isFinished()).isFalse();

            assertThat(buffer.pollPage()).isNotNull();
            assertThat(buffer.isFinished()).isTrue();
        }
    }

    @Test
    public void testRemainingBufferCapacity()
    {
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.QUERY)) {
            assertThat(buffer.isFinished()).isFalse();

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            Slice page = utf8Slice("page");
            buffer.addPages(taskId, ImmutableList.of(page));

            assertThat(buffer.getRemainingCapacityInBytes()).isEqualTo(Long.MAX_VALUE);
        }
    }

    @Test
    public void testRemoteTaskFailedError()
    {
        testRemoteTaskFailedError(RetryPolicy.QUERY);
    }

    private void testRemoteTaskFailedError(RetryPolicy retryPolicy)
    {
        // fail before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, retryPolicy)) {
            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.taskFailed(taskId, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));
            buffer.noMoreTasks();

            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isFalse();
            assertBlocked(buffer.isBlocked());
            assertThat(buffer.pollPage()).isNull();
        }

        // fail after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, retryPolicy)) {
            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.noMoreTasks();
            buffer.taskFailed(taskId, new TrinoException(REMOTE_TASK_FAILED, "Remote task failed"));

            assertThat(buffer.isFinished()).isFalse();
            assertThat(buffer.isFailed()).isFalse();
            assertBlocked(buffer.isBlocked());
            assertThat(buffer.pollPage()).isNull();
        }
    }

    private DeduplicatingDirectExchangeBuffer createDeduplicatingDirectExchangeBuffer(DataSize bufferCapacity, RetryPolicy retryPolicy)
    {
        return new DeduplicatingDirectExchangeBuffer(
                directExecutor(),
                bufferCapacity,
                retryPolicy,
                exchangeManagerRegistry,
                new QueryId("query"),
                Span.getInvalid(),
                createRandomExchangeId());
    }

    private static TaskId createTaskId(int partition, int attempt)
    {
        return new TaskId(new StageId("query", 0), partition, attempt);
    }

    private static Slice createPage(String value, DataSize size)
    {
        Slice encodedValue = utf8Slice(value);
        int sizeInBytes = toIntExact(size.toBytes());
        checkArgument(encodedValue.length() <= sizeInBytes, "encoded value %s is larger than the total requested size of a page %s", value, size);
        Slice result = Slices.allocate(sizeInBytes);
        result.setBytes(0, encodedValue);
        return result;
    }

    private static void assertNotBlocked(ListenableFuture<Void> blocked)
    {
        assertThat(blocked.isDone()).isTrue();
    }

    private static void assertBlocked(ListenableFuture<Void> blocked)
    {
        assertThat(blocked.isDone()).isFalse();
    }
}
