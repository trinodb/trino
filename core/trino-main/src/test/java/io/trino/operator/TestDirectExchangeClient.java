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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.block.BlockAssertions;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PagesSerde;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.metadata.HandleResolver;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoTransportException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.trino.execution.buffer.PagesSerde.getSerializedPagePositionCount;
import static io.trino.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDirectExchangeClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }
    }

    @Test
    public void testHappyPath()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        List<Slice> pages = ImmutableList.of(createSerializedPage(1), createSerializedPage(2), createSerializedPage(3));

        URI location = URI.create("http://localhost:8080");
        pages.forEach(page -> processor.addPage(location, page));
        processor.setComplete(location);

        TestingDirectExchangeBuffer buffer = new TestingDirectExchangeBuffer(DataSize.of(1, Unit.MEGABYTE));

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                buffer,
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        assertThat(buffer.getAllTasks()).isEmpty();
        assertThat(buffer.getPages().asMap()).isEmpty();
        assertThat(buffer.getFinishedTasks()).isEmpty();
        assertThat(buffer.getFailedTasks().asMap()).isEmpty();
        assertFalse(buffer.isNoMoreTasks());

        TaskId taskId = new TaskId(new StageId("query", 1), 0, 0);
        exchangeClient.addLocation(taskId, location);
        assertThat(buffer.getAllTasks()).containsExactly(taskId);
        exchangeClient.noMoreLocations();
        assertTrue(buffer.isNoMoreTasks());

        buffer.whenTaskFinished(taskId).get(10, SECONDS);
        assertThat(buffer.getFinishedTasks()).containsExactly(taskId);
        assertThat(buffer.getPages().get(taskId)).hasSize(3);
        assertThat(buffer.getFailedTasks().asMap()).isEmpty();

        assertFalse(exchangeClient.isFinished());
        buffer.setFinished(true);
        assertTrue(exchangeClient.isFinished());

        DirectExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);

        // client should have sent only 3 requests: one to get all pages, one to acknowledge and one to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");

        exchangeClient.close();

        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(0).getHttpRequestState(), "not scheduled", "httpRequestState"));

        assertThat(buffer.getFinishedTasks()).containsExactly(taskId);
        assertThat(buffer.getFailedTasks().asMap()).isEmpty();
        assertThat(buffer.getPages().size()).isEqualTo(3);
    }

    @Test
    public void testStreamingHappyPath()
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                new StreamingDirectExchangeBuffer(scheduler, DataSize.of(32, Unit.MEGABYTE)),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        exchangeClient.addLocation(new TaskId(new StageId("query", 1), 0, 0), location);
        exchangeClient.noMoreLocations();

        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(3));
        assertNull(getNextPage(exchangeClient));
        assertTrue(exchangeClient.isFinished());

        DirectExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);

        // client should have sent only 3 requests: one to get all pages, one to acknowledge and one to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");

        exchangeClient.close();
    }

    @Test
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        TaskId task1 = new TaskId(new StageId("query", 1), 0, 0);
        TaskId task2 = new TaskId(new StageId("query", 1), 1, 0);
        TaskId task3 = new TaskId(new StageId("query", 1), 2, 0);

        URI location1 = URI.create("http://localhost:8080/1");
        URI location2 = URI.create("http://localhost:8080/2");
        URI location3 = URI.create("http://localhost:8080/3");

        processor.addPage(location1, createSerializedPage(1));
        processor.addPage(location1, createSerializedPage(2));

        TestingDirectExchangeBuffer buffer = new TestingDirectExchangeBuffer(DataSize.of(1, Unit.MEGABYTE));

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                buffer,
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        assertThat(buffer.getAllTasks()).isEmpty();
        assertThat(buffer.getPages().asMap()).isEmpty();
        assertThat(buffer.getFinishedTasks()).isEmpty();
        assertThat(buffer.getFailedTasks().asMap()).isEmpty();
        assertFalse(buffer.isNoMoreTasks());

        exchangeClient.addLocation(task1, location1);
        assertThat(buffer.getAllTasks()).containsExactly(task1);
        assertTaskIsNotFinished(buffer, task1);

        processor.setComplete(location1);
        buffer.whenTaskFinished(task1).get(10, SECONDS);
        assertThat(buffer.getPages().get(task1)).hasSize(2);
        assertThat(buffer.getFinishedTasks()).containsExactly(task1);

        exchangeClient.addLocation(task2, location2);
        assertThat(buffer.getAllTasks()).containsExactlyInAnyOrder(task1, task2);
        assertTaskIsNotFinished(buffer, task2);

        processor.setComplete(location2);
        buffer.whenTaskFinished(task2).get(10, SECONDS);
        assertThat(buffer.getFinishedTasks()).containsExactlyInAnyOrder(task1, task2);
        assertThat(buffer.getPages().get(task2)).hasSize(0);

        exchangeClient.addLocation(task3, location3);
        assertThat(buffer.getAllTasks()).containsExactlyInAnyOrder(task1, task2, task3);
        assertTaskIsNotFinished(buffer, task3);

        exchangeClient.noMoreLocations();
        assertTrue(buffer.isNoMoreTasks());

        assertThat(buffer.getAllTasks()).containsExactlyInAnyOrder(task1, task2, task3);
        assertTaskIsNotFinished(buffer, task3);

        exchangeClient.close();
        buffer.whenTaskFinished(task3).get(10, SECONDS);
        assertThat(buffer.getFinishedTasks()).containsExactlyInAnyOrder(task1, task2, task3);
        assertThat(buffer.getFailedTasks().asMap()).isEmpty();

        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(0).getHttpRequestState(), "not scheduled", "httpRequestState"));
        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(1).getHttpRequestState(), "not scheduled", "httpRequestState"));
        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(2).getHttpRequestState(), "not scheduled", "httpRequestState"));

        assertTrue(exchangeClient.isFinished());
    }

    @Test(timeOut = 10000)
    public void testStreamingAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                new StreamingDirectExchangeBuffer(scheduler, DataSize.of(32, Unit.MEGABYTE)),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-testAddLocation-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        URI location1 = URI.create("http://localhost:8081/foo");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(new TaskId(new StageId("query", 1), 0, 0), location1);

        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        assertNull(exchangeClient.pollPage());
        ListenableFuture<Void> firstBlocked = exchangeClient.isBlocked();
        assertFalse(tryGetFutureValue(firstBlocked, 10, MILLISECONDS).isPresent());
        assertFalse(firstBlocked.isDone());

        assertNull(exchangeClient.pollPage());
        ListenableFuture<Void> secondBlocked = exchangeClient.isBlocked();
        assertFalse(tryGetFutureValue(secondBlocked, 10, MILLISECONDS).isPresent());
        assertFalse(secondBlocked.isDone());

        assertNull(exchangeClient.pollPage());
        ListenableFuture<Void> thirdBlocked = exchangeClient.isBlocked();
        assertFalse(tryGetFutureValue(thirdBlocked, 10, MILLISECONDS).isPresent());
        assertFalse(thirdBlocked.isDone());

        thirdBlocked.cancel(true);
        assertTrue(thirdBlocked.isDone());
        assertFalse(tryGetFutureValue(firstBlocked, 10, MILLISECONDS).isPresent());
        assertFalse(firstBlocked.isDone());
        assertFalse(tryGetFutureValue(secondBlocked, 10, MILLISECONDS).isPresent());
        assertFalse(secondBlocked.isDone());

        assertFalse(exchangeClient.isFinished());

        URI location2 = URI.create("http://localhost:8082/bar");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(new TaskId(new StageId("query", 1), 1, 0), location2);

        tryGetFutureValue(firstBlocked, 5, SECONDS);
        assertTrue(firstBlocked.isDone());
        tryGetFutureValue(secondBlocked, 5, SECONDS);
        assertTrue(secondBlocked.isDone());

        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(4));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(5));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertFalse(exchangeClient.isFinished());

        exchangeClient.noMoreLocations();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!exchangeClient.isFinished()) {
            Thread.sleep(1);
        }
        exchangeClient.close();

        ImmutableMap<URI, PageBufferClientStatus> statuses = uniqueIndex(exchangeClient.getStatus().getPageBufferClientStatuses(), PageBufferClientStatus::getUri);
        assertStatus(statuses.get(location1), location1, "closed", 3, 3, 3, "not scheduled");
        assertStatus(statuses.get(location2), location2, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testStreamingTaskFailure()
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        TaskId task1 = new TaskId(new StageId("query", 1), 0, 0);
        TaskId task2 = new TaskId(new StageId("query", 1), 1, 0);

        URI location1 = URI.create("http://localhost:8080/1");
        URI location2 = URI.create("http://localhost:8080/2");

        processor.addPage(location1, createPage(1));

        StreamingDirectExchangeBuffer buffer = new StreamingDirectExchangeBuffer(scheduler, DataSize.of(1, Unit.MEGABYTE));

        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                buffer,
                maxResponseSize,
                1,
                new Duration(1, SECONDS),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        exchangeClient.addLocation(task1, location1);
        exchangeClient.addLocation(task2, location2);

        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        processor.setComplete(location1);

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());

        RuntimeException randomException = new RuntimeException("randomfailure");
        processor.setFailed(location2, randomException);

        assertThatThrownBy(() -> getNextPage(exchangeClient)).hasMessageContaining("Encountered too many errors talking to a worker node");

        assertFalse(exchangeClient.isFinished());
    }

    @Test
    public void testDeduplicationTaskFailure()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        TaskId attempt0Task1 = new TaskId(new StageId("query", 1), 0, 0);
        TaskId attempt1Task1 = new TaskId(new StageId("query", 1), 1, 0);
        TaskId attempt1Task2 = new TaskId(new StageId("query", 1), 2, 0);

        URI attempt0Task1Location = URI.create("http://localhost:8080/1/0");
        URI attempt1Task1Location = URI.create("http://localhost:8080/1/1");
        URI attempt1Task2Location = URI.create("http://localhost:8080/2/1");

        processor.setFailed(attempt0Task1Location, new RuntimeException("randomfailure"));
        processor.addPage(attempt1Task1Location, createPage(1));
        processor.setComplete(attempt1Task1Location);
        processor.setFailed(attempt1Task2Location, new RuntimeException("randomfailure"));

        DeduplicatingDirectExchangeBuffer buffer = new DeduplicatingDirectExchangeBuffer(
                scheduler,
                DataSize.of(1, Unit.MEGABYTE),
                RetryPolicy.QUERY,
                new ExchangeManagerRegistry(new HandleResolver()),
                new QueryId("query"),
                createRandomExchangeId());

        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                buffer,
                maxResponseSize,
                1,
                new Duration(1, SECONDS),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        exchangeClient.addLocation(attempt0Task1, attempt0Task1Location);
        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());

        exchangeClient.addLocation(attempt1Task1, attempt1Task1Location);
        exchangeClient.addLocation(attempt1Task2, attempt1Task2Location);
        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());

        exchangeClient.noMoreLocations();
        exchangeClient.isBlocked().get(10, SECONDS);

        assertThatThrownBy(() -> getNextPage(exchangeClient)).hasMessageContaining("Encountered too many errors talking to a worker node");

        assertFalse(exchangeClient.isFinished());
    }

    @Test
    public void testDeduplication()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        TaskId taskP0A0 = new TaskId(new StageId("query", 1), 0, 0);
        TaskId taskP1A0 = new TaskId(new StageId("query", 1), 1, 0);
        TaskId taskP0A1 = new TaskId(new StageId("query", 1), 0, 1);

        URI locationP0A0 = URI.create("http://localhost:8080/1");
        URI locationP1A0 = URI.create("http://localhost:8080/2");
        URI locationP0A1 = URI.create("http://localhost:8080/3");

        processor.addPage(locationP1A0, createSerializedPage(1));
        processor.addPage(locationP0A1, createSerializedPage(2));
        processor.addPage(locationP0A1, createSerializedPage(3));

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                new DeduplicatingDirectExchangeBuffer(
                        scheduler,
                        DataSize.of(1, Unit.KILOBYTE),
                        RetryPolicy.QUERY,
                        new ExchangeManagerRegistry(new HandleResolver()),
                        new QueryId("query"),
                        createRandomExchangeId()),
                maxResponseSize,
                1,
                new Duration(1, SECONDS),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        exchangeClient.addLocation(taskP0A0, locationP0A0);
        exchangeClient.addLocation(taskP1A0, locationP1A0);
        exchangeClient.addLocation(taskP0A1, locationP0A1);

        processor.setComplete(locationP0A0);
        // Failing attempt 0. Results from all tasks for attempt 0 must be discarded.
        processor.setFailed(locationP1A0, new RuntimeException("failure"));
        processor.setComplete(locationP0A1);

        assertFalse(exchangeClient.isFinished());
        assertThatThrownBy(() -> exchangeClient.isBlocked().get(50, MILLISECONDS))
                .isInstanceOf(TimeoutException.class);

        exchangeClient.noMoreLocations();
        exchangeClient.isBlocked().get(10, SECONDS);

        List<Page> pages = new ArrayList<>();
        while (!exchangeClient.isFinished()) {
            Slice page = exchangeClient.pollPage();
            if (page == null) {
                break;
            }
            pages.add(PAGES_SERDE.deserialize(page));
        }

        assertThat(pages).hasSize(2);
        assertThat(pages.stream().map(Page::getPositionCount).collect(toImmutableSet())).containsAll(ImmutableList.of(2, 3));
        assertEventually(() -> assertTrue(exchangeClient.isFinished()));

        assertEventually(() -> {
            assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(0).getHttpRequestState(), "not scheduled", "httpRequestState");
            assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(1).getHttpRequestState(), "not scheduled", "httpRequestState");
            assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(2).getHttpRequestState(), "not scheduled", "httpRequestState");
        });

        exchangeClient.close();
    }

    @Test
    public void testTaskFailure()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        TaskId task1 = new TaskId(new StageId("query", 1), 0, 0);
        TaskId task2 = new TaskId(new StageId("query", 1), 1, 0);
        TaskId task3 = new TaskId(new StageId("query", 1), 2, 0);
        TaskId task4 = new TaskId(new StageId("query", 1), 3, 0);

        URI location1 = URI.create("http://localhost:8080/1");
        URI location2 = URI.create("http://localhost:8080/2");
        URI location3 = URI.create("http://localhost:8080/3");
        URI location4 = URI.create("http://localhost:8080/4");

        processor.addPage(location1, createSerializedPage(1));
        processor.addPage(location4, createSerializedPage(2));
        processor.addPage(location4, createSerializedPage(3));

        TestingDirectExchangeBuffer buffer = new TestingDirectExchangeBuffer(DataSize.of(1, Unit.MEGABYTE));

        Set<TaskId> failedTasks = newConcurrentHashSet();
        CountDownLatch latch = new CountDownLatch(2);

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                buffer,
                maxResponseSize,
                1,
                new Duration(1, SECONDS),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {
                    failedTasks.add(taskId);
                    latch.countDown();
                });

        assertThat(buffer.getAllTasks()).isEmpty();
        assertThat(buffer.getPages().asMap()).isEmpty();
        assertThat(buffer.getFinishedTasks()).isEmpty();
        assertThat(buffer.getFailedTasks().asMap()).isEmpty();
        assertFalse(buffer.isNoMoreTasks());

        exchangeClient.addLocation(task1, location1);
        assertThat(buffer.getAllTasks()).containsExactly(task1);
        assertTaskIsNotFinished(buffer, task1);

        processor.setComplete(location1);
        buffer.whenTaskFinished(task1).get(10, SECONDS);
        assertThat(buffer.getPages().get(task1)).hasSize(1);
        assertThat(buffer.getFinishedTasks()).containsExactly(task1);

        exchangeClient.addLocation(task2, location2);
        assertThat(buffer.getAllTasks()).containsExactlyInAnyOrder(task1, task2);
        assertTaskIsNotFinished(buffer, task2);

        RuntimeException randomException = new RuntimeException("randomfailure");
        processor.setFailed(location2, randomException);
        buffer.whenTaskFailed(task2).get(10, SECONDS);

        assertThat(buffer.getFinishedTasks()).containsExactly(task1);
        assertThat(buffer.getFailedTasks().keySet()).containsExactly(task2);
        assertThat(buffer.getPages().get(task2)).hasSize(0);

        exchangeClient.addLocation(task3, location3);
        assertThat(buffer.getAllTasks()).containsExactlyInAnyOrder(task1, task2, task3);
        assertTaskIsNotFinished(buffer, task2);
        assertTaskIsNotFinished(buffer, task3);

        TrinoException trinoException = new TrinoException(GENERIC_INTERNAL_ERROR, "generic internal error");
        processor.setFailed(location3, trinoException);
        buffer.whenTaskFailed(task3).get(10, SECONDS);

        assertThat(buffer.getFinishedTasks()).containsExactly(task1);
        assertThat(buffer.getFailedTasks().keySet()).containsExactlyInAnyOrder(task2, task3);
        assertThat(buffer.getPages().get(task2)).hasSize(0);
        assertThat(buffer.getPages().get(task3)).hasSize(0);

        assertTrue(latch.await(10, SECONDS));
        assertEquals(failedTasks, ImmutableSet.of(task2, task3));

        exchangeClient.addLocation(task4, location4);
        assertThat(buffer.getAllTasks()).containsExactlyInAnyOrder(task1, task2, task3, task4);
        assertTaskIsNotFinished(buffer, task4);

        processor.setComplete(location4);
        buffer.whenTaskFinished(task4).get(10, SECONDS);
        assertThat(buffer.getPages().get(task4)).hasSize(2);
        assertThat(buffer.getFinishedTasks()).containsExactlyInAnyOrder(task1, task4);

        assertFalse(exchangeClient.isFinished());
        buffer.setFinished(true);
        assertTrue(exchangeClient.isFinished());

        exchangeClient.close();

        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(0).getHttpRequestState(), "not scheduled", "httpRequestState"));
        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(1).getHttpRequestState(), "not scheduled", "httpRequestState"));
        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(2).getHttpRequestState(), "not scheduled", "httpRequestState"));
        assertEventually(() -> assertEquals(exchangeClient.getStatus().getPageBufferClientStatuses().get(3).getHttpRequestState(), "not scheduled", "httpRequestState"));

        assertThat(buffer.getFinishedTasks()).containsExactlyInAnyOrder(task1, task4);
        assertThat(buffer.getFailedTasks().keySet()).containsExactlyInAnyOrder(task2, task3);
        assertThat(buffer.getFailedTasks().asMap().get(task2)).hasSize(1);
        assertThat(buffer.getFailedTasks().asMap().get(task2).iterator().next()).isInstanceOf(TrinoTransportException.class);
        assertThat(buffer.getFailedTasks().asMap().get(task3)).hasSize(1);
        assertThat(buffer.getFailedTasks().asMap().get(task3).iterator().next()).isEqualTo(trinoException);

        assertTrue(exchangeClient.isFinished());
    }

    private static void assertTaskIsNotFinished(TestingDirectExchangeBuffer buffer, TaskId task)
    {
        assertThatThrownBy(() -> buffer.whenTaskFinished(task).get(50, MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    public void testStreamingBufferLimit()
    {
        DataSize maxResponseSize = DataSize.ofBytes(1);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                new StreamingDirectExchangeBuffer(scheduler, DataSize.ofBytes(1)),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-testBufferLimit-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        exchangeClient.addLocation(new TaskId(new StageId("query", 1), 0, 0), location);
        exchangeClient.noMoreLocations();
        assertFalse(exchangeClient.isFinished());

        long start = System.nanoTime();

        // wait for a page to be fetched
        do {
            // there is no thread coordination here, so sleep is the best we can do
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 1, 1, 1, "not scheduled");

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(), createPage(1));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 2, 2, 2, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(), createPage(2));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 3, 3, 3, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove last page
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertTrue(exchangeClient.isFinished());
        exchangeClient.close();
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testStreamingAbortOnDataCorruption()
    {
        URI location = URI.create("http://localhost:8080");
        DirectExchangeClient exchangeClient = setUpDataCorruption(DataIntegrityVerification.ABORT, location);

        assertThatThrownBy(() -> getNextPage(exchangeClient))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Checksum verification failure on localhost when reading from http://localhost:8080/0: Data corruption, read checksum: 0xdd450d930a94ddde, calculated checksum: 0x9bdc9de3ce57c972");

        exchangeClient.close();
    }

    @Test
    public void testStreamingRetryDataCorruption()
    {
        URI location = URI.create("http://localhost:8080");
        DirectExchangeClient exchangeClient = setUpDataCorruption(DataIntegrityVerification.RETRY, location);

        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertNull(getNextPage(exchangeClient));
        assertTrue(exchangeClient.isFinished());
        exchangeClient.close();

        DirectExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 2, 4, 4, "not scheduled");
    }

    private DirectExchangeClient setUpDataCorruption(DataIntegrityVerification dataIntegrityVerification, URI location)
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);

        MockExchangeRequestProcessor delegate = new MockExchangeRequestProcessor(maxResponseSize);
        delegate.addPage(location, createPage(1));
        delegate.addPage(location, createPage(2));
        delegate.setComplete(location);

        TestingHttpClient.Processor processor = new TestingHttpClient.Processor()
        {
            private int completedRequests;
            private TestingResponse savedResponse;

            @Override
            public synchronized Response handle(Request request)
                    throws Exception
            {
                if (completedRequests == 0) {
                    verify(savedResponse == null);
                    TestingResponse response = (TestingResponse) delegate.handle(request);
                    checkState(response.getStatusCode() == HttpStatus.OK.code(), "Unexpected status code: %s", response.getStatusCode());
                    ListMultimap<String, String> headers = response.getHeaders().entries().stream()
                            .collect(toImmutableListMultimap(entry -> entry.getKey().toString(), Map.Entry::getValue));
                    byte[] bytes = toByteArray(response.getInputStream());
                    checkState(bytes.length > 42, "too short");
                    savedResponse = new TestingResponse(HttpStatus.OK, headers, bytes.clone());
                    // corrupt
                    bytes[42]++;
                    completedRequests++;
                    return new TestingResponse(HttpStatus.OK, headers, bytes);
                }

                if (completedRequests == 1) {
                    verify(savedResponse != null);
                    Response response = savedResponse;
                    savedResponse = null;
                    completedRequests++;
                    return response;
                }

                completedRequests++;
                return delegate.handle(request);
            }
        };

        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                dataIntegrityVerification,
                new StreamingDirectExchangeBuffer(scheduler, DataSize.of(32, Unit.MEGABYTE)),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});

        exchangeClient.addLocation(new TaskId(new StageId("query", 1), 0, 0), location);
        exchangeClient.noMoreLocations();

        return exchangeClient;
    }

    @Test
    public void testStreamingClose()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.ofBytes(1);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        DirectExchangeClient exchangeClient = new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                new StreamingDirectExchangeBuffer(scheduler, DataSize.ofBytes(1)),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-testClose-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                (taskId, failure) -> {});
        exchangeClient.addLocation(new TaskId(new StageId("query", 1), 0, 0), location);
        exchangeClient.noMoreLocations();

        // fetch a page
        assertFalse(exchangeClient.isFinished());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        while (!exchangeClient.isFinished()) {
            MILLISECONDS.sleep(10);
        }
        assertTrue(exchangeClient.isFinished());
        assertNull(exchangeClient.pollPage());
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);

        PageBufferClientStatus clientStatus = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getHttpRequestState(), "not scheduled", "httpRequestState");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static Slice createSerializedPage(int size)
    {
        return PAGES_SERDE.serialize(PAGES_SERDE.newContext(), createPage(size));
    }

    private static Slice getNextPage(DirectExchangeClient exchangeClient)
    {
        ListenableFuture<Slice> futurePage = Futures.transform(exchangeClient.isBlocked(), ignored -> exchangeClient.isFinished() ? null : exchangeClient.pollPage(), directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static void assertPageEquals(Slice actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(getSerializedPagePositionCount(actualPage), expectedPage.getPositionCount());
        assertEquals(PAGES_SERDE.deserialize(actualPage).getChannelCount(), expectedPage.getChannelCount());
    }

    private static void assertStatus(
            PageBufferClientStatus clientStatus,
            URI location,
            String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }
}
