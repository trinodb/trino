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

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.Slice;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.operator.HttpPageBufferClient.ClientCallback;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.TrinoMediaTypes.TRINO_PAGES;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.TestingPagesSerdes.createTestingPagesSerdeFactory;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.PAGE_TOO_LARGE;
import static io.trino.spi.StandardErrorCode.PAGE_TRANSPORT_ERROR;
import static io.trino.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.Failures.WORKER_NODE_ERROR;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHttpPageBufferClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final TaskId TASK_ID = new TaskId(new StageId("query", 0), 0, 0);

    @BeforeAll
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterAll
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
        Page expectedPage = new Page(100);

        DataSize expectedMaxSize = DataSize.of(11, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(expectedMaxSize);

        CyclicBarrier requestComplete = new CyclicBarrier(2);

        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(processor, scheduler),
                DataIntegrityVerification.ABORT,
                expectedMaxSize,
                new Duration(1, TimeUnit.MINUTES),
                true,
                TASK_ID,
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // fetch a page and verify
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertThat(callback.getPages()).hasSize(1);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertStatus(client, location, "queued", 1, 1, 1, 0, "not scheduled");

        // fetch no data and verify
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertStatus(client, location, "queued", 1, 2, 2, 0, "not scheduled");

        // fetch two more pages and verify
        processor.addPage(location, expectedPage);
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertThat(callback.getPages()).hasSize(2);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertPageEquals(expectedPage, callback.getPages().get(1));
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(0);
        callback.resetStats();
        assertStatus(client, location, "queued", 3, 3, 3, 0, "not scheduled");

        // finish and verify
        callback.resetStats();
        processor.setComplete(location);
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        // get the buffer complete signal
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(1);

        // schedule the delete call to the buffer
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getFinishedBuffers()).isEqualTo(1);

        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(0);

        assertStatus(client, location, "closed", 3, 5, 5, 0, "not scheduled");
    }

    @Test
    public void testLifecycle()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(processor, scheduler),
                DataIntegrityVerification.ABORT,
                DataSize.of(10, MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                true,
                TASK_ID,
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        client.scheduleRequest();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, 0, "PROCESSING_REQUEST");
        assertThat(client.isRunning()).isEqualTo(true);
        afterRequest.await(10, TimeUnit.SECONDS);

        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled");

        client.close();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 1, 1, "PROCESSING_REQUEST");
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 2, 1, "not scheduled");
    }

    @Test
    public void testInvalidResponses()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(1);
        CyclicBarrier afterRequest = new CyclicBarrier(1);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(processor, scheduler),
                DataIntegrityVerification.ABORT,
                DataSize.of(10, MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                true,
                TASK_ID,
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // send not found response and verify response was ignored
        processor.setResponse(new TestingResponse(HttpStatus.NOT_FOUND, ImmutableListMultimap.of(CONTENT_TYPE, TRINO_PAGES), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(1);
        assertThat(callback.getFailure()).isInstanceOf(PageTransportErrorException.class);
        assertThat(callback.getFailure()).hasMessageContaining("Expected response code to be 200, but was 404");
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled");

        // send invalid content type response and verify response was ignored
        callback.resetStats();
        processor.setResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "INVALID_TYPE"), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(1);
        assertThat(callback.getFailure()).isInstanceOf(PageTransportErrorException.class);
        assertThat(callback.getFailure()).hasMessageContaining("Expected application/x-trino-pages response from server but got INVALID_TYPE");
        assertStatus(client, location, "queued", 0, 2, 2, 2, "not scheduled");

        // send unexpected content type response and verify response was ignored
        callback.resetStats();
        processor.setResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(1);
        assertThat(callback.getFailure()).isInstanceOf(PageTransportErrorException.class);
        assertThat(callback.getFailure()).hasMessageContaining("Expected application/x-trino-pages response from server but got text/plain");
        assertStatus(client, location, "queued", 0, 3, 3, 3, "not scheduled");

        // close client and verify
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));
        client.close();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 3, 4, 3, "not scheduled");
    }

    @Test
    public void testCloseDuringPendingRequest()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(processor, scheduler),
                DataIntegrityVerification.ABORT,
                DataSize.of(10, MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                true,
                TASK_ID,
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // send request
        client.scheduleRequest();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, 0, "PROCESSING_REQUEST");
        assertThat(client.isRunning()).isEqualTo(true);
        // request is pending, now close it
        client.close();

        try {
            requestComplete.await(10, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException _) {
        }
        try {
            afterRequest.await(10, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException _) {
            afterRequest.reset();
        }
        // client.close() triggers a DELETE request, so wait for it to finish
        beforeRequest.await(10, TimeUnit.SECONDS);
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 2, 1, "not scheduled");
    }

    @Test
    public void testExceptionFromResponseHandler()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        AtomicReference<Duration> tickerIncrement = new AtomicReference<>(new Duration(0, TimeUnit.SECONDS));

        TestingHttpClient.Processor processor = input -> {
            Duration delta = tickerIncrement.get();
            ticker.increment(delta.toMillis(), TimeUnit.MILLISECONDS);
            throw new RuntimeException("Foo");
        };

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(processor, scheduler),
                DataIntegrityVerification.ABORT,
                DataSize.of(10, MEGABYTE),
                new Duration(30, TimeUnit.SECONDS),
                true,
                TASK_ID,
                location,
                callback,
                scheduler,
                ticker,
                pageBufferClientCallbackExecutor);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // request processor will throw exception, verify the request is marked a completed
        // this starts the error stopwatch
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(0);
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled");

        // advance time forward, but not enough to fail the client
        tickerIncrement.set(new Duration(30, TimeUnit.SECONDS));

        // verify that the client has not failed
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(2);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(0);
        assertStatus(client, location, "queued", 0, 2, 2, 2, "not scheduled");

        // advance time forward beyond the minimum error duration
        tickerIncrement.set(new Duration(31, TimeUnit.SECONDS));

        // verify that the client has failed
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertThat(callback.getPages()).isEmpty();
        assertThat(callback.getCompletedRequests()).isEqualTo(3);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(1);
        assertThat(callback.getFailure()).isInstanceOf(PageTransportTimeoutException.class);
        assertThat(callback.getFailure()).hasMessageContaining(WORKER_NODE_ERROR + " (http://localhost:8080/0 - 3 failures, failure duration 31.00s, total failed request time 31.00s)");
        assertStatus(client, location, "queued", 0, 3, 3, 3, "not scheduled");
    }

    @Test
    public void testErrorCodes()
    {
        assertThat(new PageTooLargeException().getErrorCode()).isEqualTo(PAGE_TOO_LARGE.toErrorCode());
        assertThat(new PageTransportErrorException(HostAddress.fromParts("127.0.0.1", 8080), "").getErrorCode()).isEqualTo(PAGE_TRANSPORT_ERROR.toErrorCode());
        assertThat(new PageTransportTimeoutException(HostAddress.fromParts("127.0.0.1", 8080), "", null).getErrorCode()).isEqualTo(PAGE_TRANSPORT_TIMEOUT.toErrorCode());
    }

    @Test
    public void testAverageSizeOfRequest()
    {
        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(new MockExchangeRequestProcessor(DataSize.of(10, MEGABYTE)), scheduler),
                DataIntegrityVerification.ABORT,
                DataSize.of(10, MEGABYTE),
                new Duration(30, TimeUnit.SECONDS),
                true,
                TASK_ID,
                URI.create("http://localhost:8080"),
                new TestingClientCallback(new CyclicBarrier(1)),
                scheduler,
                new TestingTicker(),
                pageBufferClientCallbackExecutor);

        assertThat(client.getAverageRequestSizeInBytes()).isEqualTo(0);

        client.requestSucceeded(0);
        assertThat(client.getAverageRequestSizeInBytes()).isEqualTo(0);

        client.requestSucceeded(1000);
        client.requestSucceeded(800);
        assertThat(client.getAverageRequestSizeInBytes()).isEqualTo(600);
    }

    @Test
    public void testMemoryExceededInAddPages()
            throws Exception
    {
        URI location = URI.create("http://localhost:8080");
        Page page = new Page(RunLengthEncodedBlock.create(BIGINT, 1L, 100));

        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(DataSize.of(10, MEGABYTE));
        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TrinoException expectedException = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Memory limit exceeded");
        AtomicBoolean addPagesCalled = new AtomicBoolean(false);

        TestingClientCallback callback = new TestingClientCallback(requestComplete)
        {
            @Override
            public boolean addPages(HttpPageBufferClient client, List<Slice> pages)
            {
                addPagesCalled.set(true);
                throw expectedException;
            }
        };

        HttpPageBufferClient client = new HttpPageBufferClient(
                "localhost",
                new TestingHttpClient(processor, scheduler),
                DataIntegrityVerification.ABORT,
                DataSize.of(10, MEGABYTE),
                new Duration(30, TimeUnit.SECONDS),
                true,
                TASK_ID,
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor);

        // attempt to fetch a page
        processor.addPage(location, page);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        // addPages was called
        assertThat(addPagesCalled.get()).isTrue();

        // Memory exceeded failure is reported
        assertThat(callback.getCompletedRequests()).isEqualTo(1);
        assertThat(callback.getFinishedBuffers()).isEqualTo(0);
        assertThat(callback.getFailedBuffers()).isEqualTo(1);
        assertThat(callback.getFailure()).isEqualTo(expectedException);
    }

    private static void assertStatus(
            HttpPageBufferClient client,
            URI location, String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            int requestsFailed,
            String httpRequestState)
    {
        PageBufferClientStatus actualStatus = client.getStatus();
        assertThat(actualStatus.getUri()).isEqualTo(location);
        assertThat(actualStatus.getState())
                .describedAs("status")
                .isEqualTo(status);
        assertThat(actualStatus.getPagesReceived())
                .describedAs("pagesReceived")
                .isEqualTo(pagesReceived);
        assertThat(actualStatus.getRequestsScheduled())
                .describedAs("requestsScheduled")
                .isEqualTo(requestsScheduled);
        assertThat(actualStatus.getRequestsCompleted())
                .describedAs("requestsCompleted")
                .isEqualTo(requestsCompleted);
        assertThat(actualStatus.getRequestsFailed())
                .describedAs("requestsFailed")
                .isEqualTo(requestsFailed);
        assertThat(actualStatus.getHttpRequestState())
                .describedAs("httpRequestState")
                .isEqualTo(httpRequestState);
    }

    private static void assertPageEquals(Page expectedPage, Page actualPage)
    {
        assertThat(actualPage.getPositionCount()).isEqualTo(expectedPage.getPositionCount());
        assertThat(actualPage.getChannelCount()).isEqualTo(expectedPage.getChannelCount());
    }

    private static class TestingClientCallback
            implements ClientCallback
    {
        private final PagesSerdeFactory serdeFactory = createTestingPagesSerdeFactory(LZ4);

        private final CyclicBarrier done;
        private final List<Slice> pages = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger completedRequests = new AtomicInteger();
        private final AtomicInteger finishedBuffers = new AtomicInteger();
        private final AtomicInteger failedBuffers = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        public TestingClientCallback(CyclicBarrier done)
        {
            this.done = done;
        }

        public List<Page> getPages()
        {
            PageDeserializer deserializer = serdeFactory.createDeserializer(Optional.empty());
            return pages.stream()
                    .map(deserializer::deserialize)
                    .collect(Collectors.toList());
        }

        private int getCompletedRequests()
        {
            return completedRequests.get();
        }

        private int getFinishedBuffers()
        {
            return finishedBuffers.get();
        }

        public int getFailedBuffers()
        {
            return failedBuffers.get();
        }

        public Throwable getFailure()
        {
            return failure.get();
        }

        @Override
        public boolean addPages(HttpPageBufferClient client, List<Slice> pages)
        {
            this.pages.addAll(pages);
            return true;
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            completedRequests.getAndIncrement();
            awaitDone();
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            finishedBuffers.getAndIncrement();
            awaitDone();
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause)
        {
            failedBuffers.getAndIncrement();
            failure.compareAndSet(null, cause);
            // requestComplete() will be called after this
        }

        public void resetStats()
        {
            pages.clear();
            completedRequests.set(0);
            finishedBuffers.set(0);
            failedBuffers.set(0);
            failure.set(null);
        }

        private void awaitDone()
        {
            try {
                done.await(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class StaticRequestProcessor
            implements TestingHttpClient.Processor
    {
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final CyclicBarrier beforeRequest;
        private final CyclicBarrier afterRequest;

        private StaticRequestProcessor(CyclicBarrier beforeRequest, CyclicBarrier afterRequest)
        {
            this.beforeRequest = beforeRequest;
            this.afterRequest = afterRequest;
        }

        private void setResponse(Response response)
        {
            this.response.set(response);
        }

        @SuppressWarnings({"ThrowFromFinallyBlock", "Finally"})
        @Override
        public Response handle(Request request)
                throws Exception
        {
            beforeRequest.await(10, TimeUnit.SECONDS);

            try {
                return response.get();
            }
            finally {
                afterRequest.await(10, TimeUnit.SECONDS);
            }
        }
    }
}
