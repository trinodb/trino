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
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.trino.block.BlockAssertions;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.SerializedPage;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.sql.analyzer.FeaturesConfig.DataIntegrityVerification;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.trino.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExchangeClient
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
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                DataSize.of(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(3));
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.isClosed(), true);

        ExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test(timeOut = 10000)
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.of(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                DataSize.of(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-testAddLocation-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        URI location1 = URI.create("http://localhost:8081/foo");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(location1);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        URI location2 = URI.create("http://localhost:8082/bar");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(location2);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(4));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(5));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        exchangeClient.noMoreLocations();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!exchangeClient.isClosed()) {
            Thread.sleep(1);
        }

        ImmutableMap<URI, PageBufferClientStatus> statuses = uniqueIndex(exchangeClient.getStatus().getPageBufferClientStatuses(), PageBufferClientStatus::getUri);
        assertStatus(statuses.get(location1), location1, "closed", 3, 3, 3, "not scheduled");
        assertStatus(statuses.get(location2), location2, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testBufferLimit()
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
        ExchangeClient exchangeClient = new ExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                DataSize.ofBytes(1),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-testBufferLimit-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();
        assertEquals(exchangeClient.isClosed(), false);

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
        assertTrue(exchangeClient.getStatus().getBufferedBytes() == 0);
        assertEquals(exchangeClient.isClosed(), true);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testAbortOnDataCorruption()
    {
        URI location = URI.create("http://localhost:8080");
        ExchangeClient exchangeClient = setUpDataCorruption(DataIntegrityVerification.ABORT, location);

        assertFalse(exchangeClient.isClosed());
        assertThatThrownBy(() -> getNextPage(exchangeClient))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Checksum verification failure on localhost when reading from http://localhost:8080/0: Data corruption, read checksum: 0xf91cfe5d2bc6e1c2, calculated checksum: 0x3c51297c7b78052f");

        assertThatThrownBy(exchangeClient::isFinished)
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Checksum verification failure on localhost when reading from http://localhost:8080/0: Data corruption, read checksum: 0xf91cfe5d2bc6e1c2, calculated checksum: 0x3c51297c7b78052f");

        exchangeClient.close();
    }

    @Test
    public void testRetryDataCorruption()
    {
        URI location = URI.create("http://localhost:8080");
        ExchangeClient exchangeClient = setUpDataCorruption(DataIntegrityVerification.RETRY, location);

        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertNull(getNextPage(exchangeClient));
        assertTrue(exchangeClient.isClosed());

        ExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 2, 4, 4, "not scheduled");
    }

    private ExchangeClient setUpDataCorruption(DataIntegrityVerification dataIntegrityVerification, URI location)
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

        ExchangeClient exchangeClient = new ExchangeClient(
                "localhost",
                dataIntegrityVerification,
                DataSize.of(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        return exchangeClient;
    }

    @Test
    public void testClose()
            throws Exception
    {
        DataSize maxResponseSize = DataSize.ofBytes(1);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                DataSize.ofBytes(1),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-testClose-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);
        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        // fetch a page
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        while (!exchangeClient.isFinished()) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(exchangeClient.isClosed(), true);
        assertNull(exchangeClient.pollPage());
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertEquals(exchangeClient.getStatus().getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        PageBufferClientStatus clientStatus = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getHttpRequestState(), "not scheduled", "httpRequestState");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static SerializedPage getNextPage(ExchangeClient exchangeClient)
    {
        ListenableFuture<SerializedPage> futurePage = Futures.transform(exchangeClient.isBlocked(), ignored -> exchangeClient.pollPage(), directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static void assertPageEquals(SerializedPage actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
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
