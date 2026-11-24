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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.azure.core.http.HttpMethod.GET;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static reactor.core.scheduler.Schedulers.newSingle;

// Every pipeline has a single permit
class TestAzureConcurrencyLimitHttpPipelinePolicy
{
    private static final String EVENT_LOOP_THREAD = "azure-test-event-loop";

    private final ExecutorService requestExecutor = newCachedThreadPool(daemonThreadsNamed("azure-test-request-%s"));

    @AfterEach
    void shutdownRequestExecutor()
    {
        requestExecutor.shutdownNow();
    }

    @Test
    void testRejectsNonPositiveMaxHttpRequests()
    {
        assertThatThrownBy(() -> new ConcurrencyLimitHttpPipelinePolicy(0, Duration.ofSeconds(30), requestExecutor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxHttpRequests must be greater than 0");
        assertThatThrownBy(() -> new ConcurrencyLimitHttpPipelinePolicy(-1, Duration.ofSeconds(30), requestExecutor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxHttpRequests must be greater than 0");
    }

    @Test
    void testRequestSentFromResponseHandlerRunsAfterResponseReleasesPermit()
            throws Exception
    {
        Sinks.One<HttpResponse> firstResponse = Sinks.one();
        CountDownLatch firstSent = new CountDownLatch(1);
        AtomicInteger sent = new AtomicInteger();
        AtomicReference<String> lastRequestThread = new AtomicReference<>();
        HttpClient client = firstThenEmpty(sent, Mono.fromRunnable(firstSent::countDown).then(firstResponse.asMono()));
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), request -> {
            lastRequestThread.set(Thread.currentThread().getName());
            return client.send(request);
        });

        Scheduler eventLoop = newSingle(EVENT_LOOP_THREAD);
        try {
            CompletableFuture<Void> completed = pipeline.send(newRequest())
                    .flatMap(_ -> pipeline.send(newRequest()))
                    .then()
                    .toFuture();
            assertThat(firstSent.await(5, SECONDS)).isTrue();

            // The event loop delivers the first response, and the flatMap sends the second request from that same
            // thread while the first request still holds the only permit
            eventLoop.schedule(() -> firstResponse.tryEmitValue(new EmptyHttpResponse(newRequest())));
            completed.get(5, SECONDS);
            assertThat(sent.get()).isEqualTo(2);
            assertThat(lastRequestThread.get()).doesNotStartWith(EVENT_LOOP_THREAD);
        }
        finally {
            eventLoop.dispose();
        }
    }

    @Test
    void testRequestWaitingForPermitDoesNotBlockSubscribingThread()
            throws Exception
    {
        Sinks.Empty<HttpResponse> firstResponse = Sinks.empty();
        CountDownLatch firstSent = new CountDownLatch(1);
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), firstThenEmpty(sent, Mono.fromRunnable(firstSent::countDown).then(firstResponse.asMono())));

        Scheduler eventLoop = newSingle(EVENT_LOOP_THREAD);
        try {
            CompletableFuture<HttpResponse> first = pipeline.send(newRequest()).toFuture();
            assertThat(firstSent.await(5, SECONDS)).isTrue();

            // The second request waits for the permit, so subscribing must return without blocking the event loop
            CompletableFuture<CompletableFuture<HttpResponse>> subscribed = new CompletableFuture<>();
            eventLoop.schedule(() -> subscribed.complete(pipeline.send(newRequest()).toFuture()));
            CompletableFuture<HttpResponse> next = subscribed.get(5, SECONDS);
            assertThat(next).isNotDone();
            assertThat(sent.get()).isEqualTo(1);

            eventLoop.schedule(firstResponse::tryEmitEmpty);
            first.get(5, SECONDS);
            next.get(5, SECONDS);
            assertThat(sent.get()).isEqualTo(2);
        }
        finally {
            eventLoop.dispose();
        }
    }

    @Test
    void testCancelledQueuedAndInFlightRequestsPassPermitToNextRequest()
            throws Exception
    {
        CountDownLatch firstSent = new CountDownLatch(1);
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), firstThenEmpty(sent, Mono.fromRunnable(firstSent::countDown).then(Mono.never())));

        CompletableFuture<HttpResponse> first = pipeline.send(newRequest()).toFuture();
        assertThat(firstSent.await(5, SECONDS)).isTrue();
        CompletableFuture<HttpResponse> cancelled = pipeline.send(newRequest()).toFuture();
        CompletableFuture<HttpResponse> next = pipeline.send(newRequest()).toFuture();
        assertThat(sent.get()).isEqualTo(1);

        assertThat(cancelled.cancel(true)).isTrue();
        assertThat(first.cancel(true)).isTrue();
        next.get(5, SECONDS);
        assertThat(sent.get()).isEqualTo(2);
    }

    @Test
    void testRequestCancelledAfterPermitGrantedBeforeStartReleasesPermit()
            throws Exception
    {
        // Requests get the permit on subscribe, but start only when the test runs them
        Queue<Runnable> pendingStarts = new ConcurrentLinkedQueue<>();
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), pendingStarts::add, _ -> {
            sent.incrementAndGet();
            return Mono.empty();
        });

        CompletableFuture<HttpResponse> cancelled = pipeline.send(newRequest()).toFuture();
        // The request holds the only permit, but has not started
        assertThat(pendingStarts).hasSize(1);
        assertThat(cancelled.cancel(true)).isTrue();
        pendingStarts.remove().run();

        CompletableFuture<HttpResponse> next = pipeline.send(newRequest()).toFuture();
        assertThat(pendingStarts).hasSize(1);
        pendingStarts.remove().run();
        next.get(5, SECONDS);
        assertThat(sent.get()).isEqualTo(1);
    }

    @Test
    void testTimedOutRequestReleasesPermit()
            throws Exception
    {
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofMillis(100), firstThenEmpty(sent, Mono.never()));

        CompletableFuture<HttpResponse> first = pipeline.send(newRequest()).toFuture();
        assertThatThrownBy(() -> first.get(5, SECONDS))
                .hasCauseInstanceOf(TimeoutException.class);
        pipeline.send(newRequest()).toFuture().get(5, SECONDS);
        assertThat(sent.get()).isEqualTo(2);
    }

    @Test
    void testRequestTimeoutDoesNotIncludeTimeBeforeStart()
    {
        // Each request holds the only permit until it times out, so the last one waits twice the timeout before it starts
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofMillis(100), _ -> {
            sent.incrementAndGet();
            return Mono.never();
        });

        List<CompletableFuture<HttpResponse>> responses = Stream.generate(() -> pipeline.send(newRequest()).toFuture())
                .limit(3)
                .collect(toImmutableList());
        for (CompletableFuture<HttpResponse> response : responses) {
            assertThatThrownBy(() -> response.get(5, SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
        }
        assertThat(sent.get()).isEqualTo(3);
    }

    @Test
    void testRequestThrowingInsteadOfFailingReleasesPermit()
            throws Exception
    {
        // The first request throws an exception and the second an error, instead of returning a failed Mono
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), _ -> switch (sent.getAndIncrement()) {
            case 0 -> throw new IllegalStateException("send failed");
            case 1 -> throw new AssertionError("send failed");
            default -> Mono.empty();
        });

        assertThatThrownBy(() -> pipeline.send(newRequest()).toFuture().get(5, SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> pipeline.send(newRequest()).toFuture().get(5, SECONDS))
                .hasCauseInstanceOf(AssertionError.class);
        pipeline.send(newRequest()).toFuture().get(5, SECONDS);
        assertThat(sent.get()).isEqualTo(3);
    }

    @Test
    void testSynchronousSendAcquiresAndReleasesPermit()
    {
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), _ -> {
            sent.incrementAndGet();
            return Mono.empty();
        });
        pipeline.sendSync(newRequest(), Context.NONE);
        pipeline.sendSync(newRequest(), Context.NONE);
        assertThat(sent.get()).isEqualTo(2);
    }

    @Test
    void testInterruptedSynchronousSendCancelsTransportAndReleasesPermit()
            throws Exception
    {
        CountDownLatch firstSent = new CountDownLatch(1);
        CountDownLatch firstCancelled = new CountDownLatch(1);
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(Duration.ofSeconds(30), firstThenEmpty(sent, Mono.create(sink -> {
            sink.onCancel(firstCancelled::countDown);
            firstSent.countDown();
        })));

        FutureTask<HttpResponse> first = new FutureTask<>(() -> pipeline.sendSync(newRequest(), Context.NONE));
        Thread caller = Thread.ofVirtual().start(first);
        try {
            assertThat(firstSent.await(5, SECONDS)).isTrue();
            CompletableFuture<HttpResponse> next = pipeline.send(newRequest()).toFuture();
            assertThat(next).isNotDone();

            caller.interrupt();
            assertThatThrownBy(() -> first.get(5, SECONDS))
                    .hasRootCauseInstanceOf(InterruptedException.class);
            assertThat(firstCancelled.await(5, SECONDS)).isTrue();
            next.get(5, SECONDS);
            assertThat(sent.get()).isEqualTo(2);
        }
        finally {
            caller.interrupt();
        }
    }

    @Test
    void testCloseCancelsInFlightAndQueuedRequests()
    {
        // Run permit handoffs inline so cancelling the first request hands off the permit during close.
        ConcurrencyLimitHttpPipelinePolicy policy = new ConcurrencyLimitHttpPipelinePolicy(1, Duration.ofSeconds(30), directExecutor());
        AtomicBoolean transportCancelled = new AtomicBoolean();
        AtomicInteger sent = new AtomicInteger();
        HttpPipeline pipeline = newPipeline(policy, firstThenEmpty(sent, Mono.create(sink -> sink.onCancel(() -> transportCancelled.set(true)))));

        CompletableFuture<HttpResponse> first = pipeline.send(newRequest()).toFuture();
        CompletableFuture<HttpResponse> queued = pipeline.send(newRequest()).toFuture();
        assertThat(first).isNotDone();
        assertThat(queued).isNotDone();
        assertThat(sent.get()).isEqualTo(1);

        policy.close();

        assertThat(first).isCancelled();
        assertThat(queued).isCancelled();
        assertThat(transportCancelled).isTrue();
        assertThat(pipeline.send(newRequest()).toFuture()).isCancelled();
        assertThat(sent.get()).isEqualTo(1);
    }

    private HttpPipeline newPipeline(Duration timeout, HttpClient client)
    {
        return newPipeline(timeout, requestExecutor, client);
    }

    private static HttpPipeline newPipeline(Duration timeout, Executor requestExecutor, HttpClient client)
    {
        return newPipeline(new ConcurrencyLimitHttpPipelinePolicy(1, timeout, requestExecutor), client);
    }

    private static HttpPipeline newPipeline(ConcurrencyLimitHttpPipelinePolicy policy, HttpClient client)
    {
        return new HttpPipelineBuilder()
                .policies(policy)
                .httpClient(client)
                .build();
    }

    // Responds to the first request with the given response, and to later requests with an empty response
    private static HttpClient firstThenEmpty(AtomicInteger sent, Mono<HttpResponse> first)
    {
        return _ -> sent.getAndIncrement() == 0 ? first : Mono.empty();
    }

    private static HttpRequest newRequest()
    {
        return new HttpRequest(GET, "https://example.invalid/");
    }

    private static final class EmptyHttpResponse
            extends HttpResponse
    {
        private EmptyHttpResponse(HttpRequest request)
        {
            super(request);
        }

        @Override
        public int getStatusCode()
        {
            return 200;
        }

        @Deprecated
        @Override
        public String getHeaderValue(String name)
        {
            return null;
        }

        @Override
        public HttpHeaders getHeaders()
        {
            return new HttpHeaders();
        }

        @Override
        public Flux<ByteBuffer> getBody()
        {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray()
        {
            return Mono.just(new byte[0]);
        }

        @Override
        public Mono<String> getBodyAsString()
        {
            return Mono.just("");
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset)
        {
            return getBodyAsString();
        }
    }
}
