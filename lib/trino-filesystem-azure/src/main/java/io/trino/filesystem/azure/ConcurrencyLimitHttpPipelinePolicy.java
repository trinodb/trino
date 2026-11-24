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

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.AsyncSemaphore;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateCancelledFuture;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.submit;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

/**
 * Limits in-flight requests without making the subscribing thread wait for a permit. This
 * thread can be the Netty event loop that must complete the responses that free permits.
 */
final class ConcurrencyLimitHttpPipelinePolicy
        implements HttpPipelinePolicy
{
    private final AsyncSemaphore<Supplier<ListenableFuture<HttpResponse>>, HttpResponse> requestSemaphore;
    private final Duration requestTimeout;
    private final Executor requestExecutor;

    // Futures do not define equality, so we need to use a unique key for each request to remove it from the map when it completes
    private final Map<Object, ListenableFuture<HttpResponse>> outstandingRequests = new LinkedHashMap<>();
    private volatile boolean closed;

    ConcurrencyLimitHttpPipelinePolicy(int maxHttpRequests, Duration requestTimeout, Executor requestExecutor)
    {
        checkArgument(maxHttpRequests > 0, "maxHttpRequests must be greater than 0");
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.requestExecutor = requireNonNull(requestExecutor, "requestExecutor is null");
        this.requestSemaphore = new AsyncSemaphore<>(maxHttpRequests, requestExecutor, Supplier::get);
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next)
    {
        return process(next::process);
    }

    @Override
    public HttpResponse processSync(HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next)
    {
        // Return a future to the semaphore before the transport finishes so cancellation can interrupt the blocking send.
        return process(() -> Mono.fromFuture(toCompletableFuture(submit(next::processSync, requestExecutor)))).block();
    }

    private Mono<HttpResponse> process(Supplier<Mono<HttpResponse>> request)
    {
        // Apply the request timeout only after a permit is granted and the request starts.
        return Mono.deferContextual(reactorContext -> {
            ListenableFuture<HttpResponse> response = submitRequest(() -> {
                try {
                    return toListenableFuture(request.get().timeout(requestTimeout).contextWrite(reactorContext).toFuture());
                }
                catch (Throwable t) {
                    // AsyncSemaphore releases the permit of a throwing task only for exceptions
                    return immediateFailedFuture(t);
                }
            });
            return Mono.fromFuture(toCompletableFuture(response));
        });
    }

    private synchronized ListenableFuture<HttpResponse> submitRequest(Supplier<ListenableFuture<HttpResponse>> request)
    {
        if (closed) {
            return immediateCancelledFuture();
        }

        // A permit can be handed to the next request while close is still cancelling the queue.
        ListenableFuture<HttpResponse> response = requestSemaphore.submit(() -> closed ? immediateCancelledFuture() : request.get());
        Object requestKey = new Object();
        outstandingRequests.put(requestKey, response);
        response.addListener(() -> removeRequest(requestKey), directExecutor());
        return response;
    }

    private synchronized void removeRequest(Object requestKey)
    {
        outstandingRequests.remove(requestKey);
    }

    void close()
    {
        List<ListenableFuture<HttpResponse>> outstanding;
        synchronized (this) {
            closed = true;
            outstanding = List.copyOf(outstandingRequests.values());
        }
        outstanding.forEach(response -> response.cancel(true));
    }
}
