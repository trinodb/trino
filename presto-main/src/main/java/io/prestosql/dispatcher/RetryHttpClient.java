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
package io.prestosql.dispatcher;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.PrestoException;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class RetryHttpClient
{
    private final HttpClient httpClient;
    private final Executor callbackExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    public RetryHttpClient(
            HttpClient httpClient,
            Executor callbackExecutor,
            ScheduledExecutorService scheduledExecutor)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.callbackExecutor = requireNonNull(callbackExecutor, "callbackExecutor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
    }

    public <T> ListenableFuture<T> execute(
            String action,
            Supplier<Request> requestSupplier,
            ResponseHandler<T, ?> responseHandler,
            Duration maxRequestTime)
    {
        HttpFutureValue<T> futureValue = new HttpFutureValue<>(
                action,
                requestSupplier,
                responseHandler,
                new Backoff(maxRequestTime));
        futureValue.sendNextRequest();
        return futureValue;
    }

    private class HttpFutureValue<T>
            extends AbstractFuture<T>
    {
        private final String action;
        private final Supplier<Request> requestSupplier;
        private final ResponseHandler<T, ?> responseHandler;
        private final Backoff backoff;

        public HttpFutureValue(
                String action,
                Supplier<Request> requestSupplier,
                ResponseHandler<T, ?> responseHandler,
                Backoff backoff)
        {
            this.action = requireNonNull(action, "action is null");
            this.requestSupplier = requireNonNull(requestSupplier, "requestSupplier is null");
            this.responseHandler = requireNonNull(responseHandler, "responseHandler is null");

            this.backoff = requireNonNull(backoff, "backoff is null");
        }

        private void sendNextRequest()
        {
            // if future is already completed (likely canceled), don't send another request
            if (isDone()) {
                return;
            }

            try {
                Request request = requestSupplier.get();

                Futures.addCallback(
                        httpClient.executeAsync(request, responseHandler),
                        new FutureCallback<T>()
                        {
                            @Override
                            public void onSuccess(T value)
                            {
                                set(value);
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                                if (t instanceof PrestoException) {
                                    setException(t);
                                    return;
                                }
                                if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
                                    setException(new PrestoException(
                                            SERVER_SHUTTING_DOWN,
                                            String.format("Unable to %s. HTTP client is closed: %s", action, request.getUri())));
                                    return;
                                }
                                failed();
                            }

                            private void failed()
                            {
                                // record failure
                                if (backoff.failure()) {
                                    setException(new PrestoException(
                                            TOO_MANY_REQUESTS_FAILED,
                                            String.format("Unable to %s. Back off depleted: %s", action, request.getUri())));
                                    return;
                                }

                                // if already done, just exit
                                if (isDone()) {
                                    return;
                                }

                                // reschedule
                                long delayNanos = backoff.getBackoffDelayNanos();
                                if (delayNanos == 0) {
                                    sendNextRequest();
                                }
                                else {
                                    scheduledExecutor.schedule(() -> sendNextRequest(), delayNanos, NANOSECONDS);
                                }
                            }
                        },
                        callbackExecutor);
            }
            catch (Throwable t) {
                setException(t);
            }
        }
    }

    public static <T> ResponseHandler<T, ?> createJsonResponseHandler(String action, JsonCodec<T> responseCodec, int... successfulResponseCodes)
    {
        return new ResponseHandlerAdapter<>(
                createFullJsonResponseHandler(requireNonNull(responseCodec, "responseCodec is null")),
                getJsonResponseProcessor(action, successfulResponseCodes));
    }

    private static <T> BiFunction<Request, JsonResponse<T>, T> getJsonResponseProcessor(String action, int... successfulResponseCodes)
    {
        ImmutableSet<Integer> responseCodes = ImmutableSet.copyOf(Ints.asList(successfulResponseCodes));
        return (request, result) -> {
            if (!responseCodes.contains(result.getStatusCode())) {
                throw new RuntimeException(String.format(
                        "Unable to %s. Unexpected response status code %s. Expected codes %s. URL: %s",
                        action,
                        result.getStatusCode(),
                        responseCodes,
                        request.getUri()));
            }

            if (!result.hasValue()) {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        String.format("Unable to %s. Server returned success with invalid json. URL: %s", action, request.getUri()),
                        result.getException());
            }
            return result.getValue();
        };
    }

    private static class ResponseHandlerAdapter<F, T, E extends Exception>
            implements ResponseHandler<T, E>
    {
        private final ResponseHandler<F, E> delegate;
        private final BiFunction<Request, F, T> adapter;

        public ResponseHandlerAdapter(ResponseHandler<F, E> delegate, BiFunction<Request, F, T> adapter)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.adapter = requireNonNull(adapter, "adapter is null");
        }

        @Override
        public T handleException(Request request, Exception exception)
                throws E
        {
            return adapter.apply(request, delegate.handleException(request, exception));
        }

        @Override
        public T handle(Request request, Response response)
                throws E
        {
            return adapter.apply(request, delegate.handle(request, response));
        }
    }
}
