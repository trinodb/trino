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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.ResponseTooLargeException;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PagesSerdeUtil;
import io.trino.server.remotetask.Backoff;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoTransportException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.NO_CONTENT;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.TrinoMediaTypes.TRINO_PAGES_TYPE;
import static io.trino.execution.buffer.PagesSerdeUtil.NO_CHECKSUM;
import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;
import static io.trino.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static io.trino.operator.HttpPageBufferClient.PagesResponse.createEmptyPagesResponse;
import static io.trino.operator.HttpPageBufferClient.PagesResponse.createPagesResponse;
import static io.trino.server.InternalHeaders.TRINO_BUFFER_COMPLETE;
import static io.trino.server.InternalHeaders.TRINO_MAX_SIZE;
import static io.trino.server.InternalHeaders.TRINO_PAGE_NEXT_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_PAGE_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_TASK_FAILED;
import static io.trino.server.InternalHeaders.TRINO_TASK_INSTANCE_ID;
import static io.trino.server.PagesResponseWriter.SERIALIZED_PAGES_MAGIC;
import static io.trino.spi.HostAddress.fromUri;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.trino.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static io.trino.util.Failures.WORKER_NODE_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class HttpPageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(HttpPageBufferClient.class);

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p/>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback
    {
        boolean addPages(HttpPageBufferClient client, List<Slice> pages);

        void requestComplete(HttpPageBufferClient client);

        void clientFinished(HttpPageBufferClient client);

        void clientFailed(HttpPageBufferClient client, Throwable cause);
    }

    private final String selfAddress;
    private final HttpClient httpClient;
    private final DataIntegrityVerification dataIntegrityVerification;
    private final DataSize maxResponseSize;
    private final boolean acknowledgePages;
    private final TaskId remoteTaskId;
    private final URI location;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService scheduledExecutor;
    private final Backoff backoff;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private HttpResponseFuture<?> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();
    @GuardedBy("this")
    private long token;
    @GuardedBy("this")
    private boolean scheduled;
    @GuardedBy("this")
    private boolean completed;
    @GuardedBy("this")
    private String taskInstanceId;
    private volatile long lastRequestStartNanos;
    private volatile long lastRequestDurationMillis;
    // it is synchronized on `this` for update
    private volatile long averageRequestSizeInBytes;

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsSucceeded = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();

    private final Executor pageBufferClientCallbackExecutor;
    private final Ticker ticker;

    public HttpPageBufferClient(
            String selfAddress,
            HttpClient httpClient,
            DataIntegrityVerification dataIntegrityVerification,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            TaskId remoteTaskId,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduledExecutor,
            Executor pageBufferClientCallbackExecutor)
    {
        this(
                selfAddress,
                httpClient,
                dataIntegrityVerification,
                maxResponseSize,
                maxErrorDuration,
                acknowledgePages,
                remoteTaskId,
                location,
                clientCallback,
                scheduledExecutor,
                Ticker.systemTicker(),
                pageBufferClientCallbackExecutor);
    }

    public HttpPageBufferClient(
            String selfAddress,
            HttpClient httpClient,
            DataIntegrityVerification dataIntegrityVerification,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            TaskId remoteTaskId,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduledExecutor,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor)
    {
        this.selfAddress = requireNonNull(selfAddress, "selfAddress is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.dataIntegrityVerification = requireNonNull(dataIntegrityVerification, "dataIntegrityVerification is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.acknowledgePages = acknowledgePages;
        this.remoteTaskId = requireNonNull(remoteTaskId, "remoteTaskId is null");
        this.location = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(maxErrorDuration, ticker);
        this.ticker = ticker;
    }

    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (future != null) {
            state = "running";
        }
        else if (scheduled) {
            state = "scheduled";
        }
        else if (completed) {
            state = "completed";
        }
        else {
            state = "queued";
        }
        String httpRequestState = "not scheduled";
        if (future != null) {
            httpRequestState = future.getState();
        }

        long rejectedRows = rowsRejected.get();
        int rejectedPages = pagesRejected.get();

        return new PageBufferClientStatus(
                location,
                state,
                lastUpdate,
                rowsReceived.get(),
                pagesReceived.get(),
                rejectedRows == 0 ? OptionalLong.empty() : OptionalLong.of(rejectedRows),
                rejectedPages == 0 ? OptionalInt.empty() : OptionalInt.of(rejectedPages),
                requestsScheduled.get(),
                requestsCompleted.get(),
                requestsFailed.get(),
                requestsSucceeded.get(),
                httpRequestState);
    }

    public TaskId getRemoteTaskId()
    {
        return remoteTaskId;
    }

    public long getAverageRequestSizeInBytes()
    {
        return averageRequestSizeInBytes;
    }

    public synchronized boolean isRunning()
    {
        return future != null;
    }

    @Override
    public void close()
    {
        boolean shouldDestroyTaskResults;
        Future<?> future;
        synchronized (this) {
            shouldDestroyTaskResults = !closed;

            closed = true;

            future = this.future;

            this.future = null;

            lastUpdate = DateTime.now();
        }

        if (future != null && !future.isDone()) {
            future.cancel(true);
        }

        // destroy task results on the remote node; response is ignored
        if (shouldDestroyTaskResults) {
            destroyTaskResults();
        }
    }

    public synchronized void scheduleRequest()
    {
        if (closed || (future != null) || scheduled) {
            return;
        }
        scheduled = true;

        // start before scheduling to include error delay
        backoff.startRequest();

        long delayNanos = backoff.getBackoffDelayNanos();
        scheduledExecutor.schedule(() -> {
            try {
                initiateRequest();
            }
            catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(HttpPageBufferClient.this, t);
            }
        }, delayNanos, NANOSECONDS);

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    public long getLastRequestDurationMillis()
    {
        return lastRequestDurationMillis;
    }

    private synchronized void initiateRequest()
    {
        scheduled = false;
        if (closed || (future != null)) {
            return;
        }

        if (completed) {
            destroyTaskResults();
        }
        else {
            sendGetResults();
        }

        lastUpdate = DateTime.now();
    }

    private synchronized void sendGetResults()
    {
        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(token)).build();
        lastRequestStartNanos = ticker.read();
        HttpResponseFuture<PagesResponse> resultFuture = httpClient.executeAsync(
                prepareGet()
                        .setHeader(TRINO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri).build(),
                new PageResponseHandler(dataIntegrityVerification != DataIntegrityVerification.NONE));

        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(PagesResponse result)
            {
                assertNotHoldsLock(this);
                lastRequestDurationMillis = (ticker.read() - lastRequestStartNanos) / 1_000_000;
                backoff.success();

                List<Slice> pages;
                boolean pagesAccepted;
                try {
                    if (result.isTaskFailed()) {
                        throw new TrinoException(REMOTE_TASK_FAILED, format("Remote task failed: %s", remoteTaskId));
                    }

                    boolean shouldAcknowledge = false;
                    synchronized (HttpPageBufferClient.this) {
                        if (taskInstanceId == null) {
                            taskInstanceId = result.getTaskInstanceId();
                        }

                        if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                            throw new TrinoException(REMOTE_TASK_MISMATCH, format("%s (%s). Expected taskInstanceId: %s, received taskInstanceId: %s",
                                    REMOTE_TASK_MISMATCH_ERROR,
                                    fromUri(uri),
                                    taskInstanceId,
                                    result.getTaskInstanceId()));
                        }

                        if (result.getToken() == token) {
                            pages = result.getPages();
                            token = result.getNextToken();
                            shouldAcknowledge = pages.size() > 0;
                        }
                        else {
                            pages = ImmutableList.of();
                        }
                    }

                    if (shouldAcknowledge && acknowledgePages) {
                        // Acknowledge token without handling the response.
                        // The next request will also make sure the token is acknowledged.
                        // This is to fast release the pages on the buffer side.
                        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(result.getNextToken())).appendPath("acknowledge").build();
                        httpClient.executeAsync(prepareGet().setUri(uri).build(), new ResponseHandler<Void, RuntimeException>()
                        {
                            @Override
                            public Void handleException(Request request, Exception exception)
                            {
                                log.debug(exception, "Acknowledge request failed: %s", uri);
                                return null;
                            }

                            @Override
                            public Void handle(Request request, Response response)
                            {
                                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                                    log.debug("Unexpected acknowledge response code: %s", response.getStatusCode());
                                }
                                return null;
                            }
                        });
                    }

                    // add pages:
                    // addPages must be called regardless of whether pages is an empty list because
                    // clientCallback can keep stats of requests and responses. For example, it may
                    // keep track of how often a client returns empty response and adjust request
                    // frequency or buffer size.
                    pagesAccepted = clientCallback.addPages(HttpPageBufferClient.this, pages);
                }
                catch (TrinoException e) {
                    handleFailure(e, resultFuture);
                    return;
                }

                // update client stats
                if (!pages.isEmpty()) {
                    int pageCount = pages.size();
                    long rowCount = pages.stream().mapToLong(PagesSerdeUtil::getSerializedPagePositionCount).sum();
                    if (pagesAccepted) {
                        pagesReceived.addAndGet(pageCount);
                        rowsReceived.addAndGet(rowCount);
                    }
                    else {
                        pagesRejected.addAndGet(pageCount);
                        rowsRejected.addAndGet(rowCount);
                    }
                }
                requestsCompleted.incrementAndGet();
                long responseSize = pages.stream().mapToLong(Slice::length).sum();
                requestSucceeded(responseSize);

                synchronized (HttpPageBufferClient.this) {
                    // client is complete, acknowledge it by sending it a delete in the next request
                    if (result.isClientComplete()) {
                        completed = true;
                    }
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                clientCallback.requestComplete(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug("Request to %s failed %s", uri, t);
                assertNotHoldsLock(this);

                lastRequestDurationMillis = (ticker.read() - lastRequestStartNanos) / 1_000_000;

                if (t instanceof ChecksumVerificationException) {
                    switch (dataIntegrityVerification) {
                        case NONE:
                            // In case of NONE, failure is possible in case of inconsistent cluster configuration, so we should not retry.
                        case ABORT:
                            // TrinoException will not be retried
                            t = new TrinoException(GENERIC_INTERNAL_ERROR, format("Checksum verification failure on %s when reading from %s: %s", selfAddress, uri, t.getMessage()), t);
                            break;
                        case RETRY:
                            log.warn("Checksum verification failure on %s when reading from %s, may be retried: %s", selfAddress, uri, t.getMessage());
                            break;
                        default:
                            throw new AssertionError("Unsupported option: " + dataIntegrityVerification);
                    }
                }

                t = rewriteException(t);
                if (!(t instanceof TrinoException) && backoff.failure()) {
                    String message = format("%s (%s - %s failures, failure duration %s, total failed request time %s)",
                            WORKER_NODE_ERROR,
                            uri,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                    t = new PageTransportTimeoutException(fromUri(uri), message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, pageBufferClientCallbackExecutor);
    }

    @VisibleForTesting
    synchronized void requestSucceeded(long responseSize)
    {
        int successfulRequests = requestsSucceeded.incrementAndGet();
        // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
        averageRequestSizeInBytes = (long) ((1.0 * averageRequestSizeInBytes * (successfulRequests - 1)) + responseSize) / successfulRequests;
    }

    private synchronized void destroyTaskResults()
    {
        HttpResponseFuture<StatusResponse> resultFuture = httpClient.executeAsync(prepareDelete().setUri(location).build(), createStatusResponseHandler());
        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse result)
            {
                assertNotHoldsLock(this);

                if (result.getStatusCode() != NO_CONTENT.code()) {
                    onFailure(new TrinoTransportException(
                            REMOTE_BUFFER_CLOSE_FAILED,
                            fromUri(location),
                            format("Error closing remote buffer, expected %s got %s", NO_CONTENT.code(), result.getStatusCode())));
                    return;
                }

                backoff.success();
                synchronized (HttpPageBufferClient.this) {
                    closed = true;
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.clientFinished(HttpPageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                assertNotHoldsLock(this);

                log.error("Request to delete %s failed %s", location, t);
                if (!(t instanceof TrinoException) && backoff.failure()) {
                    String message = format("Error closing remote buffer (%s - %s failures, failure duration %s, total failed request time %s)",
                            location,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                    t = new TrinoTransportException(REMOTE_BUFFER_CLOSE_FAILED, fromUri(location), message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, pageBufferClientCallbackExecutor);
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private static void assertNotHoldsLock(Object lock)
    {
        assert !Thread.holdsLock(lock) : "Cannot execute this method while holding a lock";
    }

    private void handleFailure(Throwable t, HttpResponseFuture<?> expectedFuture)
    {
        // Cannot delegate to other callback while holding a lock on this
        assertNotHoldsLock(this);

        requestsFailed.incrementAndGet();
        requestsCompleted.incrementAndGet();

        if (t instanceof TrinoException) {
            clientCallback.clientFailed(HttpPageBufferClient.this, t);
        }

        synchronized (HttpPageBufferClient.this) {
            if (future == expectedFuture) {
                future = null;
            }
            lastUpdate = DateTime.now();
        }
        clientCallback.requestComplete(HttpPageBufferClient.this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HttpPageBufferClient that = (HttpPageBufferClient) o;

        if (!location.equals(that.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return location.hashCode();
    }

    @Override
    public String toString()
    {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            }
            else if (future != null) {
                state = "RUNNING";
            }
            else {
                state = "QUEUED";
            }
        }
        return toStringHelper(this)
                .add("location", location)
                .addValue(state)
                .toString();
    }

    private static Throwable rewriteException(Throwable t)
    {
        if (t instanceof ResponseTooLargeException) {
            return new PageTooLargeException();
        }
        return t;
    }

    public static class PageResponseHandler
            implements ResponseHandler<PagesResponse, RuntimeException>
    {
        private final boolean dataIntegrityVerificationEnabled;

        private PageResponseHandler(boolean dataIntegrityVerificationEnabled)
        {
            this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        }

        @Override
        public PagesResponse handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response)
        {
            URI uri = request.getUri();
            try {
                // no content means no content was created within the wait period, but query is still ok
                // if job is finished, complete is set in the response
                if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                    return createEmptyPagesResponse(
                            getTaskInstanceId(response, uri),
                            getToken(response, uri),
                            getNextToken(response, uri),
                            getComplete(response, uri),
                            getTaskFailed(response, uri));
                }

                // otherwise we must have gotten an OK response, everything else is considered fatal
                if (response.getStatusCode() != HttpStatus.OK.code()) {
                    StringBuilder body = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream(), UTF_8))) {
                        // Get up to 1000 lines for debugging
                        for (int i = 0; i < 1000; i++) {
                            String line = reader.readLine();
                            // Don't output more than 100KB
                            if (line == null || body.length() + line.length() > 100 * 1024) {
                                break;
                            }
                            body.append(line + "\n");
                        }
                    }
                    catch (RuntimeException | IOException e) {
                        // Ignored. Just return whatever message we were able to decode
                    }
                    throw new PageTransportErrorException(fromUri(uri), format("Expected response code to be 200, but was %s:%n%s", response.getStatusCode(), body));
                }

                // invalid content type can happen when an error page is returned, but is unlikely given the above 200
                String contentType = response.getHeader(CONTENT_TYPE);
                if (contentType == null) {
                    throw new PageTransportErrorException(fromUri(uri), format("%s header is not set: %s", CONTENT_TYPE, response));
                }
                if (!mediaTypeMatches(contentType, TRINO_PAGES_TYPE)) {
                    throw new PageTransportErrorException(fromUri(uri), format("Expected %s response from server but got %s", TRINO_PAGES_TYPE, contentType));
                }

                String taskInstanceId = getTaskInstanceId(response, uri);
                long token = getToken(response, uri);
                long nextToken = getNextToken(response, uri);
                boolean complete = getComplete(response, uri);
                boolean remoteTaskFailed = getTaskFailed(response, uri);

                try (LittleEndianDataInputStream input = new LittleEndianDataInputStream(response.getInputStream())) {
                    int magic = input.readInt();
                    if (magic != SERIALIZED_PAGES_MAGIC) {
                        throw new IllegalStateException(format("Invalid stream header, expected 0x%08x, but was 0x%08x", SERIALIZED_PAGES_MAGIC, magic));
                    }
                    long checksum = input.readLong();
                    int pagesCount = input.readInt();
                    List<Slice> pages = ImmutableList.copyOf(readSerializedPages(input));
                    verifyChecksum(checksum, pages);
                    checkState(pages.size() == pagesCount, "Wrong number of pages, expected %s, but read %s", pagesCount, pages.size());
                    return createPagesResponse(taskInstanceId, token, nextToken, pages, complete, remoteTaskFailed);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            catch (PageTransportErrorException e) {
                throw new PageTransportErrorException(fromUri(uri), format("Error fetching %s: %s", request.getUri().toASCIIString(), e.getMessage()), e);
            }
        }

        private void verifyChecksum(long readChecksum, List<Slice> pages)
        {
            if (dataIntegrityVerificationEnabled) {
                long calculatedChecksum = calculateChecksum(pages);
                if (readChecksum != calculatedChecksum) {
                    throw new ChecksumVerificationException(format("Data corruption, read checksum: 0x%08x, calculated checksum: 0x%08x", readChecksum, calculatedChecksum));
                }
            }
            else {
                if (readChecksum != NO_CHECKSUM) {
                    throw new ChecksumVerificationException(format("Expected checksum to be NO_CHECKSUM (0x%08x) but is 0x%08x", NO_CHECKSUM, readChecksum));
                }
            }
        }

        private static String getTaskInstanceId(Response response, URI uri)
        {
            String taskInstanceId = response.getHeader(TRINO_TASK_INSTANCE_ID);
            if (taskInstanceId == null) {
                throw new PageTransportErrorException(fromUri(uri), format("Expected %s header", TRINO_TASK_INSTANCE_ID));
            }
            return taskInstanceId;
        }

        private static long getToken(Response response, URI uri)
        {
            String tokenHeader = response.getHeader(TRINO_PAGE_TOKEN);
            if (tokenHeader == null) {
                throw new PageTransportErrorException(fromUri(uri), format("Expected %s header", TRINO_PAGE_TOKEN));
            }
            return Long.parseLong(tokenHeader);
        }

        private static long getNextToken(Response response, URI uri)
        {
            String nextTokenHeader = response.getHeader(TRINO_PAGE_NEXT_TOKEN);
            if (nextTokenHeader == null) {
                throw new PageTransportErrorException(fromUri(uri), format("Expected %s header", TRINO_PAGE_NEXT_TOKEN));
            }
            return Long.parseLong(nextTokenHeader);
        }

        private static boolean getComplete(Response response, URI uri)
        {
            String bufferComplete = response.getHeader(TRINO_BUFFER_COMPLETE);
            if (bufferComplete == null) {
                throw new PageTransportErrorException(fromUri(uri), format("Expected %s header", TRINO_BUFFER_COMPLETE));
            }
            return Boolean.parseBoolean(bufferComplete);
        }

        private static boolean getTaskFailed(Response response, URI uri)
        {
            String taskFailed = response.getHeader(TRINO_TASK_FAILED);
            if (taskFailed == null) {
                throw new PageTransportErrorException(fromUri(uri), format("Expected %s header", TRINO_TASK_FAILED));
            }
            return Boolean.parseBoolean(taskFailed);
        }

        private static boolean mediaTypeMatches(String value, MediaType range)
        {
            try {
                return MediaType.parse(value).is(range);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }

    public static class PagesResponse
    {
        public static PagesResponse createPagesResponse(String taskInstanceId, long token, long nextToken, Iterable<Slice> pages, boolean complete, boolean taskFailed)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, pages, complete, taskFailed);
        }

        public static PagesResponse createEmptyPagesResponse(String taskInstanceId, long token, long nextToken, boolean complete, boolean taskFailed)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, ImmutableList.of(), complete, taskFailed);
        }

        private final String taskInstanceId;
        private final long token;
        private final long nextToken;
        private final List<Slice> pages;
        private final boolean clientComplete;
        private final boolean taskFailed;

        private PagesResponse(String taskInstanceId, long token, long nextToken, Iterable<Slice> pages, boolean clientComplete, boolean taskFailed)
        {
            this.taskInstanceId = taskInstanceId;
            this.token = token;
            this.nextToken = nextToken;
            this.pages = ImmutableList.copyOf(pages);
            this.clientComplete = clientComplete;
            this.taskFailed = taskFailed;
        }

        public long getToken()
        {
            return token;
        }

        public long getNextToken()
        {
            return nextToken;
        }

        public List<Slice> getPages()
        {
            return pages;
        }

        public boolean isClientComplete()
        {
            return clientComplete;
        }

        public String getTaskInstanceId()
        {
            return taskInstanceId;
        }

        public boolean isTaskFailed()
        {
            return taskFailed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("token", token)
                    .add("nextToken", nextToken)
                    .add("pagesSize", pages.size())
                    .add("clientComplete", clientComplete)
                    .add("taskFailed", taskFailed)
                    .toString();
        }
    }

    private static class ChecksumVerificationException
            extends RuntimeException
    {
        public ChecksumVerificationException(String message)
        {
            super(requireNonNull(message, "message is null"));
        }
    }
}
