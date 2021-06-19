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
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.buffer.PageCodecMarker;
import io.trino.execution.buffer.SerializedPage;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.HttpPageBufferClient.ClientCallback;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.sql.analyzer.FeaturesConfig.DataIntegrityVerification;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ExchangeClient
        implements Closeable
{
    private static final SerializedPage NO_MORE_PAGES = new SerializedPage(EMPTY_SLICE, PageCodecMarker.MarkerSet.empty(), 0, 0);
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final String selfAddress;
    private final DataIntegrityVerification dataIntegrityVerification;
    private final long bufferCapacity;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final Duration maxErrorDuration;
    private final boolean acknowledgePages;
    private final HttpClient httpClient;
    private final ScheduledExecutorService scheduler;

    @GuardedBy("this")
    private boolean noMoreLocations;

    private final ConcurrentMap<URI, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();

    private final Set<HttpPageBufferClient> completedClients = newConcurrentHashSet();
    private final LinkedBlockingDeque<SerializedPage> pageBuffer = new LinkedBlockingDeque<>();

    @GuardedBy("this")
    private final List<SettableFuture<Void>> blockedCallers = new ArrayList<>();

    @GuardedBy("this")
    private long bufferRetainedSizeInBytes;
    @GuardedBy("this")
    private long maxBufferRetainedSizeInBytes;
    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private final LocalMemoryContext systemMemoryContext;
    private final Executor pageBufferClientCallbackExecutor;

    // ExchangeClientStatus.mergeWith assumes all clients have the same bufferCapacity.
    // Please change that method accordingly when this assumption becomes not true.
    public ExchangeClient(
            String selfAddress,
            DataIntegrityVerification dataIntegrityVerification,
            DataSize bufferCapacity,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            HttpClient httpClient,
            ScheduledExecutorService scheduler,
            LocalMemoryContext systemMemoryContext,
            Executor pageBufferClientCallbackExecutor)
    {
        this.selfAddress = requireNonNull(selfAddress, "selfAddress is null");
        this.dataIntegrityVerification = requireNonNull(dataIntegrityVerification, "dataIntegrityVerification is null");
        this.bufferCapacity = bufferCapacity.toBytes();
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxErrorDuration = maxErrorDuration;
        this.acknowledgePages = acknowledgePages;
        this.httpClient = httpClient;
        this.scheduler = scheduler;
        this.systemMemoryContext = systemMemoryContext;
        this.maxBufferRetainedSizeInBytes = Long.MIN_VALUE;
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
    }

    public ExchangeClientStatus getStatus()
    {
        // The stats created by this method is only for diagnostics.
        // It does not guarantee a consistent view between different exchange clients.
        // Guaranteeing a consistent view introduces significant lock contention.
        ImmutableList.Builder<PageBufferClientStatus> pageBufferClientStatusBuilder = ImmutableList.builder();
        for (HttpPageBufferClient client : allClients.values()) {
            pageBufferClientStatusBuilder.add(client.getStatus());
        }
        List<PageBufferClientStatus> pageBufferClientStatus = pageBufferClientStatusBuilder.build();
        synchronized (this) {
            int bufferedPages = pageBuffer.size();
            if (bufferedPages > 0 && pageBuffer.peekLast() == NO_MORE_PAGES) {
                bufferedPages--;
            }
            return new ExchangeClientStatus(bufferRetainedSizeInBytes, maxBufferRetainedSizeInBytes, averageBytesPerRequest, successfulRequests, bufferedPages, noMoreLocations, pageBufferClientStatus);
        }
    }

    public synchronized void addLocation(URI location)
    {
        requireNonNull(location, "location is null");

        // Ignore new locations after close
        // NOTE: this MUST happen before checking no more locations is checked
        if (closed.get()) {
            return;
        }

        // ignore duplicate locations
        if (allClients.containsKey(location)) {
            return;
        }

        checkState(!noMoreLocations, "No more locations already set");

        HttpPageBufferClient client = new HttpPageBufferClient(
                selfAddress,
                httpClient,
                dataIntegrityVerification,
                maxResponseSize,
                maxErrorDuration,
                acknowledgePages,
                location,
                new ExchangeClientCallback(),
                scheduler,
                pageBufferClientCallbackExecutor);
        allClients.put(location, client);
        queuedClients.add(client);

        scheduleRequestIfNecessary();
    }

    public synchronized void noMoreLocations()
    {
        noMoreLocations = true;
        scheduleRequestIfNecessary();
    }

    public WorkProcessor<SerializedPage> pages()
    {
        return WorkProcessor.create(() -> {
            SerializedPage page = pollPage();
            if (page == null) {
                if (isFinished()) {
                    return ProcessState.finished();
                }

                ListenableFuture<Void> blocked = isBlocked();
                if (!blocked.isDone()) {
                    return ProcessState.blocked(blocked);
                }

                return ProcessState.yield();
            }

            return ProcessState.ofResult(page);
        });
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private void assertNotHoldsLock()
    {
        assert !Thread.holdsLock(this) : "Cannot get next page while holding a lock on this";
    }

    @Nullable
    public SerializedPage pollPage()
    {
        assertNotHoldsLock();

        throwIfFailed();

        if (closed.get()) {
            return null;
        }

        SerializedPage page = pageBuffer.poll();

        if (page == null) {
            return null;
        }

        if (page == NO_MORE_PAGES) {
            // mark client closed; close() will add the end marker
            close();

            notifyBlockedCallers();

            // don't return end of stream marker
            return null;
        }

        synchronized (this) {
            if (!closed.get()) {
                bufferRetainedSizeInBytes -= page.getRetainedSizeInBytes();
                systemMemoryContext.setBytes(bufferRetainedSizeInBytes);
            }
            scheduleRequestIfNecessary();
        }

        return page;
    }

    public boolean isFinished()
    {
        throwIfFailed();
        // For this to works, locations must never be added after is closed is set
        return isClosed() && completedClients.size() == allClients.size();
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public synchronized void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        for (HttpPageBufferClient client : allClients.values()) {
            closeQuietly(client);
        }
        pageBuffer.clear();
        systemMemoryContext.setBytes(0);
        bufferRetainedSizeInBytes = 0;
        if (pageBuffer.peekLast() != NO_MORE_PAGES) {
            checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
        }
        notifyBlockedCallers();
    }

    public synchronized void scheduleRequestIfNecessary()
    {
        if (isFinished() || isFailed()) {
            return;
        }

        // if finished, add the end marker
        if (noMoreLocations && completedClients.size() == allClients.size()) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (pageBuffer.peek() == NO_MORE_PAGES) {
                close();
            }
            notifyBlockedCallers();
            return;
        }

        long neededBytes = bufferCapacity - bufferRetainedSizeInBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            client.scheduleRequest();
        }
    }

    public ListenableFuture<Void> isBlocked()
    {
        // Fast path pre-check
        if (isClosed() || isFailed() || pageBuffer.peek() != null) {
            return NOT_BLOCKED;
        }
        synchronized (this) {
            // Recheck after acquiring the lock
            if (isClosed() || isFailed() || pageBuffer.peek() != null) {
                return NOT_BLOCKED;
            }
            SettableFuture<Void> future = SettableFuture.create();
            blockedCallers.add(future);
            return future;
        }
    }

    private boolean addPages(List<SerializedPage> pages)
    {
        // Compute stats before acquiring the lock
        long pagesRetainedSizeInBytes = 0;
        long responseSize = 0;
        for (SerializedPage page : pages) {
            pagesRetainedSizeInBytes += page.getRetainedSizeInBytes();
            responseSize += page.getSizeInBytes();
        }

        List<SettableFuture<Void>> notify = ImmutableList.of();
        synchronized (this) {
            if (isClosed() || isFailed()) {
                return false;
            }

            if (!pages.isEmpty()) {
                pageBuffer.addAll(pages);

                bufferRetainedSizeInBytes += pagesRetainedSizeInBytes;
                maxBufferRetainedSizeInBytes = Math.max(maxBufferRetainedSizeInBytes, bufferRetainedSizeInBytes);
                systemMemoryContext.setBytes(bufferRetainedSizeInBytes);

                // Notify pending listeners that a page has been added
                notify = ImmutableList.copyOf(blockedCallers);
                blockedCallers.clear();
            }

            successfulRequests++;
            // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
            averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests + responseSize / successfulRequests);
        }

        // Trigger notifications after releasing the lock
        notifyListeners(notify);

        return true;
    }

    private void notifyBlockedCallers()
    {
        List<SettableFuture<Void>> callers;
        synchronized (this) {
            callers = ImmutableList.copyOf(blockedCallers);
            blockedCallers.clear();
        }
        notifyListeners(callers);
    }

    private void notifyListeners(List<SettableFuture<Void>> blockedCallers)
    {
        for (SettableFuture<Void> blockedCaller : blockedCallers) {
            // Notify callers in a separate thread to avoid callbacks while holding a lock
            scheduler.execute(() -> blockedCaller.set(null));
        }
    }

    private synchronized void requestComplete(HttpPageBufferClient client)
    {
        if (!queuedClients.contains(client)) {
            queuedClients.add(client);
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFinished(HttpPageBufferClient client)
    {
        requireNonNull(client, "client is null");
        completedClients.add(client);
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFailed(Throwable cause)
    {
        // TODO: properly handle the failed vs closed state
        // it is important not to treat failures as a successful close
        if (!isClosed()) {
            failure.compareAndSet(null, cause);
            notifyBlockedCallers();
        }
    }

    private boolean isFailed()
    {
        return failure.get() != null;
    }

    private void throwIfFailed()
    {
        Throwable t = failure.get();
        if (t != null) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private class ExchangeClientCallback
            implements ClientCallback
    {
        @Override
        public boolean addPages(HttpPageBufferClient client, List<SerializedPage> pages)
        {
            requireNonNull(client, "client is null");
            requireNonNull(pages, "pages is null");
            return ExchangeClient.this.addPages(pages);
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            requireNonNull(client, "client is null");
            ExchangeClient.this.requestComplete(client);
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            ExchangeClient.this.clientFinished(client);
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause)
        {
            requireNonNull(client, "client is null");
            requireNonNull(cause, "cause is null");
            ExchangeClient.this.clientFailed(cause);
        }
    }

    private static void closeQuietly(HttpPageBufferClient client)
    {
        try {
            client.close();
        }
        catch (RuntimeException e) {
            // ignored
        }
    }
}
