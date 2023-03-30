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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.execution.TaskFailureListener;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.HttpPageBufferClient.ClientCallback;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.plugin.base.metrics.TDigestHistogram;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DirectExchangeClient
        implements Closeable
{
    private static final Logger log = Logger.get(DirectExchangeClient.class);

    private final String selfAddress;
    private final DataIntegrityVerification dataIntegrityVerification;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final Duration maxErrorDuration;
    private final boolean acknowledgePages;
    private final HttpClient httpClient;
    private final ScheduledExecutorService scheduledExecutor;

    @GuardedBy("this")
    private boolean noMoreLocations;

    private final Map<URI, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();

    private final Set<HttpPageBufferClient> completedClients = newConcurrentHashSet();
    private final DirectExchangeBuffer buffer;

    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private final TDigest requestDuration = new TDigest();

    @GuardedBy("memoryContextLock")
    @Nullable
    private LocalMemoryContext memoryContext;
    private final ReadWriteLock memoryContextLock = new ReentrantReadWriteLock();
    private final Lock memoryContextReadLock = memoryContextLock.readLock();
    private final Lock memoryContextWriteLock = memoryContextLock.writeLock();
    private final Executor pageBufferClientCallbackExecutor;
    private final TaskFailureListener taskFailureListener;

    // DirectExchangeClientStatus.mergeWith assumes all clients have the same bufferCapacity.
    // Please change that method accordingly when this assumption becomes not true.
    public DirectExchangeClient(
            String selfAddress,
            DataIntegrityVerification dataIntegrityVerification,
            DirectExchangeBuffer buffer,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            HttpClient httpClient,
            ScheduledExecutorService scheduledExecutor,
            LocalMemoryContext memoryContext,
            Executor pageBufferClientCallbackExecutor,
            TaskFailureListener taskFailureListener)
    {
        this.selfAddress = requireNonNull(selfAddress, "selfAddress is null");
        this.dataIntegrityVerification = requireNonNull(dataIntegrityVerification, "dataIntegrityVerification is null");
        this.buffer = requireNonNull(buffer, "buffer is null");
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxErrorDuration = maxErrorDuration;
        this.acknowledgePages = acknowledgePages;
        this.httpClient = httpClient;
        this.scheduledExecutor = scheduledExecutor;
        this.memoryContext = memoryContext;
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        this.taskFailureListener = requireNonNull(taskFailureListener, "taskFailureListener is null");
    }

    public DirectExchangeClientStatus getStatus()
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
            return new DirectExchangeClientStatus(
                    buffer.getRetainedSizeInBytes(),
                    buffer.getMaxRetainedSizeInBytes(),
                    averageBytesPerRequest,
                    successfulRequests,
                    buffer.getBufferedPageCount(),
                    buffer.getSpilledPageCount(),
                    buffer.getSpilledBytes(),
                    noMoreLocations,
                    pageBufferClientStatus,
                    new TDigestHistogram(TDigest.copyOf(requestDuration)));
        }
    }

    public synchronized void addLocation(TaskId taskId, URI location)
    {
        requireNonNull(location, "location is null");

        // Ignore new locations after close
        // NOTE: this MUST happen before checking no more locations is checked
        if (closed) {
            return;
        }

        checkArgument(!allClients.containsKey(location), "location already exist: %s", location);

        checkState(!noMoreLocations, "No more locations already set");
        buffer.addTask(taskId);
        HttpPageBufferClient client = new HttpPageBufferClient(
                selfAddress,
                httpClient,
                dataIntegrityVerification,
                maxResponseSize,
                maxErrorDuration,
                acknowledgePages,
                taskId,
                location,
                new ExchangeClientCallback(),
                scheduledExecutor,
                pageBufferClientCallbackExecutor);
        allClients.put(location, client);
        queuedClients.add(client);

        scheduleRequestIfNecessary();
    }

    public synchronized void noMoreLocations()
    {
        noMoreLocations = true;
        buffer.noMoreTasks();
        scheduleRequestIfNecessary();
    }

    public WorkProcessor<Slice> pages()
    {
        return WorkProcessor.create(() -> {
            Slice page = pollPage();
            if (page == null) {
                if (isFinished()) {
                    return ProcessState.finished();
                }

                ListenableFuture<Void> blocked = isBlocked();
                if (!blocked.isDone()) {
                    return ProcessState.blocked(blocked);
                }

                return ProcessState.yielded();
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
    public Slice pollPage()
    {
        assertNotHoldsLock();

        Slice page = buffer.pollPage();

        if (page == null) {
            return null;
        }

        // updating retained memory might be expensive, therefore it needs to be updated outside of exclusive lock
        updateRetainedMemory();
        scheduleRequestIfNecessary();

        // Return the page even if the client is closed.
        // A concurrent thread may have responded to the `isFinished` change
        // triggered by polling this page and may have closed the client.
        return page;
    }

    public boolean isFinished()
    {
        return buffer.isFinished() && completedClients.size() == allClients.size();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        for (HttpPageBufferClient client : allClients.values()) {
            closeQuietly(client);
        }
        try {
            buffer.close();
        }
        catch (RuntimeException e) {
            log.warn(e, "error closing buffer");
        }
        finally {
            releaseMemoryContext();
        }
    }

    @VisibleForTesting
    synchronized int scheduleRequestIfNecessary()
    {
        if ((buffer.isFinished() || buffer.isFailed()) && completedClients.size() == allClients.size()) {
            return 0;
        }

        long neededBytes = buffer.getRemainingCapacityInBytes();
        if (neededBytes <= 0) {
            return 0;
        }

        long reservedBytesForScheduledClients = allClients.values().stream()
                .filter(client -> !queuedClients.contains(client) && !completedClients.contains(client))
                .mapToLong(HttpPageBufferClient::getAverageRequestSizeInBytes)
                .sum();
        long projectedBytesToBeRequested = 0;
        int clientCount = 0;

        for (HttpPageBufferClient client : queuedClients) {
            if (projectedBytesToBeRequested >= neededBytes * concurrentRequestMultiplier - reservedBytesForScheduledClients) {
                break;
            }
            projectedBytesToBeRequested += client.getAverageRequestSizeInBytes();
            clientCount++;
        }
        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            client.scheduleRequest();
        }
        return clientCount;
    }

    public ListenableFuture<Void> isBlocked()
    {
        return buffer.isBlocked();
    }

    @VisibleForTesting
    Deque<HttpPageBufferClient> getQueuedClients()
    {
        return queuedClients;
    }

    @VisibleForTesting
    Map<URI, HttpPageBufferClient> getAllClients()
    {
        return allClients;
    }

    private boolean addPages(HttpPageBufferClient client, List<Slice> pages)
    {
        checkState(!completedClients.contains(client), "client is already marked as completed");
        // Compute stats before acquiring the lock
        long responseSize = 0;
        if (!pages.isEmpty()) {
            for (Slice page : pages) {
                responseSize += page.length();
            }
            // Buffer may already be closed at this point. In such situation the buffer is expected to simply ignore this call.
            buffer.addPages(client.getRemoteTaskId(), pages);
            // updating retained memory might be expensive, therefore it needs to be updated outside of exclusive lock
            updateRetainedMemory();
        }

        synchronized (this) {
            if (closed || buffer.isFinished() || buffer.isFailed()) {
                return false;
            }

            successfulRequests++;
            // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
            averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests + responseSize / successfulRequests);
        }

        return true;
    }

    private void updateRetainedMemory()
    {
        memoryContextReadLock.lock();
        try {
            if (memoryContext != null) {
                memoryContext.setBytes(buffer.getRetainedSizeInBytes());
            }
        }
        finally {
            memoryContextReadLock.unlock();
        }
    }

    private void releaseMemoryContext()
    {
        memoryContextWriteLock.lock();
        try {
            if (memoryContext != null) {
                memoryContext.setBytes(0);
                // prevent further memory allocations
                memoryContext = null;
            }
        }
        finally {
            memoryContextWriteLock.unlock();
        }
    }

    private synchronized void requestComplete(HttpPageBufferClient client)
    {
        requestDuration.add(client.getLastRequestDurationMillis());
        if (!completedClients.contains(client) && !queuedClients.contains(client)) {
            queuedClients.add(client);
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFinished(HttpPageBufferClient client)
    {
        requireNonNull(client, "client is null");
        if (completedClients.add(client)) {
            buffer.taskFinished(client.getRemoteTaskId());
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFailed(HttpPageBufferClient client, Throwable cause)
    {
        requireNonNull(client, "client is null");
        if (completedClients.add(client)) {
            buffer.taskFailed(client.getRemoteTaskId(), cause);
            scheduledExecutor.execute(() -> taskFailureListener.onTaskFailed(client.getRemoteTaskId(), cause));
            closeQuietly(client);
        }
        scheduleRequestIfNecessary();
    }

    private class ExchangeClientCallback
            implements ClientCallback
    {
        @Override
        public boolean addPages(HttpPageBufferClient client, List<Slice> pages)
        {
            requireNonNull(client, "client is null");
            requireNonNull(pages, "pages is null");
            return DirectExchangeClient.this.addPages(client, pages);
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            requireNonNull(client, "client is null");
            DirectExchangeClient.this.requestComplete(client);
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            DirectExchangeClient.this.clientFinished(client);
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause)
        {
            requireNonNull(client, "client is null");
            requireNonNull(cause, "cause is null");
            DirectExchangeClient.this.clientFailed(client, cause);
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
