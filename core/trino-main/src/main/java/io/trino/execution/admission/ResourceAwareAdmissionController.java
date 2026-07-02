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
package io.trino.execution.admission;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.QueryExecution;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.admission.ResourceAwareAdmissionSessionProperties.getQueryAdmissionMaxWait;
import static io.trino.execution.admission.ResourceAwareAdmissionSessionProperties.getRequiredFreeClusterMemory;
import static io.trino.execution.admission.ResourceAwareAdmissionSessionProperties.getRequiredFreeClusterVcpu;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Resource-aware query admission controller.
 * <p>
 * This is an admission gate the dispatch path waits on in addition to the existing
 * minimum-worker-count requirement. It does not replace or wrap that requirement:
 * {@code LocalDispatchQuery} keeps its {@code ClusterSizeMonitor.waitForMinimumWorkers(...)} call
 * and simply gates on admission first, so the two concerns stay independent.
 * <p>
 * {@link #awaitAdmission(Session)} returns a future that completes once the cluster has enough
 * observed free memory and vCPU for the query (per {@link ResourceAwareAdmissionConfig} / session
 * overrides), or fails with {@link io.trino.spi.StandardErrorCode#GENERIC_INSUFFICIENT_RESOURCES}
 * if the wait budget is exhausted. When the feature is disabled (the default) it returns an
 * already-completed future, so admission behaves exactly as it does without this gate.
 * <p>
 * Held queries sit in {@code WAITING_FOR_RESOURCES} and are re-evaluated on a fixed poll interval
 * against a fresh capacity snapshot read through the public
 * {@link ClusterMemoryManager#getWorkersMemoryInfo()}, releasing the oldest waiting query first
 * (FIFO, one release per cycle). This class is thread-safe.
 */
public class ResourceAwareAdmissionController
{
    private static final Logger log = Logger.get(ResourceAwareAdmissionController.class);

    private final Supplier<Map<String, Optional<MemoryInfo>>> workersMemoryInfoSupplier;
    private final ResourceAwareAdmissionConfig config;
    private final Ticker ticker;
    private final ScheduledExecutorService executor;
    private final QueryMemoryHistory memoryHistory;

    private final AtomicLong sequence = new AtomicLong();

    @GuardedBy("this")
    private final PriorityQueue<PendingQuery> pending = new PriorityQueue<>(Comparator.comparingLong(PendingQuery::sequence));

    @GuardedBy("this")
    private boolean scheduled;

    @Inject
    public ResourceAwareAdmissionController(ClusterMemoryManager clusterMemoryManager, ResourceAwareAdmissionConfig config)
    {
        this(clusterMemoryManager::getWorkersMemoryInfo,
                config,
                Ticker.systemTicker(),
                newSingleThreadScheduledExecutor(daemonThreadsNamed("query-admission-%s")));
    }

    @VisibleForTesting
    public ResourceAwareAdmissionController(
            ClusterMemoryManager clusterMemoryManager,
            ResourceAwareAdmissionConfig config,
            Ticker ticker,
            ScheduledExecutorService executor)
    {
        this(clusterMemoryManager::getWorkersMemoryInfo, config, ticker, executor);
    }

    @VisibleForTesting
    public ResourceAwareAdmissionController(
            Supplier<Map<String, Optional<MemoryInfo>>> workersMemoryInfoSupplier,
            ResourceAwareAdmissionConfig config,
            Ticker ticker,
            ScheduledExecutorService executor)
    {
        this.workersMemoryInfoSupplier = requireNonNull(workersMemoryInfoSupplier, "workersMemoryInfoSupplier is null");
        this.config = requireNonNull(config, "config is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.memoryHistory = new QueryMemoryHistory(config.getMemoryWindowSize());
    }

    /**
     * Package-private constructor for the no-op testing factory. When disabled, the controller
     * never calls {@code workersMemoryInfoSupplier} so it can safely return empty.
     */
    private ResourceAwareAdmissionController(ResourceAwareAdmissionConfig config)
    {
        this.workersMemoryInfoSupplier = Map::of;
        this.config = requireNonNull(config, "config is null");
        this.ticker = Ticker.systemTicker();
        this.executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("query-admission-noop-%s"));
        this.memoryHistory = new QueryMemoryHistory(config.getMemoryWindowSize());
    }

    /**
     * Returns a future that completes when the cluster has enough free capacity to admit the query,
     * or fails with {@code GENERIC_INSUFFICIENT_RESOURCES} after the query's wait budget elapses.
     * Returns an already-completed future when the feature is disabled.
     */
    public ListenableFuture<Void> awaitAdmission(Session session)
    {
        requireNonNull(session, "session is null");
        if (!config.isEnabled()) {
            return immediateVoidFuture();
        }
        return enqueue(buildContext(session));
    }

    @VisibleForTesting
    ListenableFuture<Void> enqueue(QueryAdmissionContext context)
    {
        long deadlineNanos = ticker.read() + context.maxWait().roundTo(NANOSECONDS);
        PendingQuery query = new PendingQuery(sequence.getAndIncrement(), context, deadlineNanos, SettableFuture.create());

        synchronized (this) {
            pending.add(query);
            scheduleIfNeeded();
        }
        // Drop the entry from the queue whenever its future settles (admitted, failed, or cancelled).
        query.future().addListener(() -> remove(query), directExecutor());
        return query.future();
    }

    private QueryAdmissionContext buildContext(Session session)
    {
        return new QueryAdmissionContext(
                session.getQueryId(),
                effectiveRequiredMemory(getRequiredFreeClusterMemory(session)),
                getRequiredFreeClusterVcpu(session),
                getQueryAdmissionMaxWait(session));
    }

    /**
     * Register a hook that feeds a completed query's peak user memory into the rolling history,
     * so subsequent admission decisions size their memory requirement from observed demand. The
     * {@code fallback} (configured {@code required-free-memory}) is used until the window fills.
     * No-op when the feature is disabled, so no listener is attached and there is zero overhead.
     */
    public void recordOnCompletion(QueryExecution queryExecution)
    {
        requireNonNull(queryExecution, "queryExecution is null");
        if (!config.isEnabled()) {
            return;
        }
        queryExecution.addFinalQueryInfoListener(queryInfo ->
                recordPeakMemory(queryInfo.getQueryStats().getPeakUserMemoryReservation().toBytes()));
    }

    /**
     * The memory a query must find free before admission: the rolling average of recent query
     * peaks (or {@code fallback} during cold start) scaled by the configured headroom multiplier.
     */
    @VisibleForTesting
    DataSize effectiveRequiredMemory(DataSize fallback)
    {
        DataSize average = memoryHistory.averageOrDefault(fallback);
        double multiplier = config.getRequiredMemoryMultiplier();
        if (multiplier == 1.0) {
            return average;
        }
        return DataSize.ofBytes(Math.round(average.toBytes() * multiplier));
    }

    @VisibleForTesting
    void recordPeakMemory(long peakMemoryBytes)
    {
        memoryHistory.record(peakMemoryBytes);
    }

    /**
     * Decide whether the query may proceed against the supplied capacity snapshot. Exposed for
     * testing; the engine drives admission through {@link #awaitAdmission(Session)}.
     */
    @VisibleForTesting
    WaitDecision shouldQueryWait(QueryAdmissionContext context, ClusterCapacity capacity)
    {
        long requiredMemoryBytes = context.requiredFreeMemory().toBytes();
        boolean enoughMemory = capacity.freeMemoryBytes() >= requiredMemoryBytes;
        boolean enoughVcpu = capacity.availableVcpu() >= context.requiredFreeVcpu();

        if (enoughMemory && enoughVcpu) {
            return new WaitDecision.ProceedNow(format(
                    "capacity available (free memory %s bytes >= %s, free vCPU %s >= %s)",
                    capacity.freeMemoryBytes(),
                    requiredMemoryBytes,
                    capacity.availableVcpu(),
                    context.requiredFreeVcpu()));
        }

        return new WaitDecision.Wait(context.maxWait(), format(
                "waiting for cluster capacity (free memory %s bytes / required %s, free vCPU %s / required %s)",
                capacity.freeMemoryBytes(),
                requiredMemoryBytes,
                capacity.availableVcpu(),
                context.requiredFreeVcpu()));
    }

    @GuardedBy("this")
    private void scheduleIfNeeded()
    {
        if (!scheduled && !pending.isEmpty()) {
            scheduled = true;
            executor.schedule(this::process, config.getPollInterval().toMillis(), MILLISECONDS);
        }
    }

    @VisibleForTesting
    void process()
    {
        List<PendingQuery> toAdmit = new ArrayList<>();
        List<PendingQuery> toExpire = new ArrayList<>();
        synchronized (this) {
            scheduled = false;
            if (pending.isEmpty()) {
                return;
            }

            long now = ticker.read();
            ClusterCapacity capacity = snapshotCapacity();

            // Fail any query that has exhausted its wait budget, regardless of position.
            Iterator<PendingQuery> iterator = pending.iterator();
            while (iterator.hasNext()) {
                PendingQuery query = iterator.next();
                if (now >= query.deadlineNanos()) {
                    toExpire.add(query);
                    iterator.remove();
                }
            }

            // FIFO release: only the oldest waiting query is considered each cycle, so that a query
            // admitted this cycle starts consuming memory before the next snapshot is taken. This
            // throttles admission and pairs with demand-driven scaling.
            PendingQuery head = pending.peek();
            if (head != null && shouldQueryWait(head.context(), capacity) instanceof WaitDecision.ProceedNow) {
                pending.poll();
                toAdmit.add(head);
            }

            scheduleIfNeeded();
        }

        // Complete futures outside the lock to avoid running listener callbacks while holding it.
        for (PendingQuery query : toAdmit) {
            query.future().set(null);
        }
        for (PendingQuery query : toExpire) {
            query.future().setException(insufficientResources(query.context()));
        }
    }

    private synchronized void remove(PendingQuery query)
    {
        pending.remove(query);
    }

    private ClusterCapacity snapshotCapacity()
    {
        try {
            return ClusterCapacity.from(workersMemoryInfoSupplier.get().values());
        }
        catch (RuntimeException e) {
            // Never let an observation hiccup wedge the gate; treat as no capacity this cycle.
            log.warn(e, "Failed to read cluster capacity; treating as zero capacity for this cycle");
            return new ClusterCapacity(0, 0);
        }
    }

    private static TrinoException insufficientResources(QueryAdmissionContext context)
    {
        return new TrinoException(
                GENERIC_INSUFFICIENT_RESOURCES,
                format("Insufficient cluster capacity. Waited %s for query %s but required free memory %s / vCPU %s was not available",
                        context.maxWait(),
                        context.queryId(),
                        context.requiredFreeMemory(),
                        context.requiredFreeVcpu()));
    }

    @VisibleForTesting
    synchronized int waitingQueryCount()
    {
        return pending.size();
    }

    /**
     * Creates a disabled admission controller that always admits immediately.
     * For use in tests where a real {@link ClusterMemoryManager} is not available.
     */
    @VisibleForTesting
    public static ResourceAwareAdmissionController createNoOpForTesting()
    {
        return new ResourceAwareAdmissionController(new ResourceAwareAdmissionConfig());
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    private record PendingQuery(long sequence, QueryAdmissionContext context, long deadlineNanos, SettableFuture<Void> future)
    {
        private PendingQuery
        {
            requireNonNull(context, "context is null");
            requireNonNull(future, "future is null");
        }
    }
}
