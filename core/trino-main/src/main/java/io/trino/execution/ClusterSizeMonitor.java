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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.AllNodes;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.TrinoException;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClusterSizeMonitor
{
    private final InternalNodeManager nodeManager;
    private final boolean includeCoordinator;
    private final ScheduledExecutorService executor;

    private final Consumer<AllNodes> listener = this::updateAllNodes;

    @GuardedBy("this")
    private int currentCount;

    @GuardedBy("this")
    private final PriorityQueue<MinNodesFuture> futuresQueue = new PriorityQueue<>(comparing(MinNodesFuture::getExecutionMinCount));

    @Inject
    public ClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig)
    {
        this(
                nodeManager,
                requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator());
    }

    public ClusterSizeMonitor(
            InternalNodeManager nodeManager,
            boolean includeCoordinator)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.includeCoordinator = includeCoordinator;
        this.executor = newSingleThreadScheduledExecutor(threadsNamed("node-monitor-%s"));
    }

    @PostConstruct
    public void start()
    {
        nodeManager.addNodeChangeListener(listener);
        updateAllNodes(nodeManager.getAllNodes());
    }

    @PreDestroy
    public void stop()
    {
        nodeManager.removeNodeChangeListener(listener);
        executor.shutdown();
    }

    /**
     * Returns a listener that completes when the minimum number of workers for the cluster has been met.
     * Note: caller should not add a listener using the direct executor, as this can delay the
     * notifications for other listeners.
     */
    public synchronized ListenableFuture<Void> waitForMinimumWorkers(int executionMinCount, Duration executionMaxWait)
    {
        checkArgument(executionMinCount > 0, "executionMinCount should be greater than 0");
        requireNonNull(executionMaxWait, "executionMaxWait is null");

        if (currentCount >= executionMinCount) {
            return immediateVoidFuture();
        }

        SettableFuture<Void> future = SettableFuture.create();
        MinNodesFuture minNodesFuture = new MinNodesFuture(executionMinCount, future);
        futuresQueue.add(minNodesFuture);

        // if future does not finish in wait period, complete with an exception
        ScheduledFuture<?> timeoutTask = executor.schedule(
                () -> {
                    synchronized (this) {
                        future.setException(new TrinoException(
                                GENERIC_INSUFFICIENT_RESOURCES,
                                format("Insufficient active worker nodes. Waited %s for at least %s workers, but only %s workers are active", executionMaxWait, executionMinCount, currentCount)));
                    }
                },
                executionMaxWait.toMillis(),
                MILLISECONDS);

        // remove future if finished (e.g., canceled, timed out)
        future.addListener(() -> {
            timeoutTask.cancel(true);
            removeFuture(minNodesFuture);
        }, executor);

        return future;
    }

    private synchronized void removeFuture(MinNodesFuture minNodesFuture)
    {
        futuresQueue.remove(minNodesFuture);
    }

    private synchronized void updateAllNodes(AllNodes allNodes)
    {
        if (includeCoordinator) {
            currentCount = allNodes.getActiveNodes().size();
        }
        else {
            currentCount = Sets.difference(allNodes.getActiveNodes(), allNodes.getActiveCoordinators()).size();
        }

        ImmutableList.Builder<SettableFuture<Void>> listenersBuilder = ImmutableList.builder();
        while (!futuresQueue.isEmpty()) {
            MinNodesFuture minNodesFuture = futuresQueue.peek();
            if (minNodesFuture == null || minNodesFuture.getExecutionMinCount() > currentCount) {
                break;
            }
            listenersBuilder.add(minNodesFuture.getFuture());
            // this should not happen since we have a lock
            checkState(futuresQueue.poll() == minNodesFuture, "Unexpected modifications to MinNodesFuture queue");
        }
        ImmutableList<SettableFuture<Void>> listeners = listenersBuilder.build();
        executor.submit(() -> listeners.forEach(listener -> listener.set(null)));
    }

    @Managed
    public synchronized int getRequiredWorkers()
    {
        return futuresQueue.stream()
                .map(MinNodesFuture::getExecutionMinCount)
                .max(Integer::compareTo)
                .orElse(0);
    }

    private static class MinNodesFuture
    {
        private final int executionMinCount;
        private final SettableFuture<Void> future;

        MinNodesFuture(int executionMinCount, SettableFuture<Void> future)
        {
            this.executionMinCount = executionMinCount;
            this.future = future;
        }

        int getExecutionMinCount()
        {
            return executionMinCount;
        }

        SettableFuture<Void> getFuture()
        {
            return future;
        }
    }
}
