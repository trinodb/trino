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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.node.NodeInfo;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.split.SplitSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.cycle;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.spi.cache.PlanSignature.canonicalizePlanSignature;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;

/**
 * Assigns addresses provided by {@link CacheManager} to splits that
 * are to be cached.
 */
public class CacheSplitSource
        implements SplitSource
{
    private final ConnectorSplitManager splitManager;
    private final SplitSource delegate;
    private final Function<String, Optional<HostAddress>> addressProvider;
    private final String canonicalSignature;
    private final Map<HostAddress, Queue<Split>> splitQueuePerWorker = new ConcurrentHashMap<>();
    private final SplitAdmissionController splitAdmissionController;
    private final int minSplitBatchSize;
    private final Executor executor;
    private final AtomicBoolean isLastBatchProcessed = new AtomicBoolean(false);

    public CacheSplitSource(
            PlanSignature signature,
            ConnectorSplitManager splitManager,
            SplitSource delegate,
            ConnectorAwareAddressProvider connectorAwareAddressProvider,
            NodeInfo nodeInfo,
            SplitAdmissionControllerProvider splitAdmissionControllerProvider,
            boolean schedulerIncludeCoordinator,
            int minSplitBatchSize,
            Executor executor)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        ConsistentHashingAddressProvider consistentHashingAddressProvider = connectorAwareAddressProvider.getAddressProvider(nodeInfo, delegate.getCatalogHandle(), schedulerIncludeCoordinator);
        consistentHashingAddressProvider.refreshHashRingIfNeeded();
        this.addressProvider = consistentHashingAddressProvider::getPreferredAddress;
        this.canonicalSignature = canonicalizePlanSignature(signature).toString();
        this.splitAdmissionController = requireNonNull(splitAdmissionControllerProvider, "splitAdmissionControllerProvider is null").get(signature);
        this.minSplitBatchSize = minSplitBatchSize;
        this.executor = requireNonNull(executor, "executor is null");
    }

    @VisibleForTesting
    CacheSplitSource(
            PlanSignature signature,
            ConnectorSplitManager splitManager,
            SplitSource delegate,
            Function<String, Optional<HostAddress>> addressProvider,
            SplitAdmissionController splitAdmissionController,
            int minSplitBatchSize)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.addressProvider = requireNonNull(addressProvider, "addressProvider is null");
        this.canonicalSignature = canonicalizePlanSignature(signature).toString();
        this.splitAdmissionController = requireNonNull(splitAdmissionController, "splitAdmissionController is null");
        this.minSplitBatchSize = minSplitBatchSize;
        // Set the executor to direct executor for testing purposes
        this.executor = directExecutor();
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return delegate.getCatalogHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        return assignAddressesAndGetMoreSplits(ImmutableList.of(), getSplitsFromQueue(maxSize), maxSize);
    }

    private ListenableFuture<SplitBatch> assignAddressesAndGetMoreSplits(List<Split> newBatch, List<Split> currentBatch, int maxSize)
    {
        // Assign addresses to splits that are cacheable and don't have preferred addresses.
        // Additionally, add splits to the queue if they cannot be scheduled at the moment.
        ImmutableList.Builder<Split> batchBuilder = ImmutableList.builder();
        batchBuilder.addAll(currentBatch);
        int currentSize = currentBatch.size();
        checkState(newBatch.size() <= maxSize - currentSize, "New split batch size exceeds the remaining capacity");
        for (Split split : newBatch) {
            Optional<CacheSplitId> splitId = splitManager.getCacheSplitId(split.getConnectorSplit());
            if (splitId.isEmpty()) {
                batchBuilder.add(split);
                currentSize++;
            }
            else {
                Optional<HostAddress> preferredAddress;
                if (!split.isRemotelyAccessible()) {
                    // Choose first address from connector provided worker addresses, so that split is
                    // scheduled deterministically on the worker node. This is such that we reuse the cached splits
                    // on the worker nodes.
                    preferredAddress = Optional.of(split.getAddresses().getFirst());
                }
                else {
                    // Get the preferred address for the split using consistent hashing
                    preferredAddress = addressProvider.apply(canonicalSignature + splitId.get());
                }
                if (preferredAddress.isPresent()) {
                    Split splitWithPreferredAddress = new Split(
                            split.getCatalogHandle(),
                            split.getConnectorSplit(),
                            splitId,
                            Optional.of(ImmutableList.of(preferredAddress.get())),
                            split.isSplitAddressEnforced());
                    if (splitAdmissionController.canScheduleSplit(splitId.get(), preferredAddress.get())) {
                        batchBuilder.add(splitWithPreferredAddress);
                        currentSize++;
                    }
                    else {
                        splitQueuePerWorker.computeIfAbsent(preferredAddress.get(), _ -> new ConcurrentLinkedQueue<>())
                                .add(splitWithPreferredAddress);
                    }
                }
                else {
                    // Skip caching if no preferred address could be located which could be due to no available nodes
                    batchBuilder.add(split);
                    currentSize++;
                }
            }
        }

        // If the current batch is not full, try fetching more splits from the queue in case some
        // splits became free to be scheduled.
        List<Split> splitsFromQueue = getSplitsFromQueue(maxSize - currentSize);
        batchBuilder.addAll(splitsFromQueue);
        currentSize += splitsFromQueue.size();

        int remainingSize = maxSize - currentSize;
        // If the current batch is still not full, fetch more splits from the source
        if ((remainingSize > 0 && currentSize < minSplitBatchSize) && !isLastBatchProcessed.get()) {
            return transformAsync(
                    delegate.getNextBatch(remainingSize),
                    nextBatch -> {
                        isLastBatchProcessed.set(nextBatch.isLastBatch());
                        return assignAddressesAndGetMoreSplits(nextBatch.getSplits(), batchBuilder.build(), maxSize);
                    },
                    executor);
        }

        return immediateFuture(createSplitBatch(batchBuilder.build()));
    }

    private List<Split> getSplitsFromQueue(int maxSize)
    {
        int currentSize = 0;
        ImmutableList.Builder<Split> batchBuilder = ImmutableList.builder();
        List<Map.Entry<HostAddress, Queue<Split>>> queues = new ArrayList<>(splitQueuePerWorker.entrySet());
        // randomize queue order to prevent scheduling skewness
        shuffle(queues);

        // When there are no more new splits (i.e. isLastBatchProcessed=true), forcefully release queued
        // splits in order to avoid increasing of query latency at the cost of potential
        // cache rejections. Additionally, if we don't do it, there is a possibility that the
        // splits in the queue will never be scheduled (deadlock). For example, during self-join.
        boolean forceRelease = isLastBatchProcessed.get() || getSplitQueueSize() > 1_000_000;

        for (Iterator<Map.Entry<HostAddress, Queue<Split>>> iter = cycle(queues);
                iter.hasNext() && currentSize < maxSize; ) {
            Map.Entry<HostAddress, Queue<Split>> entry = iter.next();
            HostAddress address = entry.getKey();
            Queue<Split> splitQueue = entry.getValue();
            Split split = splitQueue.peek();
            if (split == null
                    || !(forceRelease
                    || splitAdmissionController.canScheduleSplit(split.getCacheSplitId().orElseThrow(), address))) {
                iter.remove();
                continue;
            }

            batchBuilder.add(split);
            splitQueue.remove();
            currentSize++;
        }
        return batchBuilder.build();
    }

    private SplitBatch createSplitBatch(List<Split> splits)
    {
        return new SplitBatch(splits, isLastBatchProcessed.get() && getSplitQueueSize() == 0);
    }

    private int getSplitQueueSize()
    {
        return splitQueuePerWorker.values().stream()
                .mapToInt(Queue::size)
                .sum();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return delegate.getTableExecuteSplitsInfo();
    }
}
