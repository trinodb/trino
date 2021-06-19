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
package io.trino.operator.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.Lifespan;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PipelineExecutionStrategy;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.type.Type;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.type.BlockTypeOperators;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.trino.operator.exchange.LocalExchangeSink.finishedLocalExchangeSink;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@ThreadSafe
public class LocalExchange
{
    private final Supplier<LocalExchanger> exchangerSupplier;

    private final List<LocalExchangeSource> sources;

    private final LocalExchangeMemoryManager memoryManager;

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final List<LocalExchangeSinkFactory> allSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    @GuardedBy("this")
    private int nextSourceIndex;

    public LocalExchange(
            NodePartitioningManager nodePartitioningManager,
            Session session,
            int sinkFactoryCount,
            int bufferCount,
            PartitioningHandle partitioning,
            List<Integer> partitionChannels,
            List<Type> partitionChannelTypes,
            Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes,
            BlockTypeOperators blockTypeOperators)
    {
        this.allSinkFactories = Stream.generate(() -> new LocalExchangeSinkFactory(LocalExchange.this))
                .limit(sinkFactoryCount)
                .collect(toImmutableList());
        openSinkFactories.addAll(allSinkFactories);
        noMoreSinkFactories();

        ImmutableList.Builder<LocalExchangeSource> sources = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            sources.add(new LocalExchangeSource(source -> checkAllSourcesFinished()));
        }
        this.sources = sources.build();

        List<Consumer<PageReference>> buffers = this.sources.stream()
                .map(buffer -> (Consumer<PageReference>) buffer::addPage)
                .collect(toImmutableList());

        this.memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            Iterator<LocalExchangeSource> sourceIterator = this.sources.iterator();
            exchangerSupplier = () -> {
                checkState(sourceIterator.hasNext(), "no more sources");
                return new PassthroughExchanger(sourceIterator.next(), maxBufferedBytes.toBytes() / bufferCount, memoryManager::updateMemoryUsage);
            };
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            exchangerSupplier = () -> {
                PartitionFunction partitionFunction = createPartitionFunction(
                        nodePartitioningManager,
                        session,
                        blockTypeOperators,
                        partitioning,
                        bufferCount,
                        partitionChannels,
                        partitionChannelTypes,
                        partitionHashChannel);
                Function<Page, Page> partitionPagePreparer;
                if (isSystemPartitioning(partitioning)) {
                    partitionPagePreparer = identity();
                }
                else {
                    int[] partitionChannelsArray = Ints.toArray(partitionChannels);
                    partitionPagePreparer = page -> page.getColumns(partitionChannelsArray);
                }
                return new PartitioningExchanger(
                        buffers,
                        memoryManager,
                        partitionPagePreparer,
                        partitionFunction);
            };
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
    }

    public int getBufferCount()
    {
        return sources.size();
    }

    public long getBufferedBytes()
    {
        return memoryManager.getBufferedBytes();
    }

    public synchronized LocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        LocalExchangeSinkFactory newFactory = new LocalExchangeSinkFactory(this);
        openSinkFactories.add(newFactory);
        return newFactory;
    }

    public synchronized LocalExchangeSinkFactory getSinkFactory(LocalExchangeSinkFactoryId id)
    {
        return allSinkFactories.get(id.id);
    }

    public synchronized LocalExchangeSource getNextSource()
    {
        checkState(nextSourceIndex < sources.size(), "All operators already created");
        LocalExchangeSource result = sources.get(nextSourceIndex);
        nextSourceIndex++;
        return result;
    }

    @VisibleForTesting
    LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private static PartitionFunction createPartitionFunction(
            NodePartitioningManager nodePartitioningManager,
            Session session,
            BlockTypeOperators blockTypeOperators,
            PartitioningHandle partitioning,
            int partitionCount,
            List<Integer> partitionChannels,
            List<Type> partitionChannelTypes,
            Optional<Integer> partitionHashChannel)
    {
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");

        if (isSystemPartitioning(partitioning)) {
            HashGenerator hashGenerator;
            if (partitionHashChannel.isPresent()) {
                hashGenerator = new PrecomputedHashGenerator(partitionHashChannel.get());
            }
            else {
                hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels), blockTypeOperators);
            }
            return new LocalPartitionGenerator(hashGenerator, partitionCount);
        }

        // Distribute buckets assigned to this node among threads.
        // The same bucket function (with the same bucket count) as for node
        // partitioning must be used. This way rows within a single bucket
        // will be being processed by single thread.
        ConnectorBucketNodeMap connectorBucketNodeMap = nodePartitioningManager.getConnectorBucketNodeMap(session, partitioning);
        int bucketCount = connectorBucketNodeMap.getBucketCount();
        int[] bucketToPartition = new int[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // mix the bucket bits so we don't use the same bucket number used to distribute between stages
            int hashedBucket = (int) XxHash64.hash(Long.reverse(bucket));
            bucketToPartition[bucket] = hashedBucket & (partitionCount - 1);
        }

        return new BucketPartitionFunction(
                nodePartitioningManager.getBucketFunction(session, partitioning, partitionChannelTypes, bucketCount),
                bucketToPartition);
    }

    private static boolean isSystemPartitioning(PartitioningHandle partitioning)
    {
        return partitioning.getConnectorHandle() instanceof SystemPartitioningHandle;
    }

    private void checkAllSourcesFinished()
    {
        checkNotHoldsLock(this);

        if (!sources.stream().allMatch(LocalExchangeSource::isFinished)) {
            return;
        }

        // all sources are finished, so finish the sinks
        ImmutableList<LocalExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // since all sources are finished there is no reason to allow new pages to be added
        // this can happen with a limit query
        openSinks.forEach(LocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private LocalExchangeSink createSink(LocalExchangeSinkFactory factory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                // all sources have completed so return a sink that is already finished
                return finishedLocalExchangeSink();
            }

            // Note: exchanger can be stateful so create a new one for each sink
            LocalExchanger exchanger = exchangerSupplier.get();
            LocalExchangeSink sink = new LocalExchangeSink(exchanger, this::sinkFinished);
            sinks.add(sink);
            return sink;
        }
    }

    private void sinkFinished(LocalExchangeSink sink)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    private void noMoreSinkFactories()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            noMoreSinkFactories = true;
        }
        checkAllSinksComplete();
    }

    private void sinkFactoryClosed(LocalExchangeSinkFactory sinkFactory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Cannot execute this method while holding a lock");
    }

    @ThreadSafe
    public static class LocalExchangeFactory
    {
        private final NodePartitioningManager nodePartitioningManager;
        private final Session session;
        private final PartitioningHandle partitioning;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionChannelTypes;
        private final Optional<Integer> partitionHashChannel;
        private final PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy;
        private final DataSize maxBufferedBytes;
        private final BlockTypeOperators blockTypeOperators;
        private final int bufferCount;

        @GuardedBy("this")
        private boolean noMoreSinkFactories;
        // The number of total sink factories are tracked at planning time
        // so that the exact number of sink factory is known by the time execution starts.
        @GuardedBy("this")
        private int numSinkFactories;

        @GuardedBy("this")
        private final Map<Lifespan, LocalExchange> localExchangeMap = new HashMap<>();
        @GuardedBy("this")
        private final List<LocalExchangeSinkFactoryId> closedSinkFactories = new ArrayList<>();

        public LocalExchangeFactory(
                NodePartitioningManager nodePartitioningManager,
                Session session,
                PartitioningHandle partitioning,
                int defaultConcurrency,
                List<Type> types,
                List<Integer> partitionChannels,
                Optional<Integer> partitionHashChannel,
                PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy,
                DataSize maxBufferedBytes,
                BlockTypeOperators blockTypeOperators)
        {
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.session = requireNonNull(session, "session is null");
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            requireNonNull(types, "types is null");
            this.partitionChannelTypes = partitionChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            this.partitionHashChannel = requireNonNull(partitionHashChannel, "partitionHashChannel is null");
            this.exchangeSourcePipelineExecutionStrategy = requireNonNull(exchangeSourcePipelineExecutionStrategy, "exchangeSourcePipelineExecutionStrategy is null");
            this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
            this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");

            this.bufferCount = computeBufferCount(partitioning, defaultConcurrency, partitionChannels);
        }

        public synchronized LocalExchangeSinkFactoryId newSinkFactoryId()
        {
            checkState(!noMoreSinkFactories);
            LocalExchangeSinkFactoryId result = new LocalExchangeSinkFactoryId(numSinkFactories);
            numSinkFactories++;
            return result;
        }

        public synchronized void noMoreSinkFactories()
        {
            noMoreSinkFactories = true;
        }

        public int getBufferCount()
        {
            return bufferCount;
        }

        public synchronized LocalExchange getLocalExchange(Lifespan lifespan)
        {
            if (exchangeSourcePipelineExecutionStrategy == UNGROUPED_EXECUTION) {
                checkArgument(lifespan.isTaskWide(), "LocalExchangeFactory is declared as UNGROUPED_EXECUTION. Driver-group exchange cannot be created.");
            }
            else {
                checkArgument(!lifespan.isTaskWide(), "LocalExchangeFactory is declared as GROUPED_EXECUTION. Task-wide exchange cannot be created.");
            }
            return localExchangeMap.computeIfAbsent(lifespan, ignored -> {
                checkState(noMoreSinkFactories);
                LocalExchange localExchange = new LocalExchange(
                        nodePartitioningManager,
                        session,
                        numSinkFactories,
                        bufferCount,
                        partitioning,
                        partitionChannels,
                        partitionChannelTypes,
                        partitionHashChannel,
                        maxBufferedBytes,
                        blockTypeOperators);
                for (LocalExchangeSinkFactoryId closedSinkFactoryId : closedSinkFactories) {
                    localExchange.getSinkFactory(closedSinkFactoryId).close();
                }
                return localExchange;
            });
        }

        public synchronized void closeSinks(LocalExchangeSinkFactoryId sinkFactoryId)
        {
            closedSinkFactories.add(sinkFactoryId);
            for (LocalExchange localExchange : localExchangeMap.values()) {
                localExchange.getSinkFactory(sinkFactoryId).close();
            }
        }
    }

    private static int computeBufferCount(PartitioningHandle partitioning, int defaultConcurrency, List<Integer> partitionChannels)
    {
        int bufferCount;
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            bufferCount = 1;
            checkArgument(partitionChannels.isEmpty(), "Gather exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Broadcast exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Arbitrary exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Passthrough exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            // partitioned exchange
            bufferCount = defaultConcurrency;
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
        return bufferCount;
    }

    public static class LocalExchangeSinkFactoryId
    {
        private final int id;

        public LocalExchangeSinkFactoryId(int id)
        {
            this.id = id;
        }
    }

    // Sink factory is entirely a pass thought to LocalExchange.
    // This class only exists as a separate entity to deal with the complex lifecycle caused
    // by operator factories (e.g., duplicate and noMoreSinkFactories).
    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public LocalExchangeSink createSink()
        {
            return exchange.createSink(this);
        }

        public LocalExchangeSinkFactory duplicate()
        {
            return exchange.createSinkFactory();
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }

        public void noMoreSinkFactories()
        {
            exchange.noMoreSinkFactories();
        }
    }
}
