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
package io.trino.operator.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OperatorInfo;
import io.trino.operator.OutputFactory;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Mergeable;
import io.trino.spi.Page;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputOperator
        implements Operator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final OutputBuffer outputBuffer;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;
        private final PositionsAppenderFactory positionsAppenderFactory;
        private final Optional<Slice> exchangeEncryptionKey;
        private final AggregatedMemoryContext memoryContext;
        private final int pagePartitionerPoolSize;
        private final Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer;

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory,
                PositionsAppenderFactory positionsAppenderFactory,
                Optional<Slice> exchangeEncryptionKey,
                AggregatedMemoryContext memoryContext,
                int pagePartitionerPoolSize,
                Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.positionsAppenderFactory = requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
            this.exchangeEncryptionKey = requireNonNull(exchangeEncryptionKey, "exchangeEncryptionKey is null");
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.pagePartitionerPoolSize = pagePartitionerPoolSize;
            this.skewedPartitionRebalancer = requireNonNull(skewedPartitionRebalancer, "skewedPartitionRebalancer is null");
        }

        @Override
        public OperatorFactory createOutputOperator(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Function<Page, Page> pagePreprocessor,
                PagesSerdeFactory serdeFactory)
        {
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    types,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory,
                    positionsAppenderFactory,
                    exchangeEncryptionKey,
                    memoryContext,
                    pagePartitionerPoolSize,
                    skewedPartitionRebalancer);
        }
    }

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final OutputBuffer outputBuffer;
        private final PagesSerdeFactory serdeFactory;
        private final DataSize maxMemory;
        private final PositionsAppenderFactory positionsAppenderFactory;
        private final Optional<Slice> exchangeEncryptionKey;
        private final AggregatedMemoryContext memoryContext;
        private final int pagePartitionerPoolSize;
        private final Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer;
        private final PagePartitionerPool pagePartitionerPool;

        public PartitionedOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerdeFactory serdeFactory,
                DataSize maxMemory,
                PositionsAppenderFactory positionsAppenderFactory,
                Optional<Slice> exchangeEncryptionKey,
                AggregatedMemoryContext memoryContext,
                int pagePartitionerPoolSize,
                Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.positionsAppenderFactory = requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
            this.exchangeEncryptionKey = requireNonNull(exchangeEncryptionKey, "exchangeEncryptionKey is null");
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.pagePartitionerPoolSize = pagePartitionerPoolSize;
            this.skewedPartitionRebalancer = requireNonNull(skewedPartitionRebalancer, "skewedPartitionRebalancer is null");
            this.pagePartitionerPool = new PagePartitionerPool(
                    pagePartitionerPoolSize,
                    () -> {
                        boolean partitionProcessRleAndDictionaryBlocks = true;
                        PartitionFunction function = partitionFunction;
                        if (skewedPartitionRebalancer.isPresent()) {
                            function = new SkewedPartitionFunction(partitionFunction, skewedPartitionRebalancer.get());
                            // Partition flattened Rle and Dictionary blocks since if they are scaled then we want to
                            // round-robin the entire block to increase the writing parallelism across tasks/workers.
                            partitionProcessRleAndDictionaryBlocks = false;
                        }
                        return new PagePartitioner(
                                function,
                                partitionChannels,
                                partitionConstants,
                                replicatesAnyRow,
                                nullChannel,
                                outputBuffer,
                                serdeFactory,
                                sourceTypes,
                                maxMemory,
                                positionsAppenderFactory,
                                exchangeEncryptionKey,
                                memoryContext,
                                partitionProcessRleAndDictionaryBlocks);
                    });
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedOutputOperator.class.getSimpleName());
            return new PartitionedOutputOperator(
                    operatorContext,
                    pagePreprocessor,
                    outputBuffer,
                    pagePartitionerPool,
                    skewedPartitionRebalancer);
        }

        @Override
        public void noMoreOperators()
        {
            pagePartitionerPool.close();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory,
                    positionsAppenderFactory,
                    exchangeEncryptionKey,
                    memoryContext,
                    pagePartitionerPoolSize,
                    skewedPartitionRebalancer);
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitionerPool pagePartitionerPool;
    private final PagePartitioner pagePartitioner;
    private final Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer;
    // outputBuffer is used only to block the operator from finishing if the outputBuffer is full
    private final OutputBuffer outputBuffer;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;
    private boolean finished;

    public PartitionedOutputOperator(
            OperatorContext operatorContext,
            Function<Page, Page> pagePreprocessor,
            OutputBuffer outputBuffer,
            PagePartitionerPool pagePartitionerPool,
            Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.pagePartitionerPool = requireNonNull(pagePartitionerPool, "pagePartitionerPool is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.pagePartitioner = requireNonNull(pagePartitionerPool.poll(), "pagePartitioner is null");
        this.skewedPartitionRebalancer = requireNonNull(skewedPartitionRebalancer, "skewedPartitionRebalancer is null");
        this.pagePartitioner.setupOperator(operatorContext);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (!finished) {
            pagePartitionerPool.release(pagePartitioner);
            finished = true;
        }
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public void close()
            throws Exception
    {
        // make sure the operator is finished and partitionFunction released
        finish();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // Avoid re-synchronizing on the output buffer when operator is already blocked
        if (isBlocked.isDone()) {
            isBlocked = outputBuffer.isFull();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finished);

        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);
        pagePartitioner.partitionPage(page);

        // Rebalance skewed partitions in the case of scale writer hash partitioning
        if (skewedPartitionRebalancer.isPresent()) {
            SkewedPartitionRebalancer rebalancer = skewedPartitionRebalancer.get();

            // Update data processed and partitionRowCount state
            rebalancer.addDataProcessed(page.getSizeInBytes());
            ((SkewedPartitionFunction) pagePartitioner.getPartitionFunction()).flushPartitionRowCountToRebalancer();

            // Rebalance only when output buffer is full. This resembles that the downstream writing stage is slow, and
            // we could rebalance partitions to increase the concurrency at downstream stage.
            if (!outputBuffer.isFull().isDone()) {
                rebalancer.rebalance();
            }
        }
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    public static class PartitionedOutputInfo
            implements Mergeable<PartitionedOutputInfo>, OperatorInfo
    {
        private final long rowsAdded;
        private final long pagesAdded;
        private final long outputBufferPeakMemoryUsage;

        @JsonCreator
        public PartitionedOutputInfo(
                @JsonProperty("rowsAdded") long rowsAdded,
                @JsonProperty("pagesAdded") long pagesAdded,
                @JsonProperty("outputBufferPeakMemoryUsage") long outputBufferPeakMemoryUsage)
        {
            this.rowsAdded = rowsAdded;
            this.pagesAdded = pagesAdded;
            this.outputBufferPeakMemoryUsage = outputBufferPeakMemoryUsage;
        }

        @JsonProperty
        public long getRowsAdded()
        {
            return rowsAdded;
        }

        @JsonProperty
        public long getPagesAdded()
        {
            return pagesAdded;
        }

        @JsonProperty
        public long getOutputBufferPeakMemoryUsage()
        {
            return outputBufferPeakMemoryUsage;
        }

        @Override
        public PartitionedOutputInfo mergeWith(PartitionedOutputInfo other)
        {
            return new PartitionedOutputInfo(
                    rowsAdded + other.rowsAdded,
                    pagesAdded + other.pagesAdded,
                    Math.max(outputBufferPeakMemoryUsage, other.outputBufferPeakMemoryUsage));
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("rowsAdded", rowsAdded)
                    .add("pagesAdded", pagesAdded)
                    .add("outputBufferPeakMemoryUsage", outputBufferPeakMemoryUsage)
                    .toString();
        }
    }
}
