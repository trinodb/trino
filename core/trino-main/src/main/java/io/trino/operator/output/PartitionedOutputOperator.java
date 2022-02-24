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
import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.LocalMemoryContext;
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

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
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
                    maxMemory);
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
                DataSize maxMemory)
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
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedOutputOperator.class.getSimpleName());
            return new PartitionedOutputOperator(
                    operatorContext,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }

        @Override
        public void noMoreOperators()
        {
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
                    maxMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner partitionFunction;
    private final LocalMemoryContext memoryContext;
    private final long partitionsInitialRetainedSize;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;
    private boolean finished;

    public PartitionedOutputOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
            boolean replicatesAnyRow,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            PagesSerdeFactory serdeFactory,
            DataSize maxMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.partitionFunction = new PagePartitioner(
                partitionFunction,
                partitionChannels,
                partitionConstants,
                replicatesAnyRow,
                nullChannel,
                outputBuffer,
                serdeFactory,
                sourceTypes,
                maxMemory,
                operatorContext);

        operatorContext.setInfoSupplier(this.partitionFunction.getOperatorInfoSupplier());
        this.memoryContext = operatorContext.newLocalUserMemoryContext(PartitionedOutputOperator.class.getSimpleName());
        this.partitionsInitialRetainedSize = this.partitionFunction.getRetainedSizeInBytes();
        this.memoryContext.setBytes(partitionsInitialRetainedSize);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finished = true;
        partitionFunction.flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // Avoid re-synchronizing on the output buffer when operator is already blocked
        if (isBlocked.isDone()) {
            isBlocked = partitionFunction.isFull();
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

        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);
        partitionFunction.partitionPage(page);

        // We use getSizeInBytes() here instead of getRetainedSizeInBytes() for an approximation of
        // the amount of memory used by the pageBuilders, because calculating the retained
        // size can be expensive especially for complex types.
        long partitionsSizeInBytes = partitionFunction.getSizeInBytes();

        // We also add partitionsInitialRetainedSize as an approximation of the object overhead of the partitions.
        memoryContext.setBytes(partitionsSizeInBytes + partitionsInitialRetainedSize);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        memoryContext.close();
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
