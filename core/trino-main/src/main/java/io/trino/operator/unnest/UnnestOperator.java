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
package io.trino.operator.unnest;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class UnnestOperator
        implements Operator
{
    public static class UnnestOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Integer> replicateChannels;
        private final List<Type> replicateTypes;
        private final List<Integer> unnestChannels;
        private final List<Type> unnestTypes;
        private final boolean withOrdinality;
        private final boolean outer;
        private boolean closed;

        public UnnestOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Integer> replicateChannels, List<Type> replicateTypes, List<Integer> unnestChannels, List<Type> unnestTypes, boolean withOrdinality, boolean outer)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.replicateChannels = ImmutableList.copyOf(requireNonNull(replicateChannels, "replicateChannels is null"));
            this.replicateTypes = ImmutableList.copyOf(requireNonNull(replicateTypes, "replicateTypes is null"));
            checkArgument(replicateChannels.size() == replicateTypes.size(), "replicateChannels and replicateTypes do not match");
            this.unnestChannels = ImmutableList.copyOf(requireNonNull(unnestChannels, "unnestChannels is null"));
            this.unnestTypes = ImmutableList.copyOf(requireNonNull(unnestTypes, "unnestTypes is null"));
            checkArgument(unnestChannels.size() == unnestTypes.size(), "unnestChannels and unnestTypes do not match");
            this.withOrdinality = withOrdinality;
            this.outer = outer;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, UnnestOperator.class.getSimpleName());
            return new UnnestOperator(operatorContext, replicateChannels, replicateTypes, unnestChannels, unnestTypes, withOrdinality, outer);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new UnnestOperatorFactory(operatorId, planNodeId, replicateChannels, replicateTypes, unnestChannels, unnestTypes, withOrdinality, outer);
        }
    }

    private static final int INSTANCE_SIZE = instanceSize(UnnestOperator.class);
    private static final int MAX_ROWS_PER_BLOCK = 1000;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final List<Integer> replicateChannels;
    private final List<Type> replicateTypes;
    private final List<Integer> unnestChannels;
    private final List<Type> unnestTypes;
    private final boolean withOrdinality;
    private final boolean outer;

    private boolean finishing;
    private Page currentPage;
    private int currentPosition;

    private final List<Unnester> unnesters;
    private final List<ReplicatedBlockBuilder> replicatedBlockBuilders;
    private final int outputChannelCount;

    // Track output row count per input position for the "current" input page
    private int[] outputEntriesPerPosition = new int[0];

    // Track positions for which ordinality value should be null if "outer" is true. This helps differentiate between
    // the following cases:
    // (1) outputEntriesPerPosition[i]=1 because at least one unnester provides length 1
    // (2) outputEntriesPerPosition[i]=1 because all unnesters provide length 0 and this is an outer join
    private boolean[] ordinalityNull = new boolean[0];

    private int currentBatchOutputRowCount;

    public UnnestOperator(OperatorContext operatorContext, List<Integer> replicateChannels, List<Type> replicateTypes, List<Integer> unnestChannels, List<Type> unnestTypes, boolean withOrdinality, boolean outer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalUserMemoryContext(UnnestOperator.class.getSimpleName());

        this.replicateChannels = ImmutableList.copyOf(requireNonNull(replicateChannels, "replicateChannels is null"));
        this.replicateTypes = ImmutableList.copyOf(requireNonNull(replicateTypes, "replicateTypes is null"));
        checkArgument(replicateChannels.size() == replicateTypes.size(), "replicate channels or types has wrong size");
        this.replicatedBlockBuilders = replicateTypes.stream()
                .map(type -> new ReplicatedBlockBuilder())
                .collect(toImmutableList());

        this.unnestChannels = ImmutableList.copyOf(requireNonNull(unnestChannels, "unnestChannels is null"));
        this.unnestTypes = ImmutableList.copyOf(requireNonNull(unnestTypes, "unnestTypes is null"));
        checkArgument(unnestChannels.size() == unnestTypes.size(), "unnest channels or types has wrong size");
        this.unnesters = unnestTypes.stream()
                .map(UnnestOperator::createUnnester)
                .collect(toImmutableList());

        this.withOrdinality = withOrdinality;
        this.outer = outer;

        int unnestOutputChannelCount = unnesters.stream().mapToInt(Unnester::getChannelCount).sum();
        this.outputChannelCount = unnestOutputChannelCount + replicateTypes.size() + (withOrdinality ? 1 : 0);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && currentPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && currentPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(currentPage == null, "currentPage is not null");

        currentPage = page;
        currentPosition = 0;
        resetBlockBuilders();
        systemMemoryContext.setBytes(getRetainedSizeInBytes());
    }

    private void resetBlockBuilders()
    {
        for (int i = 0; i < replicateTypes.size(); i++) {
            Block newInputBlock = currentPage.getBlock(replicateChannels.get(i));
            replicatedBlockBuilders.get(i).resetInputBlock(newInputBlock);
        }

        int positionCount = currentPage.getPositionCount();
        outputEntriesPerPosition = ensureCapacity(outputEntriesPerPosition, positionCount, true);

        for (int i = 0; i < unnestTypes.size(); i++) {
            int inputChannel = unnestChannels.get(i);
            Block unnestChannelInputBlock = currentPage.getBlock(inputChannel);
            Unnester unnester = unnesters.get(i);
            unnester.resetInput(unnestChannelInputBlock);

            int[] lengths = unnester.getOutputEntriesPerPosition();
            for (int j = 0; j < positionCount; j++) {
                outputEntriesPerPosition[j] = max(outputEntriesPerPosition[j], lengths[j]);
            }
        }

        if (outer) {
            ordinalityNull = ensureCapacity(ordinalityNull, positionCount, true);
            for (int i = 0; i < outputEntriesPerPosition.length; i++) {
                if (outputEntriesPerPosition[i] == 0) {
                    outputEntriesPerPosition[i] = 1;
                    ordinalityNull[i] = true;
                }
            }
        }
    }

    @Override
    public Page getOutput()
    {
        if (currentPage == null) {
            return null;
        }

        if (currentPosition == currentPage.getPositionCount()) {
            currentPage = null;
            currentPosition = 0;
            return null;
        }

        int batchSize = calculateNextBatchSize();
        Block[] outputBlocks = buildOutputBlocks(batchSize);

        return new Page(outputBlocks);
    }

    private int calculateNextBatchSize()
    {
        int positionCount = currentPage.getPositionCount();

        int outputRowCount = 0;
        int position = currentPosition;

        while (position < positionCount) {
            int length = outputEntriesPerPosition[position];
            if (outputRowCount + length >= MAX_ROWS_PER_BLOCK) {
                break;
            }
            outputRowCount += length;
            position++;
        }

        // grab at least a single position
        if (position == currentPosition) {
            // currentPosition is guaranteed to be less than positionCount (i.e. within array bounds)
            // because of checks in getOutput
            currentBatchOutputRowCount = outputEntriesPerPosition[currentPosition];
            return 1;
        }

        currentBatchOutputRowCount = outputRowCount;
        return position - currentPosition;
    }

    private Block[] buildOutputBlocks(int batchSize)
    {
        Block[] outputBlocks = new Block[outputChannelCount];
        int channel = 0;

        for (int replicateIndex = 0; replicateIndex < replicateTypes.size(); replicateIndex++) {
            outputBlocks[channel++] = replicatedBlockBuilders.get(replicateIndex)
                    .buildOutputBlock(outputEntriesPerPosition, currentPosition, batchSize, currentBatchOutputRowCount);
        }

        for (int unnestIndex = 0; unnestIndex < unnesters.size(); unnestIndex++) {
            Unnester unnester = unnesters.get(unnestIndex);
            Block[] blocks = unnester.buildOutputBlocks(outputEntriesPerPosition, currentPosition, batchSize, currentBatchOutputRowCount);
            for (int j = 0; j < unnester.getChannelCount(); j++) {
                outputBlocks[channel++] = blocks[j];
            }
        }

        if (withOrdinality) {
            if (outer) {
                outputBlocks[channel] = buildOrdinalityBlockWithNulls(outputEntriesPerPosition, ordinalityNull, currentPosition, batchSize, currentBatchOutputRowCount);
            }
            else {
                outputBlocks[channel] = buildOrdinalityBlock(outputEntriesPerPosition, currentPosition, batchSize, currentBatchOutputRowCount);
            }
        }

        currentPosition += batchSize;
        return outputBlocks;
    }

    private static Block buildOrdinalityBlock(int[] outputEntriesPerPosition, int offset, int inputEntryCount, int outputEntryCount)
    {
        long[] values = new long[outputEntryCount];
        int outputPosition = 0;
        for (int i = 0; i < inputEntryCount; i++) {
            int currentOutputEntries = outputEntriesPerPosition[offset + i];
            for (int j = 1; j <= currentOutputEntries; j++) {
                values[outputPosition++] = j;
            }
        }

        return new LongArrayBlock(outputEntryCount, Optional.empty(), values);
    }

    private static Block buildOrdinalityBlockWithNulls(int[] outputEntriesPerPosition, boolean[] ordinalityNull, int offset, int inputEntryCount, int outputEntryCount)
    {
        long[] values = new long[outputEntryCount];
        boolean[] isNull = new boolean[outputEntryCount];

        int outputPosition = 0;
        for (int i = 0; i < inputEntryCount; i++) {
            if (ordinalityNull[offset + i]) {
                isNull[outputPosition++] = true;
            }
            else {
                int currentOutputEntries = outputEntriesPerPosition[offset + i];
                for (int j = 1; j <= currentOutputEntries; j++) {
                    values[outputPosition++] = j;
                }
            }
        }

        return new LongArrayBlock(outputEntryCount, Optional.of(isNull), values);
    }

    private static Unnester createUnnester(Type nestedType)
    {
        if (nestedType instanceof ArrayType) {
            Type elementType = ((ArrayType) nestedType).getElementType();

            if (elementType instanceof RowType) {
                return new ArrayOfRowsUnnester(elementType.getTypeParameters().size());
            }
            return new ArrayUnnester();
        }

        if (nestedType instanceof MapType) {
            return new MapUnnester();
        }

        throw new IllegalArgumentException("Cannot unnest type: " + nestedType);
    }

    // Does not preserve original values
    public static int[] ensureCapacity(int[] buffer, int capacity, boolean forceReset)
    {
        if (buffer == null || buffer.length < capacity) {
            return new int[capacity];
        }

        if (forceReset) {
            java.util.Arrays.fill(buffer, 0);
        }

        return buffer;
    }

    // Does not preserve original values
    public static boolean[] ensureCapacity(boolean[] buffer, int capacity, boolean forceReset)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[capacity];
        }

        if (forceReset) {
            java.util.Arrays.fill(buffer, false);
        }

        return buffer;
    }

    private long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(outputEntriesPerPosition) + currentPage.getRetainedSizeInBytes();
        for (Unnester unnester : unnesters) {
            size += unnester.getRetainedSizeInBytes();
        }
        return size;
    }
}
