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
package io.prestosql.operator.unnest;

import com.google.common.collect.ImmutableList;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class UnnestingOperator
        implements Operator
{
    public static class UnnestingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Integer> replicateChannels;
        private final List<Type> replicateTypes;
        private final List<Integer> unnestChannels;
        private final List<Type> unnestTypes;
        private final boolean withOrdinality;
        private final boolean withRightId;
        private final boolean withMarker;
        private final JoinNode.Type joinType;
        private final boolean onTrue;
        private boolean closed;
        private final AtomicLong uniqueValuePool = new AtomicLong();

        public UnnestingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Integer> replicateChannels,
                List<Type> replicateTypes,
                List<Integer> unnestChannels,
                List<Type> unnestTypes,
                boolean withOrdinality,
                boolean withRightId,
                boolean withMarker,
                JoinNode.Type joinType,
                boolean onTrue)
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
            this.withRightId = withRightId;
            this.withMarker = withMarker;
            this.joinType = requireNonNull(joinType, "joinType is null");
            this.onTrue = onTrue;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, UnnestingOperator.class.getSimpleName());
            return new UnnestingOperator(
                    operatorContext,
                    replicateChannels,
                    replicateTypes,
                    unnestChannels,
                    unnestTypes,
                    withOrdinality,
                    withRightId,
                    withMarker,
                    joinType,
                    onTrue,
                    uniqueValuePool);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new UnnestingOperator.UnnestingOperatorFactory(
                    operatorId,
                    planNodeId,
                    replicateChannels,
                    replicateTypes,
                    unnestChannels,
                    unnestTypes,
                    withOrdinality,
                    withRightId,
                    withMarker,
                    joinType,
                    onTrue);
        }
    }

    private static final int MAX_ROWS_PER_BLOCK = 1000;
    private static final int MAX_BYTES_PER_PAGE = 1024 * 1024;
    private static final long RIGHT_ID_LIMIT = 1L << 40L;

    // Output row count is checked *after* processing every input row. For this reason, estimated rows per
    // block are always going to be slightly greater than {@code maxRowsPerBlock}. Accounting for this skew
    // helps avoid array copies in blocks.
    private static final double OVERFLOW_SKEW = 1.25;
    private static final int estimatedMaxRowsPerBlock = (int) Math.ceil(MAX_ROWS_PER_BLOCK * OVERFLOW_SKEW);

    private final OperatorContext operatorContext;
    private final List<Integer> replicateChannels;
    private final List<Type> replicateTypes;
    private final List<Integer> unnestChannels;
    private final List<Type> unnestTypes;
    private final boolean withOrdinality;
    private final boolean withRightId;
    private final boolean withMarker;
    private final JoinNode.Type joinType;
    private final boolean onTrue;

    private boolean finishing;
    private Page currentPage;
    private int currentPosition;

    private final List<Unnester> unnesters;
    private final int unnestOutputChannelCount;

    private final List<ReplicatedBlockBuilder> replicatedBlockBuilders;

    private BlockBuilder ordinalityBlockBuilder;
    private BlockBuilder rightIdBlockBuilder;
    private BlockBuilder markerBlockBuilder;

    private final AtomicLong rightIdPool;
    private final long rightIdMask;

    private int outputChannelCount;

    /*
    NOTE: this operator doesn't perform `JOIN involving UNNEST` computation independently
    (except for the `onTrue` case). It prepares input for sequence of other operators
    which will further impose filter (JOIN condition) and prune helper symbols.

    Helper symbols' semantics:

    marker is used to distinguish between the three types of completing rows:
    0 for replicate values completed with nulls instead of unnested values (used in LEFT and FULL join)
    1 for unnested values completed with nulls instead of replicate values (used in RIGHT and FULL join)
    2 for replicate values joined with unnested values (used in all types of join)

    rightId is needed for RIGHT and FULL join type.
    Each of unnested rows will be doubled and completed with replicate values one time
    and with nulls instead of replicate values the other time. Both rows will have the same
    rightId which will allow to identify them further as originating from the same
    unnested row.
    The absence of rightId implies that this is a join of type LEFT or INNER.
    In such case unnested rows are not doubled.

    The absence of both rightId and marker, implies that this is a join of type INNER.

    The additional information onTrue allows to optimise results. With onTrue set to true,
    unnested rows are never doubled and null-completed rows are added only when necessary.
    */
    public UnnestingOperator(
            OperatorContext operatorContext,
            List<Integer> replicateChannels,
            List<Type> replicateTypes,
            List<Integer> unnestChannels,
            List<Type> unnestTypes,
            boolean withOrdinality,
            boolean withRightId,
            boolean withMarker,
            JoinNode.Type joinType,
            boolean onTrue,
            AtomicLong uniqueValuePool)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        this.replicateChannels = ImmutableList.copyOf(requireNonNull(replicateChannels, "replicateChannels is null"));
        this.replicateTypes = ImmutableList.copyOf(requireNonNull(replicateTypes, "replicateTypes is null"));
        checkArgument(replicateChannels.size() == replicateTypes.size(), "replicate channels or types has wrong size");
        this.joinType = requireNonNull(joinType, "joinType is null");
        if ((joinType == JoinNode.Type.RIGHT || joinType == JoinNode.Type.FULL) && !onTrue) {
            this.replicatedBlockBuilders = replicateTypes.stream()
                    .map(ReplicatedBlockBuilderAppendingNulls::new)
                    .collect(toImmutableList());
        }
        else {
            this.replicatedBlockBuilders = replicateTypes.stream()
                    .map(type -> new SimpleReplicatedBlockBuilder())
                    .collect(toImmutableList());
        }

        this.unnestChannels = ImmutableList.copyOf(requireNonNull(unnestChannels, "unnestChannels is null"));
        this.unnestTypes = ImmutableList.copyOf(requireNonNull(unnestTypes, "unnestTypes is null"));
        checkArgument(unnestChannels.size() == unnestTypes.size(), "unnest channels or types has wrong size");
        this.unnesters = unnestTypes.stream()
                .map(UnnestingOperator::createUnnester)
                .collect(toImmutableList());
        this.unnestOutputChannelCount = unnesters.stream().mapToInt(Unnester::getChannelCount).sum();

        this.withOrdinality = withOrdinality;
        this.withRightId = withRightId;
        this.withMarker = withMarker;
        this.onTrue = onTrue;

        this.rightIdPool = requireNonNull(uniqueValuePool, "uniqueValuePool is null");
        TaskId fullTaskId = operatorContext.getDriverContext().getTaskId();
        this.rightIdMask = (((long) fullTaskId.getStageId().getId()) << 54) | (((long) fullTaskId.getId()) << 40);

        this.outputChannelCount = unnestOutputChannelCount + replicateTypes.size()
                + (withOrdinality ? 1 : 0)
                + (withRightId ? 1 : 0)
                + (withMarker ? 1 : 0);
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
    }

    private void resetBlockBuilders()
    {
        for (int i = 0; i < replicateTypes.size(); i++) {
            Block newInputBlock = currentPage.getBlock(replicateChannels.get(i));
            replicatedBlockBuilders.get(i).resetInputBlock(newInputBlock);
        }

        for (int i = 0; i < unnestTypes.size(); i++) {
            int inputChannel = unnestChannels.get(i);
            Block unnestChannelInputBlock = currentPage.getBlock(inputChannel);
            unnesters.get(i).resetInput(unnestChannelInputBlock);
        }
    }

    @Override
    public Page getOutput()
    {
        if (currentPage == null) {
            return null;
        }

        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus(MAX_BYTES_PER_PAGE);
        prepareForNewOutput(pageBuilderStatus);

        int outputRowCount = 0;

        while (currentPosition < currentPage.getPositionCount()) {
            outputRowCount += processCurrentPosition();
            currentPosition++;

            if (outputRowCount >= MAX_ROWS_PER_BLOCK || pageBuilderStatus.isFull()) {
                break;
            }
        }

        Block[] outputBlocks = buildOutputBlocks();

        if (currentPosition == currentPage.getPositionCount()) {
            currentPage = null;
            currentPosition = 0;
        }

        return new Page(outputBlocks);
    }

    private int processCurrentPosition()
    {
        if (onTrue) {
            return processCurrentPositionOnTrue(joinType == JoinNode.Type.LEFT || joinType == JoinNode.Type.FULL);
        }
        if (joinType == JoinNode.Type.INNER) {
            return processCurrentPositionOnTrue(false);
        }
        if (joinType == JoinNode.Type.LEFT) {
            return processCurrentPositionLeftJoin();
        }
        if (joinType == JoinNode.Type.RIGHT) {
            return processCurrentPositionRightJoin();
        }
        return processCurrentPositionFullJoin();
    }

    private int processCurrentPositionOnTrue(boolean outer)
    {
        // Determine number of output rows for this input position
        int maxEntries = getCurrentMaxEntries();

        // For 'outer' type, in case unnesters produce no output,
        // append a single row replicating values of replicate channels
        // and appending nulls for unnested chennels
        if (outer && maxEntries == 0) {
            replicatedBlockBuilders.forEach(blockBuilder -> ((SimpleReplicatedBlockBuilder) blockBuilder).appendRepeated(currentPosition, 1));
            unnesters.forEach(unnester -> unnester.appendNulls(1));
            if (withOrdinality) {
                ordinalityBlockBuilder.appendNull();
            }
            return 1;
        }

        // Append elements repeatedly to replicate output columns
        replicatedBlockBuilders.forEach(blockBuilder -> ((SimpleReplicatedBlockBuilder) blockBuilder).appendRepeated(currentPosition, maxEntries));

        // Process this position in unnesters
        unnesters.forEach(unnester -> unnester.processCurrentAndAdvance(maxEntries, false));

        if (withOrdinality) {
            for (long ordinalityCount = 1; ordinalityCount <= maxEntries; ordinalityCount++) {
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
            }
        }

        return maxEntries;
    }

    private int processCurrentPositionLeftJoin()
    {
        // Append a row with replicate values and nulls for unnested values
        replicatedBlockBuilders.forEach(blockBuilder -> ((SimpleReplicatedBlockBuilder) blockBuilder).appendRepeated(currentPosition, 1));
        unnesters.forEach(unnester -> unnester.appendNulls(1));
        if (withOrdinality) {
            ordinalityBlockBuilder.appendNull();
        }
        BIGINT.writeLong(markerBlockBuilder, 0);

        // Determine number of output rows for this input position
        int maxEntries = getCurrentMaxEntries();

        // Append elements repeatedly to replicate output columns
        replicatedBlockBuilders.forEach(blockBuilder -> ((SimpleReplicatedBlockBuilder) blockBuilder).appendRepeated(currentPosition, maxEntries));

        // Process this position in unnesters
        unnesters.forEach(unnester -> unnester.processCurrentAndAdvance(maxEntries, false));

        if (withOrdinality) {
            for (long ordinalityCount = 1; ordinalityCount <= maxEntries; ordinalityCount++) {
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
            }
        }
        for (int i = 0; i < maxEntries; i++) {
            BIGINT.writeLong(markerBlockBuilder, 2);
        }

        return maxEntries + 1;
    }

    private int processCurrentPositionRightJoin()
    {
        // Determine number of unnested rows for this input position
        int maxEntries = getCurrentMaxEntries();

        // For each unnested row, append two rows: one with nulls for replicate values and one with actual values
        replicatedBlockBuilders.forEach(blockBuilder -> ((ReplicatedBlockBuilderAppendingNulls) blockBuilder).appendRepeatedWithNulls(currentPosition, maxEntries));
        unnesters.forEach(unnester -> unnester.processCurrentAndAdvance(maxEntries, true));
        if (withOrdinality) {
            for (long ordinalityCount = 1; ordinalityCount <= maxEntries; ordinalityCount++) {
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
            }
        }
        long rightId = rightIdPool.getAndAdd(maxEntries);
        checkState(rightId + maxEntries < RIGHT_ID_LIMIT, "rightId %s exceeds limit: %s", rightId + maxEntries, RIGHT_ID_LIMIT);
        for (int i = 0; i < maxEntries; i++) {
            BIGINT.writeLong(rightIdBlockBuilder, rightIdMask | rightId);
            BIGINT.writeLong(rightIdBlockBuilder, rightIdMask | rightId);
            rightId++;
        }
        for (int i = 0; i < maxEntries; i++) {
            BIGINT.writeLong(markerBlockBuilder, 1);
            BIGINT.writeLong(markerBlockBuilder, 2);
        }

        return maxEntries * 2;
    }

    private int processCurrentPositionFullJoin()
    {
        // Determine number of unnested rows for this input position
        int maxEntries = getCurrentMaxEntries();

        // Append a row with replicate values and nulls for unnested values
        replicatedBlockBuilders.forEach(blockBuilder -> ((ReplicatedBlockBuilderAppendingNulls) blockBuilder).appendElement(currentPosition));
        unnesters.forEach(unnester -> unnester.appendNulls(1));
        if (withOrdinality) {
            ordinalityBlockBuilder.appendNull();
        }
        long rightId = rightIdPool.getAndAdd(maxEntries + 1);
        checkState(rightId + maxEntries + 1 < RIGHT_ID_LIMIT, "rightId %s exceeds limit: %s", rightId + maxEntries + 1, RIGHT_ID_LIMIT);
        BIGINT.writeLong(rightIdBlockBuilder, rightIdMask | rightId);
        rightId++;
        BIGINT.writeLong(markerBlockBuilder, 0);

        // For each unnested row, append two rows: one with nulls for replicate values and one with actual values
        replicatedBlockBuilders.forEach(blockBuilder -> ((ReplicatedBlockBuilderAppendingNulls) blockBuilder).appendRepeatedWithNulls(currentPosition, maxEntries));
        unnesters.forEach(unnester -> unnester.processCurrentAndAdvance(maxEntries, true));
        if (withOrdinality) {
            for (long ordinalityCount = 1; ordinalityCount <= maxEntries; ordinalityCount++) {
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
            }
        }
        for (int i = 0; i < maxEntries; i++) {
            BIGINT.writeLong(rightIdBlockBuilder, rightIdMask | rightId);
            BIGINT.writeLong(rightIdBlockBuilder, rightIdMask | rightId);
            rightId++;
        }
        for (int i = 0; i < maxEntries; i++) {
            BIGINT.writeLong(markerBlockBuilder, 1);
            BIGINT.writeLong(markerBlockBuilder, 2);
        }

        return maxEntries * 2 + 1;
    }

    private int getCurrentMaxEntries()
    {
        return unnesters.stream()
                .mapToInt(Unnester::getCurrentUnnestedLength)
                .max()
                .orElse(0);
    }

    private void prepareForNewOutput(PageBuilderStatus pageBuilderStatus)
    {
        unnesters.forEach(unnester -> unnester.startNewOutput(pageBuilderStatus, estimatedMaxRowsPerBlock));
        replicatedBlockBuilders.forEach(replicatedBlockBuilder -> replicatedBlockBuilder.startNewOutput(pageBuilderStatus, estimatedMaxRowsPerBlock));

        if (withOrdinality) {
            ordinalityBlockBuilder = BIGINT.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), estimatedMaxRowsPerBlock);
        }
        if (withRightId) {
            rightIdBlockBuilder = BIGINT.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), estimatedMaxRowsPerBlock);
        }
        if (withMarker) {
            markerBlockBuilder = BIGINT.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), estimatedMaxRowsPerBlock);
        }
    }

    private Block[] buildOutputBlocks()
    {
        Block[] outputBlocks = new Block[outputChannelCount];
        int offset = 0;

        for (int replicateIndex = 0; replicateIndex < replicateTypes.size(); replicateIndex++) {
            outputBlocks[offset++] = replicatedBlockBuilders.get(replicateIndex).buildOutputAndFlush();
        }

        for (int unnestIndex = 0; unnestIndex < unnesters.size(); unnestIndex++) {
            Unnester unnester = unnesters.get(unnestIndex);
            Block[] block = unnester.buildOutputBlocksAndFlush();
            for (int j = 0; j < unnester.getChannelCount(); j++) {
                outputBlocks[offset++] = block[j];
            }
        }

        if (withOrdinality) {
            outputBlocks[offset++] = ordinalityBlockBuilder.build();
        }
        if (withRightId) {
            outputBlocks[offset++] = rightIdBlockBuilder.build();
        }
        if (withMarker) {
            outputBlocks[offset++] = markerBlockBuilder.build();
        }

        return outputBlocks;
    }

    private static Unnester createUnnester(Type nestedType)
    {
        if (nestedType instanceof ArrayType) {
            Type elementType = ((ArrayType) nestedType).getElementType();

            if (elementType instanceof RowType) {
                return new ArrayOfRowsUnnester(((RowType) elementType));
            }
            else {
                return new ArrayUnnester(elementType);
            }
        }
        else if (nestedType instanceof MapType) {
            Type keyType = ((MapType) nestedType).getKeyType();
            Type valueType = ((MapType) nestedType).getValueType();
            return new MapUnnester(keyType, valueType);
        }
        else {
            throw new IllegalArgumentException("Cannot unnest type: " + nestedType);
        }
    }
}
