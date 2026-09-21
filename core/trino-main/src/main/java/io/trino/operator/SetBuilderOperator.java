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
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.operator.ChannelSet.ChannelSetBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.LocalRuntimeConstraintConsumer;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SetBuilderOperator
        implements Operator
{
    public static class SetSupplier
    {
        private final Type type;
        private final SettableFuture<ChannelSet> channelSetFuture = SettableFuture.create();
        private volatile boolean runtimeConstraintEnabled;

        public SetSupplier(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        public Type getType()
        {
            return type;
        }

        public ListenableFuture<ChannelSet> getChannelSet()
        {
            return channelSetFuture;
        }

        void setChannelSet(ChannelSet channelSet)
        {
            boolean wasSet = channelSetFuture.set(requireNonNull(channelSet, "channelSet is null"));
            checkState(wasSet, "ChannelSet already set");
        }

        public void enableRuntimeConstraint()
        {
            runtimeConstraintEnabled = true;
        }

        public boolean isRuntimeConstraintEnabled()
        {
            return runtimeConstraintEnabled;
        }
    }

    public static class SetBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SetSupplier setProvider;
        private final int setChannel;
        private final int expectedPositions;
        private boolean closed;
        private final JoinCompiler joinCompiler;
        private final TypeOperators typeOperators;
        private final RuntimeConstraintCollectionLimits runtimeConstraintLimits;
        private final DistributedCompletionPolicy runtimeConstraintCompletionPolicy;
        private final AtomicReference<TaskRuntimeConstraintManager> runtimeConstraintManager = new AtomicReference<>();
        private LocalRuntimeConstraintConsumer runtimeConstraintConsumer;
        private int operatorCount;
        private boolean runtimeConstraintCollectionRelocated;

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Type type,
                int setChannel,
                int expectedPositions,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators)
        {
            this(operatorId,
                    planNodeId,
                    type,
                    setChannel,
                    expectedPositions,
                    joinCompiler,
                    typeOperators,
                    new RuntimeConstraintCollectionLimits(50_000, DataSize.of(4, Unit.MEGABYTE), 100_000, DataSize.of(5, Unit.MEGABYTE)),
                    DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
        }

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Type type,
                int setChannel,
                int expectedPositions,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators,
                RuntimeConstraintCollectionLimits runtimeConstraintLimits)
        {
            this(operatorId, planNodeId, type, setChannel, expectedPositions, joinCompiler, typeOperators, runtimeConstraintLimits, DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
        }

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Type type,
                int setChannel,
                int expectedPositions,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators,
                RuntimeConstraintCollectionLimits runtimeConstraintLimits,
                DistributedCompletionPolicy runtimeConstraintCompletionPolicy)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            checkArgument(setChannel >= 0, "setChannel is negative");
            this.setProvider = new SetSupplier(requireNonNull(type, "type is null"));
            this.setChannel = setChannel;
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.typeOperators = requireNonNull(typeOperators, "blockTypeOperators is null");
            this.runtimeConstraintLimits = requireNonNull(runtimeConstraintLimits, "runtimeConstraintLimits is null");
            this.runtimeConstraintCompletionPolicy = requireNonNull(runtimeConstraintCompletionPolicy, "runtimeConstraintCompletionPolicy is null");
        }

        public SetSupplier getSetProvider()
        {
            return setProvider;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SetBuilderOperator.class.getSimpleName());
            Operator runtimeConstraintCollector = null;
            if (setProvider.isRuntimeConstraintEnabled() && !runtimeConstraintCollectionRelocated) {
                TaskRuntimeConstraintManager manager = driverContext.getPipelineContext().getTaskContext().getRuntimeConstraintManager();
                runtimeConstraintManager.compareAndSet(null, manager);
                checkState(runtimeConstraintManager.get() == manager, "runtime constraint manager changed");
                operatorCount++;
                runtimeConstraintCollector = RuntimeConstraintSourceOperator.createCollector(
                        operatorContext,
                        requireNonNull(runtimeConstraintConsumer, "runtimeConstraintConsumer is not initialized"),
                        ImmutableList.of(new RuntimeConstraintSourceOperator.Channel(setProvider.getType(), setChannel)),
                        runtimeConstraintLimits.maxDistinctValues(),
                        runtimeConstraintLimits.maxFilterSize(),
                        runtimeConstraintLimits.minMaxCollectionLimit(),
                        typeOperators);
            }
            return new SetBuilderOperator(operatorContext, setProvider, setChannel, typeOperators, runtimeConstraintCollector);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
            if (runtimeConstraintConsumer != null && operatorCount > 0) {
                runtimeConstraintConsumer.setPartitionCount(operatorCount);
            }
        }

        @Override
        public void completeRuntimeConstraintWiring(RuntimeConstraintWiringContext context)
        {
            if (runtimeConstraintCollectionRelocated) {
                return;
            }
            context.defer(() -> {
                if (!setProvider.isRuntimeConstraintEnabled()) {
                    return;
                }
                context.registerSource(
                        planNodeId,
                        ImmutableList.of(new CollectedConstraint(RuntimeConstraintRequest.semiJoinConstraintId(planNodeId), 0)),
                        ImmutableList.of(setProvider.getType()),
                        runtimeConstraintCompletionPolicy);
                runtimeConstraintConsumer = new LocalRuntimeConstraintConsumer(
                        ImmutableList.of(setChannel),
                        ImmutableList.of(setProvider.getType()),
                        payload -> runtimeConstraintManager.get().addContribution(planNodeId, payload),
                        runtimeConstraintLimits.maxSizePerOperator());
            });
        }

        @Override
        public List<RuntimeConstraintRequest> getInputRuntimeConstraints(RuntimeConstraintWiringContext context)
        {
            if (!context.isTaskRetry()) {
                return ImmutableList.of();
            }
            runtimeConstraintCollectionRelocated = true;
            return ImmutableList.of(RuntimeConstraintRequest.collection(
                    RuntimeConstraintRequest.semiJoinConstraintId(planNodeId),
                    setChannel,
                    ComparisonOperator.EQUAL,
                    false,
                    setProvider.getType(),
                    runtimeConstraintCompletionPolicy == DistributedCompletionPolicy.EQUIVALENT_REPLICAS));
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SetBuilderOperatorFactory(operatorId, planNodeId, setProvider.getType(), setChannel, expectedPositions, joinCompiler, typeOperators, runtimeConstraintLimits, runtimeConstraintCompletionPolicy);
        }
    }

    private final OperatorContext operatorContext;
    private final SetSupplier setSupplier;
    private final int setChannel;

    private final ChannelSetBuilder channelSetBuilder;
    @Nullable
    private final Operator runtimeConstraintCollector;

    private boolean finished;

    public SetBuilderOperator(
            OperatorContext operatorContext,
            SetSupplier setSupplier,
            int setChannel,
            int expectedPositions,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators)
    {
        this(operatorContext, setSupplier, setChannel, typeOperators, null);
        checkArgument(expectedPositions >= 0, "expectedPositions is negative");
        requireNonNull(joinCompiler, "joinCompiler is null");
    }

    private SetBuilderOperator(
            OperatorContext operatorContext,
            SetSupplier setSupplier,
            int setChannel,
            TypeOperators typeOperators,
            @Nullable Operator runtimeConstraintCollector)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.setSupplier = requireNonNull(setSupplier, "setSupplier is null");

        this.setChannel = setChannel;

        // Set builder has a single channel which goes in channel 0, if hash is present, add a hashBlock to channel 1
        this.channelSetBuilder = new ChannelSetBuilder(
                setSupplier.getType(),
                requireNonNull(typeOperators, "typeOperators is null"),
                operatorContext.localUserMemoryContext());
        this.runtimeConstraintCollector = runtimeConstraintCollector;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.finish();
        }
        ChannelSet channelSet = channelSetBuilder.build();
        setSupplier.setChannelSet(channelSet);
        operatorContext.recordOutput(channelSet.getEstimatedSizeInBytes(), channelSet.size());
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        // Since SetBuilderOperator doesn't produce any output, the getOutput()
        // method may never be called.
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.addInput(page);
            runtimeConstraintCollector.getOutput();
        }

        channelSetBuilder.addAll(page.getBlock(setChannel));
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
            throws Exception
    {
        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.close();
        }
    }
}
