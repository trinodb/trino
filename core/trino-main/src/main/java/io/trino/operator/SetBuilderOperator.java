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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.operator.ChannelSet.ChannelSetBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;

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
    }

    public static class SetBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Optional<Integer> hashChannel;
        private final SetSupplier setProvider;
        private final int setChannel;
        private final int expectedPositions;
        private boolean closed;
        private final JoinCompiler joinCompiler;
        private final TypeOperators typeOperators;

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Type type,
                int setChannel,
                Optional<Integer> hashChannel,
                int expectedPositions,
                JoinCompiler joinCompiler,
                TypeOperators typeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            checkArgument(setChannel >= 0, "setChannel is negative");
            this.setProvider = new SetSupplier(requireNonNull(type, "type is null"));
            this.setChannel = setChannel;
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.typeOperators = requireNonNull(typeOperators, "blockTypeOperators is null");
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
            return new SetBuilderOperator(operatorContext, setProvider, setChannel, hashChannel, expectedPositions, joinCompiler, typeOperators);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SetBuilderOperatorFactory(operatorId, planNodeId, setProvider.getType(), setChannel, hashChannel, expectedPositions, joinCompiler, typeOperators);
        }
    }

    private final OperatorContext operatorContext;
    private final SetSupplier setSupplier;
    private final int setChannel;
    private final int hashChannel;

    private final ChannelSetBuilder channelSetBuilder;

    private boolean finished;

    public SetBuilderOperator(
            OperatorContext operatorContext,
            SetSupplier setSupplier,
            int setChannel,
            Optional<Integer> hashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.setSupplier = requireNonNull(setSupplier, "setSupplier is null");

        this.setChannel = setChannel;
        this.hashChannel = hashChannel.orElse(-1);

        // Set builder has a single channel which goes in channel 0, if hash is present, add a hashBlock to channel 1
        this.channelSetBuilder = new ChannelSetBuilder(
                setSupplier.getType(),
                requireNonNull(typeOperators, "typeOperators is null"), operatorContext.localUserMemoryContext());
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

        channelSetBuilder.addAll(page.getBlock(setChannel), hashChannel == -1 ? null : page.getBlock(hashChannel));
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
