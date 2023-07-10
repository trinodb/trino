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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import io.trino.operator.DriverContext;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactories.JoinOperatorType;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorFactory;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.LookupOuterOperator.LookupOuterOperatorFactory;
import io.trino.operator.join.unspilled.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.INNER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperatorFactory
        implements JoinOperatorFactory, AdapterWorkProcessorOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildOutputTypes;
    private final JoinType joinType;
    private final boolean outputSingleMatch;
    private final boolean waitForBuild;
    private final JoinProbeFactory joinProbeFactory;
    private final Optional<OperatorFactory> outerOperatorFactory;
    private final JoinBridgeManager<? extends PartitionedLookupSourceFactory> joinBridgeManager;
    private final HashGenerator probeHashGenerator;

    private boolean closed;

    public LookupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            JoinOperatorType joinOperatorType,
            JoinProbeFactory joinProbeFactory,
            BlockTypeOperators blockTypeOperators,
            List<Integer> probeJoinChannels,
            OptionalInt probeHashChannel)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.joinType = requireNonNull(joinOperatorType.getType(), "joinType is null");
        this.outputSingleMatch = joinOperatorType.isOutputSingleMatch();
        this.waitForBuild = joinOperatorType.isWaitForBuild();
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.joinBridgeManager = lookupSourceFactoryManager;
        joinBridgeManager.incrementProbeFactoryCount();

        if (joinType == INNER || joinType == PROBE_OUTER) {
            this.outerOperatorFactory = Optional.empty();
        }
        else {
            this.outerOperatorFactory = Optional.of(new LookupOuterOperatorFactory(
                    operatorId,
                    planNodeId,
                    probeOutputTypes,
                    buildOutputTypes,
                    lookupSourceFactoryManager));
        }

        requireNonNull(probeHashChannel, "probeHashChannel is null");
        if (probeHashChannel.isPresent()) {
            this.probeHashGenerator = new PrecomputedHashGenerator(probeHashChannel.getAsInt());
        }
        else {
            requireNonNull(probeJoinChannels, "probeJoinChannels is null");
            List<Type> hashTypes = probeJoinChannels.stream()
                    .map(probeTypes::get)
                    .collect(toImmutableList());
            this.probeHashGenerator = new InterpretedHashGenerator(hashTypes, probeJoinChannels, blockTypeOperators);
        }
    }

    private LookupJoinOperatorFactory(LookupJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        checkArgument(!other.closed, "cannot duplicated closed OperatorFactory");

        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeTypes = other.probeTypes;
        buildOutputTypes = other.buildOutputTypes;
        joinType = other.joinType;
        outputSingleMatch = other.outputSingleMatch;
        waitForBuild = other.waitForBuild;
        joinProbeFactory = other.joinProbeFactory;
        outerOperatorFactory = other.outerOperatorFactory;
        joinBridgeManager = other.joinBridgeManager;
        probeHashGenerator = other.probeHashGenerator;

        closed = false;
        joinBridgeManager.incrementProbeFactoryCount();
    }

    @Override
    public Optional<OperatorFactory> createOuterOperatorFactory()
    {
        return outerOperatorFactory;
    }

    // Methods from OperatorFactory

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        OperatorContext operatorContext = driverContext.addOperatorContext(getOperatorId(), getPlanNodeId(), getOperatorType());
        return new WorkProcessorOperatorAdapter(operatorContext, this);
    }

    @Override
    public void noMoreOperators()
    {
        close();
    }

    // Methods from AdapterWorkProcessorOperatorFactory

    @Override
    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @Override
    public String getOperatorType()
    {
        return LookupJoinOperator.class.getSimpleName();
    }

    @Override
    public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
    {
        checkState(!closed, "Factory is already closed");
        PartitionedLookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge();

        joinBridgeManager.probeOperatorCreated();
        return new LookupJoinOperator(
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                waitForBuild,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.probeOperatorClosed(),
                processorContext,
                Optional.of(sourcePages));
    }

    @Override
    public AdapterWorkProcessorOperator createAdapterOperator(ProcessorContext processorContext)
    {
        checkState(!closed, "Factory is already closed");
        PartitionedLookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge();

        joinBridgeManager.probeOperatorCreated();
        return new LookupJoinOperator(
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                waitForBuild,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.probeOperatorClosed(),
                processorContext,
                Optional.empty());
    }

    @Override
    public void close()
    {
        joinBridgeManager.probeOperatorFactoryClosed();
        checkState(!closed);
        closed = true;
    }

    @Override
    public LookupJoinOperatorFactory duplicate()
    {
        return new LookupJoinOperatorFactory(this);
    }
}
