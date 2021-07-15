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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.trino.execution.Lifespan;
import io.trino.operator.DriverContext;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperatorFactory;
import io.trino.operator.join.JoinProbe.JoinProbeFactory;
import io.trino.operator.join.LookupOuterOperator.LookupOuterOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;
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
    public enum JoinType
    {
        INNER,
        PROBE_OUTER, // the Probe is the outer side of the join
        LOOKUP_OUTER, // The LookupSource is the outer side of the join
        FULL_OUTER,
    }

    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildOutputTypes;
    private final JoinType joinType;
    private final boolean outputSingleMatch;
    private final boolean waitForBuild;
    private final JoinProbeFactory joinProbeFactory;
    private final Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult;
    private final JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;
    private final OptionalInt totalOperatorsCount;
    private final HashGenerator probeHashGenerator;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private boolean closed;

    public LookupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            boolean outputSingleMatch,
            boolean waitForBuild,
            JoinProbeFactory joinProbeFactory,
            BlockTypeOperators blockTypeOperators,
            OptionalInt totalOperatorsCount,
            List<Integer> probeJoinChannels,
            OptionalInt probeHashChannel,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.outputSingleMatch = outputSingleMatch;
        this.waitForBuild = waitForBuild;
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.joinBridgeManager = lookupSourceFactoryManager;
        joinBridgeManager.incrementProbeFactoryCount();

        if (joinType == INNER || joinType == PROBE_OUTER) {
            this.outerOperatorFactoryResult = Optional.empty();
        }
        else {
            this.outerOperatorFactoryResult = Optional.of(new OuterOperatorFactoryResult(
                    new LookupOuterOperatorFactory(
                            operatorId,
                            planNodeId,
                            probeOutputTypes,
                            buildOutputTypes,
                            lookupSourceFactoryManager),
                    lookupSourceFactoryManager.getBuildExecutionStrategy()));
        }
        this.totalOperatorsCount = requireNonNull(totalOperatorsCount, "totalOperatorsCount is null");

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

        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
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
        joinBridgeManager = other.joinBridgeManager;
        outerOperatorFactoryResult = other.outerOperatorFactoryResult;
        totalOperatorsCount = other.totalOperatorsCount;
        probeHashGenerator = other.probeHashGenerator;
        partitioningSpillerFactory = other.partitioningSpillerFactory;

        closed = false;
        joinBridgeManager.incrementProbeFactoryCount();
    }

    @Override
    public Optional<OuterOperatorFactoryResult> createOuterOperatorFactory()
    {
        return outerOperatorFactoryResult;
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

    @Override
    public void noMoreOperators(Lifespan lifespan)
    {
        lifespanFinished(lifespan);
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
        LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(processorContext.getLifespan());

        joinBridgeManager.probeOperatorCreated(processorContext.getLifespan());
        return new LookupJoinOperator(
                probeTypes,
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                waitForBuild,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.probeOperatorClosed(processorContext.getLifespan()),
                totalOperatorsCount,
                probeHashGenerator,
                partitioningSpillerFactory,
                processorContext,
                Optional.of(sourcePages));
    }

    @Override
    public AdapterWorkProcessorOperator createAdapterOperator(ProcessorContext processorContext)
    {
        checkState(!closed, "Factory is already closed");
        LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(processorContext.getLifespan());

        joinBridgeManager.probeOperatorCreated(processorContext.getLifespan());
        return new LookupJoinOperator(
                probeTypes,
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                waitForBuild,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.probeOperatorClosed(processorContext.getLifespan()),
                totalOperatorsCount,
                probeHashGenerator,
                partitioningSpillerFactory,
                processorContext,
                Optional.empty());
    }

    @Override
    public void lifespanFinished(Lifespan lifespan)
    {
        joinBridgeManager.probeOperatorFactoryClosed(lifespan);
    }

    @Override
    public void close()
    {
        checkState(!closed);
        closed = true;
        joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
    }

    @Override
    public LookupJoinOperatorFactory duplicate()
    {
        return new LookupJoinOperatorFactory(this);
    }
}
