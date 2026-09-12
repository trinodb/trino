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
package io.trino.operator.join.nonspilling;

import com.google.common.collect.ImmutableList;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.RuntimeConstraintWiringContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorFactory;
import io.trino.operator.join.JoinType;
import io.trino.operator.join.LookupOuterOperator.LookupOuterOperatorFactory;
import io.trino.operator.join.RuntimeConstraintComparison;
import io.trino.operator.join.nonspilling.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.join.JoinType.FULL_OUTER;
import static io.trino.operator.join.JoinType.INNER;
import static io.trino.operator.join.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperatorFactory
        implements JoinOperatorFactory, WorkProcessorOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildOutputTypes;
    private final JoinType joinType;
    private final boolean outputSingleMatch;
    private final boolean waitForBuild;
    private final JoinProbeFactory joinProbeFactory;
    private final List<Integer> probeJoinChannels;
    private final List<RuntimeConstraintComparison> runtimeConstraintComparisons;
    private final List<Integer> probeOutputChannels;
    private final Optional<OperatorFactory> outerOperatorFactory;
    private final JoinBridgeManager<? extends PartitionedLookupSourceFactory> joinBridgeManager;

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
            List<Integer> probeJoinChannels)
    {
        this(operatorId, planNodeId, lookupSourceFactoryManager, probeTypes, probeOutputTypes, buildOutputTypes, joinOperatorType, joinProbeFactory, probeJoinChannels, ImmutableList.of());
    }

    public LookupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            JoinOperatorType joinOperatorType,
            JoinProbeFactory joinProbeFactory,
            List<Integer> probeJoinChannels,
            List<RuntimeConstraintComparison> runtimeConstraintComparisons)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.joinType = requireNonNull(joinOperatorType.getType(), "joinType is null");
        this.outputSingleMatch = joinOperatorType.isOutputSingleMatch();
        this.waitForBuild = joinOperatorType.isWaitForBuild();
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.probeJoinChannels = ImmutableList.copyOf(requireNonNull(probeJoinChannels, "probeJoinChannels is null"));
        this.runtimeConstraintComparisons = ImmutableList.copyOf(requireNonNull(runtimeConstraintComparisons, "runtimeConstraintComparisons is null"));
        this.probeOutputChannels = ImmutableList.copyOf(joinProbeFactory.getOutputChannels());

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
        probeJoinChannels = other.probeJoinChannels;
        runtimeConstraintComparisons = other.runtimeConstraintComparisons;
        probeOutputChannels = other.probeOutputChannels;
        outerOperatorFactory = other.outerOperatorFactory;
        joinBridgeManager = other.joinBridgeManager;

        closed = false;
        joinBridgeManager.incrementProbeFactoryCount();
    }

    @Override
    public Optional<OperatorFactory> createOuterOperatorFactory()
    {
        return outerOperatorFactory;
    }

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
    public WorkProcessorOperator create(OperatorContext operatorContext, WorkProcessor<Page> sourcePages)
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
                joinBridgeManager::probeOperatorClosed,
                operatorContext,
                sourcePages);
    }

    @Override
    public List<RuntimeConstraintRequest> getInputRuntimeConstraints()
    {
        if (joinType == PROBE_OUTER || joinType == FULL_OUTER) {
            return ImmutableList.of();
        }
        return Stream.concat(
                        IntStream.range(0, probeJoinChannels.size())
                                .mapToObj(index -> new RuntimeConstraintRequest(
                                        RuntimeConstraintRequest.joinConstraintId(planNodeId, index),
                                        probeJoinChannels.get(index),
                                        ComparisonOperator.EQUAL,
                                        false,
                                        probeTypes.get(probeJoinChannels.get(index)))),
                        IntStream.range(0, runtimeConstraintComparisons.size())
                                .mapToObj(index -> {
                                    RuntimeConstraintComparison comparison = runtimeConstraintComparisons.get(index);
                                    return new RuntimeConstraintRequest(
                                            RuntimeConstraintRequest.joinConstraintId(planNodeId, probeJoinChannels.size() + index),
                                            comparison.probeChannel(),
                                            comparison.operator(),
                                            comparison.nullAllowed(),
                                            comparison.probeType());
                                }))
                .collect(toImmutableList());
    }

    @Override
    public void propagateRuntimeConstraint(
            RuntimeConstraintRequest request,
            Consumer<RuntimeConstraintRequest> input,
            RuntimeConstraintWiringContext context)
    {
        if (joinType == INNER && request.isConstraint() &&
                request.channelsMatch(channel -> channel >= probeOutputChannels.size() && channel < probeOutputChannels.size() + buildOutputTypes.size())) {
            context.bindLocalSource(joinBridgeManager, request.mapChannels(channel -> channel - probeOutputChannels.size()));
            return;
        }
        if ((joinType != INNER && joinType != PROBE_OUTER) || !request.channelsMatch(channel -> channel < probeOutputChannels.size())) {
            context.stop(getOperatorType(), request);
            return;
        }
        input.accept(request.mapChannels(probeOutputChannels::get));
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
