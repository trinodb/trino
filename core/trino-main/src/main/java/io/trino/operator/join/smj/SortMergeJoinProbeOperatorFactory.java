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
package io.trino.operator.join.smj;

import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.type.Type;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SortMergeJoinProbeOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final JoinNode.Type joinType;
    private final SortMergeJoinBridge bridge;
    private final BlockTypeOperators blockTypeOperators;
    private final List<Type> probeTypes;
    private final List<Integer> probeEquiJoinClauseChannels;
    private final List<Integer> probeOutputChannels;
    private final List<Type> buildTypes;
    private final List<Integer> buildEquiJoinClauseChannels;
    private final List<Integer> buildOutputChannels;
    private final Optional<SpillerFactory> spillerFactory;
    private final Integer maxBufferPageCount;

    private final Integer numRowsInMemoryBufferThreshold;
    private int driverInstanceIndex;
    private boolean closed;

    public SortMergeJoinProbeOperatorFactory(Integer maxBufferPageCount, Integer numRowsInMemoryBufferThreshold, Optional<SpillerFactory> spillerFactory, int operatorId, PlanNodeId planNodeId,
            JoinNode.Type joinType, SortMergeJoinBridge bridge, BlockTypeOperators blockTypeOperators,
            List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels,
            List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        this.maxBufferPageCount = maxBufferPageCount;
        this.numRowsInMemoryBufferThreshold = numRowsInMemoryBufferThreshold;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.joinType = joinType;
        this.bridge = bridge;
        this.blockTypeOperators = blockTypeOperators;
        this.probeTypes = probeTypes;
        this.probeEquiJoinClauseChannels = probeEquiJoinClauseChannels;
        this.probeOutputChannels = probeOutputChannels;
        this.buildTypes = buildTypes;
        this.buildEquiJoinClauseChannels = buildEquiJoinClauseChannels;
        this.buildOutputChannels = buildOutputChannels;
        this.driverInstanceIndex = 0;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SortMergeJoinProbeOperator.class.getSimpleName());
        return new SortMergeJoinProbeOperator(driverInstanceIndex++, maxBufferPageCount, numRowsInMemoryBufferThreshold, spillerFactory, operatorContext, joinType, bridge, blockTypeOperators, probeTypes, probeEquiJoinClauseChannels, probeOutputChannels,
                buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
    }

    @Override
    public void noMoreOperators()
    {
        closed = true;
    }

    @Override
    public OperatorFactory duplicate()
    {
        return null;
    }
}
