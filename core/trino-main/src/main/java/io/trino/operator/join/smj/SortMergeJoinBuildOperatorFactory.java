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
import io.trino.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;

public class SortMergeJoinBuildOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final SortMergeJoinBridge bridge;
    private int driverInstanceIndex;
    private boolean closed;

    public SortMergeJoinBuildOperatorFactory(int operatorId, PlanNodeId planNodeId, SortMergeJoinBridge bridge)
    {
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.bridge = bridge;
        this.driverInstanceIndex = 0;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SortMergeJoinBuildOperator.class.getSimpleName());
        return new SortMergeJoinBuildOperator(operatorContext, bridge.getBuildPages(driverInstanceIndex++));
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
