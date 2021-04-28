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

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static java.util.Objects.requireNonNull;

public class EnforceSingleRowOperator
        implements Operator
{
    public static class EnforceSingleRowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Page nullValuesPage;

        private boolean closed;

        public EnforceSingleRowOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> types)
        {
            this(operatorId, planNodeId, makeNullValuesPage(types));
        }

        private EnforceSingleRowOperatorFactory(int operatorId, PlanNodeId planNodeId, Page nullValuesPage)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nullValuesPage = requireNonNull(nullValuesPage, "nullValuesPage is null");
        }

        private static Page makeNullValuesPage(List<Type> types)
        {
            Block[] columns = new Block[types.size()];
            for (int i = 0; i < types.size(); i++) {
                columns[i] = types.get(i).createBlockBuilder(null, 1)
                        .appendNull()
                        .build();
            }
            return new Page(1, columns);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, EnforceSingleRowOperator.class.getSimpleName());
            return new EnforceSingleRowOperator(operatorContext, nullValuesPage);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new EnforceSingleRowOperatorFactory(operatorId, planNodeId, nullValuesPage);
        }
    }

    private final OperatorContext operatorContext;
    private final Page nullValuePage;
    private boolean finishing;
    private Page page;

    public EnforceSingleRowOperator(OperatorContext operatorContext, Page nullValuePage)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nullValuePage = requireNonNull(nullValuePage, "nullValuePage is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (!finishing && page == null) {
            this.page = nullValuePage;
        }
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && page == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator did not expect any more data");
        if (page.getPositionCount() == 0) {
            return;
        }
        if (this.page != null || page.getPositionCount() > 1) {
            throw new TrinoException(SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows");
        }
        this.page = page;
    }

    @Override
    public Page getOutput()
    {
        if (!finishing) {
            return null;
        }
        checkState(page != null, "Operator is already done");

        Page pageToReturn = page;
        page = null;
        return pageToReturn;
    }
}
