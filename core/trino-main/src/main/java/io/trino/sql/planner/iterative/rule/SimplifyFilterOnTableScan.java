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
package io.trino.sql.planner.iterative.rule;

import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.Patterns.tableScan;

public final class SimplifyFilterOnTableScan
        extends FilterPushdownRule<TableScanNode>
{
    public SimplifyFilterOnTableScan(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, tableScan());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, TableScanNode node, Expression inheritedPredicate)
    {
        Expression predicate = pushdown.simplifyExpression(inheritedPredicate);

        if (!TRUE.equals(predicate)) {
            return new FilterNode(pushdown.idAllocator.getNextId(), node, predicate);
        }

        return node;
    }
}
