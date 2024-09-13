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
package io.trino.sql.planner.optimizations;

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.InternalNodeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.concurrent.ExecutorService;

import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Predicate.not;

public final class MaterializeTableScan
        extends AbstractMaterializeTableScan<TableScanNode>
{
    public MaterializeTableScan(
            PlannerContext plannerContext,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            InternalNodeManager nodeManager,
            ExecutorService executor)
    {
        super(plannerContext, splitManager, pageSourceManager, nodeManager, executor);
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return tableScan()
                .matching(not(TableScanNode::isUpdateTarget));
    }

    @Override
    protected Result apply(TableScanNode tableScan, Captures captures, Session session)
    {
        return materializeTable(session, tableScan, alwaysTrue())
                .map(rows -> new ValuesNode(tableScan.getId(), tableScan.getOutputSymbols(), rows))
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }
}
