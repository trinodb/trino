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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.LayoutConstraintEvaluator;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.sql.ir.IrUtils.filterConjuncts;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Predicate.not;

public final class MaterializeFilteredTableScan
        extends AbstractMaterializeTableScan<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(
                    tableScan().capturedAs(TABLE_SCAN)
                            .matching(not(TableScanNode::isUpdateTarget))));

    public MaterializeFilteredTableScan(
            PlannerContext plannerContext,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            InternalNodeManager nodeManager,
            ExecutorService executor)
    {
        super(plannerContext, splitManager, pageSourceManager, nodeManager, executor);
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Result apply(FilterNode filter, Captures captures, Session session)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Constraint constraint = Optional.of(filter.getPredicate())
                .map(predicate -> filterConjuncts(predicate, expression -> !DynamicFilters.isDynamicFilter(expression)))
                .map(predicate -> new LayoutConstraintEvaluator(plannerContext, session, tableScan.getAssignments(), predicate))
                .map(evaluator -> new Constraint(TupleDomain.all(), evaluator::isCandidate, evaluator.getArguments()))
                .orElse(alwaysTrue());

        return materializeTable(session, tableScan, constraint)
                .map(rows -> new ValuesNode(tableScan.getId(), tableScan.getOutputSymbols(), rows))
                .map(values -> filter.replaceChildren(ImmutableList.of(values)))
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }
}
