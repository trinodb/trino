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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.PartialSortApplicationResult;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LastNNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isPushPartialTopNIntoTableScan;
import static io.trino.sql.planner.plan.Patterns.TopN.step;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Math.toIntExact;

/**
 * This rule rewrites the partial TopNNode to LimitNode or LastNNode.
 *
 * <p>
 * Transforms:
 * <pre> {@code
 *    Top[partial]
 *      Project
 *        Filter
 *          TableScan
 *        }
 * </pre>
 * into:
 * <pre> {@code
 *    Limit / LastN
 *      Project
 *        Filter
 *         TableScan
 *    }
 *  </pre>
 *
 */
public class PushPartialTopNIntoTableScan
        implements Rule<ExchangeNode>
{
    private static final Capture<TopNNode> TOP_N_NODE = Capture.newCapture();
    private final Metadata metadata;

    // We need to ensure that the source node of TopN(partial) is either a Project, a Filter, or a TableScan.
    // In addition, if there are nodes other than TableScan, Project, Filter when traversing from TopN(partial),
    // then the optimization can not apply to the logical plan.
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getScope() == ExchangeNode.Scope.REMOTE && exchange.getType() == ExchangeNode.Type.GATHER)
            .with(source().matching(
                    Pattern.typeOf(TopNNode.class).capturedAs(TOP_N_NODE).with(step().equalTo(TopNNode.Step.PARTIAL))
                            .with(source().matching(
                                    planNode -> planNode instanceof TableScanNode || planNode instanceof FilterNode || planNode instanceof ProjectNode))));

    public PushPartialTopNIntoTableScan(PlannerContext plannerContext)
    {
        this.metadata = plannerContext.getMetadata();
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushPartialTopNIntoTableScan(session);
    }

    @Override
    public Result apply(ExchangeNode node, Captures captures, Context context)
    {
        TopNNode topNNode = captures.get(TOP_N_NODE);
        Optional<TableScanNode> tableScanNode = findTableScanNode(node, context);
        if (tableScanNode.isEmpty()) {
            return Result.empty();
        }
        Session session = context.getSession();
        TableHandle table = tableScanNode.get().getTable();
        Visitor visitor = new Visitor(context.getLookup());
        Boolean accept = topNNode.accept(visitor, null);
        if (!accept) {
            return Result.empty();
        }
        Optional<PartialSortApplicationResult<TableHandle>> result = metadata.applyPartialSort(session, table, visitor.getColumnHandleSortOrderMap());
        if (result.isEmpty() || !result.get().allSorted()) {
            return Result.empty();
        }
        PartialSortApplicationResult<TableHandle> partialSortResult = result.get();
        TableHandle newTable = partialSortResult.getHandle();
        PlanNode source = topNNode.getSource();

        // Support the following scenarios
        // Read ASC NULL LAST, write ASC NULL LAST or DESC NULL FIRST
        // Read ASC NULL FIRST, write ASC NULL FIRST or DESC NULL LAST
        // Read DESC NULL LAST, write DESC NULL LAST or ASC NULL FIRST
        // Read DESC NULL FIRST, write DESC NULL FIRST or ASC NULL LAST
        if (partialSortResult.isSameSortDirection() && partialSortResult.isSameNullOrdering()) {
            LimitNode limitNode = new LimitNode(
                    topNNode.getId(),
                    source.accept(new ReplaceTableScans(context.getLookup()), newTable),
                    topNNode.getCount(),
                    Optional.empty(),
                    true,
                    ImmutableList.of());
            return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(limitNode)));
        }
        if (!partialSortResult.isSameSortDirection() && !partialSortResult.isSameNullOrdering()) {
            LastNNode lastNNode = new LastNNode(
                    topNNode.getId(),
                    source.accept(new ReplaceTableScans(context.getLookup()), newTable),
                    toIntExact(topNNode.getCount()));
            return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(lastNNode)));
        }
        return Result.empty();
    }

    private static class ReplaceTableScans
            extends PlanVisitor<PlanNode, TableHandle>
    {
        private final Lookup lookup;

        public ReplaceTableScans(Lookup lookup)
        {
            this.lookup = lookup;
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, TableHandle context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, TableHandle context)
        {
            return node.replaceChildren(Collections.singletonList(node.getSource().accept(this, context)));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, TableHandle context)
        {
            return node.replaceChildren(Collections.singletonList(node.getSource().accept(this, context)));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, TableHandle newTableHandle)
        {
            return new TableScanNode(
                    node.getId(),
                    newTableHandle,
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getEnforcedConstraint(),
                    node.getStatistics(),
                    node.isUpdateTarget(),
                    node.getUseConnectorNodePartitioning());
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, TableHandle context)
        {
            return node;
        }
    }

    private static class Visitor
            extends PlanVisitor<Boolean, Void>
    {
        private final Lookup lookup;

        private Map<Symbol, ColumnHandle> assignments;

        private final Map<ColumnHandle, SortOrder> columnHandleSortOrderMap;

        private Visitor(Lookup lookup)
        {
            this.lookup = lookup;
            this.assignments = new HashMap<>();
            this.columnHandleSortOrderMap = new HashMap<>();
        }

        public Map<ColumnHandle, SortOrder> getColumnHandleSortOrderMap()
        {
            return columnHandleSortOrderMap;
        }

        @Override
        public Boolean visitTopN(TopNNode node, Void context)
        {
            if (node.getOrderingScheme().getOrderBy().size() != 1 || !node.getSource().accept(this, context)) {
                return false;
            }
            Symbol symbol = node.getOrderingScheme().getOrderBy().get(0);
            if (!assignments.containsKey(symbol)) {
                return false;
            }
            SortOrder ordering = node.getOrderingScheme().getOrdering(symbol);
            columnHandleSortOrderMap.put(assignments.get(symbol), ordering);
            return true;
        }

        @Override
        public Boolean visitTableScan(TableScanNode node, Void context)
        {
            this.assignments = node.getAssignments();
            return true;
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Boolean visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        protected Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }
    }

    private Optional<TableScanNode> findTableScanNode(ExchangeNode node, Context context)
    {
        return PlanNodeSearcher.searchFrom(node, context.getLookup()).where(planNode -> planNode instanceof TableScanNode).findFirst();
    }
}
