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
package io.trino.sql.planner;

import io.trino.metadata.Metadata;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Clones plan and assigns new PlanNodeIds to the copied PlanNodes.
 * Also, replaces all symbols in the copied plan with new symbols.
 * The original and copied plans can be safely used in different
 * branches of plan.
 */
public final class PlanCopier
{
    private PlanCopier() {}

    public static NodeAndMappings copyPlan(PlanNode plan, List<Symbol> fields, Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        PlanNode copy = SimplePlanRewriter.rewriteWith(new Copier(idAllocator), plan, null);
        return new UnaliasSymbolReferences(metadata).reallocateSymbols(copy, fields, symbolAllocator);
    }

    private static class Copier
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Copier(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            throw new UnsupportedOperationException("plan copying not implemented for " + node.getClass().getSimpleName());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            return new AggregationNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getAggregations(), node.getGroupingSets(), node.getPreGroupedSymbols(), node.getStep(), node.getHashSymbol(), node.getGroupIdSymbol(), node.getUseRawInputSymbol());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return new FilterNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getPredicate());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            return new ProjectNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getAssignments());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            return new TopNNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getCount(), node.getOrderingScheme(), node.getStep());
        }

        @Override
        public PlanNode visitOffset(OffsetNode node, RewriteContext<Void> context)
        {
            return new OffsetNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getCount());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            return new LimitNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getCount(), node.getTiesResolvingScheme(), node.isPartial(), node.getPreSortedInputs());
        }

        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<Void> context)
        {
            return new SampleNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getSampleRatio(), node.getSampleType());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return new TableScanNode(
                    idAllocator.getNextId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getEnforcedConstraint(),
                    node.getStatistics(),
                    node.isUpdateTarget(),
                    node.getUseConnectorNodePartitioning());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), node.getRowCount(), node.getRows());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            return new JoinNode(
                    idAllocator.getNextId(),
                    node.getType(),
                    context.rewrite(node.getLeft()),
                    context.rewrite(node.getRight()),
                    node.getCriteria(),
                    node.getLeftOutputSymbols(),
                    node.getRightOutputSymbols(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            return new SortNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getOrderingScheme(), node.isPartial());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            return new WindowNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getSpecification(), node.getWindowFunctions(), node.getHashSymbol(), node.getPrePartitionedInputs(), node.getPreSortedOrderPrefix());
        }

        @Override
        public PlanNode visitPatternRecognition(PatternRecognitionNode node, RewriteContext<Void> context)
        {
            return new PatternRecognitionNode(
                    idAllocator.getNextId(),
                    context.rewrite(node.getSource()),
                    node.getSpecification(),
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix(),
                    node.getWindowFunctions(),
                    node.getMeasures(),
                    node.getCommonBaseFrame(),
                    node.getRowsPerMatch(),
                    node.getSkipToLabel(),
                    node.getSkipToPosition(),
                    node.isInitial(),
                    node.getPattern(),
                    node.getSubsets(),
                    node.getVariableDefinitions());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            List<PlanNode> copiedSources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());
            return new UnionNode(idAllocator.getNextId(), copiedSources, node.getSymbolMapping(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Void> context)
        {
            List<PlanNode> copiedSources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());
            return new IntersectNode(idAllocator.getNextId(), copiedSources, node.getSymbolMapping(), node.getOutputSymbols(), node.isDistinct());
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<Void> context)
        {
            List<PlanNode> copiedSources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());
            return new ExceptNode(idAllocator.getNextId(), copiedSources, node.getSymbolMapping(), node.getOutputSymbols(), node.isDistinct());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Void> context)
        {
            return new UnnestNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getReplicateSymbols(), node.getMappings(), node.getOrdinalitySymbol(), node.getJoinType(), node.getFilter());
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Void> context)
        {
            return new GroupIdNode(idAllocator.getNextId(), context.rewrite(node.getSource()), node.getGroupingSets(), node.getGroupingColumns(), node.getAggregationArguments(), node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitEnforceSingleRow(EnforceSingleRowNode node, RewriteContext<Void> context)
        {
            return new EnforceSingleRowNode(idAllocator.getNextId(), context.rewrite(node.getSource()));
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            return new ApplyNode(idAllocator.getNextId(), context.rewrite(node.getInput()), context.rewrite(node.getSubquery()), node.getSubqueryAssignments(), node.getCorrelation(), node.getOriginSubquery());
        }

        @Override
        public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, RewriteContext<Void> context)
        {
            return new CorrelatedJoinNode(idAllocator.getNextId(), context.rewrite(node.getInput()), context.rewrite(node.getSubquery()), node.getCorrelation(), node.getType(), node.getFilter(), node.getOriginSubquery());
        }
    }
}
