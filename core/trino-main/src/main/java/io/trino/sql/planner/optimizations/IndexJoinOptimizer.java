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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.ResolvedIndex;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Function;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class IndexJoinOptimizer
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;

    public IndexJoinOptimizer(PlannerContext plannerContext)
    {
        this.plannerContext = plannerContext;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        requireNonNull(plan, "plan is null");
        return SimplePlanRewriter.rewriteWith(new Rewriter(context.idAllocator(), plannerContext, context.session()), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final PlannerContext plannerContext;
        private final Session session;

        private Rewriter(
                PlanNodeIdAllocator idAllocator,
                PlannerContext plannerContext,
                Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft());
            PlanNode rightRewritten = context.rewrite(node.getRight());

            if (!node.getCriteria().isEmpty()) { // Index join only possible with JOIN criteria
                List<Symbol> leftJoinSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
                List<Symbol> rightJoinSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

                Optional<PlanNode> leftIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        leftRewritten,
                        ImmutableSet.copyOf(leftJoinSymbols),
                        idAllocator,
                        plannerContext,
                        session);
                if (leftIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<Symbol, Symbol> trace = IndexKeyTracer.trace(leftIndexCandidate.get(), ImmutableSet.copyOf(leftJoinSymbols));
                    checkState(!trace.isEmpty());
                }

                Optional<PlanNode> rightIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        rightRewritten,
                        ImmutableSet.copyOf(rightJoinSymbols),
                        idAllocator,
                        plannerContext,
                        session);
                if (rightIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<Symbol, Symbol> trace = IndexKeyTracer.trace(rightIndexCandidate.get(), ImmutableSet.copyOf(rightJoinSymbols));
                    checkState(!trace.isEmpty());
                }

                switch (node.getType()) {
                    case INNER:
                        // Prefer the right candidate over the left candidate
                        PlanNode indexJoinNode = null;
                        if (rightIndexCandidate.isPresent()) {
                            indexJoinNode = new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinSymbols, rightJoinSymbols), Optional.empty(), Optional.empty());
                        }
                        else if (leftIndexCandidate.isPresent()) {
                            indexJoinNode = new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinSymbols, leftJoinSymbols), Optional.empty(), Optional.empty());
                        }

                        if (indexJoinNode != null) {
                            if (node.getFilter().isPresent()) {
                                indexJoinNode = new FilterNode(idAllocator.getNextId(), indexJoinNode, node.getFilter().get());
                            }

                            if (!indexJoinNode.getOutputSymbols().equals(node.getOutputSymbols())) {
                                indexJoinNode = new ProjectNode(
                                        idAllocator.getNextId(),
                                        indexJoinNode,
                                        Assignments.identity(node.getOutputSymbols()));
                            }

                            return indexJoinNode;
                        }
                        break;

                    case LEFT:
                        // We cannot use indices for outer joins until index join supports in-line filtering
                        if (node.getFilter().isEmpty() && rightIndexCandidate.isPresent()) {
                            return createIndexJoinWithExpectedOutputs(node.getOutputSymbols(), IndexJoinNode.Type.SOURCE_OUTER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinSymbols, rightJoinSymbols), idAllocator);
                        }
                        break;

                    case RIGHT:
                        // We cannot use indices for outer joins until index join supports in-line filtering
                        if (node.getFilter().isEmpty() && leftIndexCandidate.isPresent()) {
                            return createIndexJoinWithExpectedOutputs(node.getOutputSymbols(), IndexJoinNode.Type.SOURCE_OUTER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinSymbols, leftJoinSymbols), idAllocator);
                        }
                        break;

                    case FULL:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown type: " + node.getType());
                }
            }

            if (leftRewritten != node.getLeft() || rightRewritten != node.getRight()) {
                return new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftRewritten,
                        rightRewritten,
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
            return node;
        }

        private static PlanNode createIndexJoinWithExpectedOutputs(List<Symbol> expectedOutputs, IndexJoinNode.Type type, PlanNode probe, PlanNode index, List<IndexJoinNode.EquiJoinClause> equiJoinClause, PlanNodeIdAllocator idAllocator)
        {
            PlanNode result = new IndexJoinNode(idAllocator.getNextId(), type, probe, index, equiJoinClause, Optional.empty(), Optional.empty());
            if (!result.getOutputSymbols().equals(expectedOutputs)) {
                result = new ProjectNode(
                        idAllocator.getNextId(),
                        result,
                        Assignments.identity(expectedOutputs));
            }
            return result;
        }

        private static List<IndexJoinNode.EquiJoinClause> createEquiJoinClause(List<Symbol> probeSymbols, List<Symbol> indexSymbols)
        {
            checkArgument(probeSymbols.size() == indexSymbols.size());
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (int i = 0; i < probeSymbols.size(); i++) {
                builder.add(new IndexJoinNode.EquiJoinClause(probeSymbols.get(i), indexSymbols.get(i)));
            }
            return builder.build();
        }
    }

    /**
     * Tries to rewrite a PlanNode tree with an IndexSource instead of a TableScan
     */
    private static class IndexSourceRewriter
            extends SimplePlanRewriter<IndexSourceRewriter.Context>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final PlannerContext plannerContext;
        private final Session session;
        private final DomainTranslator domainTranslator;

        private IndexSourceRewriter(
                PlanNodeIdAllocator idAllocator,
                PlannerContext plannerContext,
                Session session)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.session = requireNonNull(session, "session is null");
            this.domainTranslator = new DomainTranslator(plannerContext.getMetadata());
        }

        public static Optional<PlanNode> rewriteWithIndex(
                PlanNode planNode,
                Set<Symbol> lookupSymbols,
                PlanNodeIdAllocator idAllocator,
                PlannerContext plannerContext,
                Session session)
        {
            AtomicBoolean success = new AtomicBoolean();
            IndexSourceRewriter indexSourceRewriter = new IndexSourceRewriter(idAllocator, plannerContext, session);
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(indexSourceRewriter, planNode, new Context(lookupSymbols, success));
            if (success.get()) {
                return Optional.of(rewritten);
            }
            return Optional.empty();
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Context> context)
        {
            // We don't know how to process this PlanNode in the context of an IndexJoin, so just give up by returning something
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            return planTableScan(node, Booleans.TRUE, context.get());
        }

        private PlanNode planTableScan(TableScanNode node, Expression predicate, Context context)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                    plannerContext,
                    session,
                    predicate);

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transformKeys(node.getAssignments()::get)
                    .intersect(node.getEnforcedConstraint());

            checkState(node.getOutputSymbols().containsAll(context.getLookupSymbols()));

            Set<ColumnHandle> lookupColumns = context.getLookupSymbols().stream()
                    .map(node.getAssignments()::get)
                    .collect(toImmutableSet());

            Set<ColumnHandle> outputColumns = node.getOutputSymbols().stream().map(node.getAssignments()::get).collect(toImmutableSet());

            Optional<ResolvedIndex> optionalResolvedIndex = plannerContext.getMetadata().resolveIndex(session, node.getTable(), lookupColumns, outputColumns, simplifiedConstraint);
            if (optionalResolvedIndex.isEmpty()) {
                // No index available, so give up by returning something
                return node;
            }
            ResolvedIndex resolvedIndex = optionalResolvedIndex.get();

            Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            PlanNode source = new IndexSourceNode(
                    idAllocator.getNextId(),
                    resolvedIndex.getIndexHandle(),
                    node.getTable(),
                    context.getLookupSymbols(),
                    node.getOutputSymbols(),
                    node.getAssignments());

            Expression resultingPredicate = combineConjuncts(
                    domainTranslator.toPredicate(resolvedIndex.getUnresolvedTupleDomain().transformKeys(inverseAssignments::get)),
                    decomposedPredicate.getRemainingExpression());

            if (!resultingPredicate.equals(TRUE)) {
                // todo it is likely we end up with redundant filters here because the predicate push down has already been run... the fix is to run predicate push down again
                source = new FilterNode(idAllocator.getNextId(), source, resultingPredicate);
            }
            context.markSuccess();
            return source;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            // Rewrite the lookup symbols in terms of only the pre-projected symbols that have direct translations
            Set<Symbol> newLookupSymbols = context.get().getLookupSymbols().stream()
                    .map(node.getAssignments()::get)
                    .filter(Reference.class::isInstance)
                    .map(Symbol::from)
                    .collect(toImmutableSet());

            if (newLookupSymbols.size() != context.get().getLookupSymbols().size()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(newLookupSymbols, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate(), context.get());
            }

            return context.defaultRewrite(node, new Context(context.get().getLookupSymbols(), context.get().getSuccess()));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Context> context)
        {
            if (!node.getWindowFunctions().values().stream()
                    .map(Function::getResolvedFunction)
                    .map(ResolvedFunction::functionKind)
                    .allMatch(AGGREGATE::equals)) {
                return node;
            }

            // Don't need this restriction if we can prove that all order by symbols are deterministically produced
            if (node.getOrderingScheme().isPresent()) {
                return node;
            }

            // Only RANGE frame type currently supported for aggregation functions because it guarantees the
            // same value for each peer group.
            // ROWS frame type requires the ordering to be fully deterministic (e.g. deterministically sorted on all columns)
            if (node.getFrames().stream().map(WindowNode.Frame::getType).anyMatch(type -> type != RANGE)) { // TODO: extract frames of type RANGE and allow optimization on them
                return node;
            }

            // Lookup symbols can only be passed through if they are part of the partitioning

            if (!node.getPartitionBy().containsAll(context.get().getLookupSymbols())) {
                return node;
            }

            return context.defaultRewrite(node, new Context(context.get().getLookupSymbols(), context.get().getSuccess()));
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Context> context)
        {
            throw new IllegalStateException("Should not be trying to generate an Index on something that has already been determined to use an Index");
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Context> context)
        {
            // Lookup symbols can only be passed through the probe side of an index join
            if (!node.getProbeSource().getOutputSymbols().containsAll(context.get().getLookupSymbols())) {
                return node;
            }

            PlanNode rewrittenProbeSource = context.rewrite(node.getProbeSource(), new Context(context.get().getLookupSymbols(), context.get().getSuccess()));

            PlanNode source = node;
            if (rewrittenProbeSource != node.getProbeSource()) {
                source = new IndexJoinNode(node.getId(), node.getType(), rewrittenProbeSource, node.getIndexSource(), node.getCriteria(), node.getProbeHashSymbol(), node.getIndexHashSymbol());
            }

            return source;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            // Lookup symbols can only be passed through if they are part of the group by columns
            if (!node.getGroupingKeys().containsAll(context.get().getLookupSymbols())) {
                return node;
            }

            return context.defaultRewrite(node, new Context(context.get().getLookupSymbols(), context.get().getSuccess()));
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Context> context)
        {
            // Sort has no bearing when building an index, so just ignore the sort
            return context.rewrite(node.getSource(), context.get());
        }

        public static class Context
        {
            private final Set<Symbol> lookupSymbols;
            private final AtomicBoolean success;

            public Context(Set<Symbol> lookupSymbols, AtomicBoolean success)
            {
                checkArgument(!lookupSymbols.isEmpty(), "lookupSymbols cannot be empty");
                this.lookupSymbols = ImmutableSet.copyOf(requireNonNull(lookupSymbols, "lookupSymbols is null"));
                this.success = requireNonNull(success, "success is null");
            }

            public Set<Symbol> getLookupSymbols()
            {
                return lookupSymbols;
            }

            public AtomicBoolean getSuccess()
            {
                return success;
            }

            public void markSuccess()
            {
                checkState(success.compareAndSet(false, true), "Can only have one success per context");
            }
        }
    }

    /**
     * Identify the mapping from the lookup symbols used at the top of the index plan to
     * the actual symbols produced by the IndexSource. Note that multiple top-level lookup symbols may share the same
     * underlying IndexSource symbol. Also note that lookup symbols that do not correspond to underlying index source symbols
     * will be omitted from the returned Map.
     */
    public static final class IndexKeyTracer
    {
        private IndexKeyTracer() {}

        public static Map<Symbol, Symbol> trace(PlanNode node, Set<Symbol> lookupSymbols)
        {
            return node.accept(new Visitor(), lookupSymbols);
        }

        private static class Visitor
                extends PlanVisitor<Map<Symbol, Symbol>, Set<Symbol>>
        {
            @Override
            protected Map<Symbol, Symbol> visitPlan(PlanNode node, Set<Symbol> lookupSymbols)
            {
                throw new UnsupportedOperationException("Node not expected to be part of Index pipeline: " + node);
            }

            @Override
            public Map<Symbol, Symbol> visitProject(ProjectNode node, Set<Symbol> lookupSymbols)
            {
                // Map from output Symbols to source Symbols
                Map<Symbol, Symbol> directSymbolTranslationOutputMap = Maps.transformValues(Maps.filterValues(node.getAssignments().getMap(), Reference.class::isInstance), Symbol::from);
                Map<Symbol, Symbol> outputToSourceMap = lookupSymbols.stream()
                        .filter(directSymbolTranslationOutputMap.keySet()::contains)
                        .collect(toImmutableMap(identity(), directSymbolTranslationOutputMap::get));

                checkState(!outputToSourceMap.isEmpty(), "No lookup symbols were able to pass through the projection");

                // Map from source Symbols to underlying index source Symbols
                Map<Symbol, Symbol> sourceToIndexMap = node.getSource().accept(this, ImmutableSet.copyOf(outputToSourceMap.values()));

                // Generate the Map the connects lookup symbols to underlying index source symbols
                Map<Symbol, Symbol> outputToIndexMap = Maps.transformValues(Maps.filterValues(outputToSourceMap, in(sourceToIndexMap.keySet())), Functions.forMap(sourceToIndexMap));
                return ImmutableMap.copyOf(outputToIndexMap);
            }

            @Override
            public Map<Symbol, Symbol> visitFilter(FilterNode node, Set<Symbol> lookupSymbols)
            {
                return node.getSource().accept(this, lookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitWindow(WindowNode node, Set<Symbol> lookupSymbols)
            {
                Set<Symbol> partitionByLookupSymbols = lookupSymbols.stream()
                        .filter(node.getPartitionBy()::contains)
                        .collect(toImmutableSet());
                checkState(!partitionByLookupSymbols.isEmpty(), "No lookup symbols were able to pass through the aggregation group by");
                return node.getSource().accept(this, partitionByLookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitIndexJoin(IndexJoinNode node, Set<Symbol> lookupSymbols)
            {
                Set<Symbol> probeLookupSymbols = lookupSymbols.stream()
                        .filter(node.getProbeSource().getOutputSymbols()::contains)
                        .collect(toImmutableSet());
                checkState(!probeLookupSymbols.isEmpty(), "No lookup symbols were able to pass through the index join probe source");
                return node.getProbeSource().accept(this, probeLookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitAggregation(AggregationNode node, Set<Symbol> lookupSymbols)
            {
                Set<Symbol> groupByLookupSymbols = lookupSymbols.stream()
                        .filter(node.getGroupingKeys()::contains)
                        .collect(toImmutableSet());
                checkState(!groupByLookupSymbols.isEmpty(), "No lookup symbols were able to pass through the aggregation group by");
                return node.getSource().accept(this, groupByLookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitSort(SortNode node, Set<Symbol> lookupSymbols)
            {
                return node.getSource().accept(this, lookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitIndexSource(IndexSourceNode node, Set<Symbol> lookupSymbols)
            {
                checkState(node.getLookupSymbols().equals(lookupSymbols), "lookupSymbols must be the same as IndexSource lookup symbols");
                return lookupSymbols.stream().collect(toImmutableMap(identity(), identity()));
            }
        }
    }
}
