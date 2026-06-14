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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.containsAssignUniqueId;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.containsCorrelatedSetOperation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isDistinctRelation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarGlobalAggregation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarSubqueryShape;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.correlationMapping;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

/// One step of the Neumann/Kemper pushdown: push the dependent join through the subquery's top
/// operator, leaving a residual dependent join (a `CorrelatedJoinNode`) below it for the
/// next rule firing — or degenerate to a cross join when the subquery no longer references the
/// correlation (the base equivalence; correlation-using predicates have risen as filters by
/// then). INNER with a TRUE filter only: `ExtractCorrelatedJoinFilter` normalizes the
/// filter away first, and LEFT must null-extend, which a partial push cannot do — LEFT goes
/// through the plain forms or the magic-set, whose residual is again INNER.
public class PushDownDependentJoin
        extends DependentJoinFrameworkRule
{
    public PushDownDependentJoin(PlannerContext plannerContext)
    {
        super(plannerContext);
    }

    @Override
    protected Optional<PlanNode> apply(CorrelatedJoinNode node, DependentJoinAlgebra algebra, Context context)
    {
        return new Rewriter(plannerContext(), context, algebra).pushDownOneStep(node);
    }

    private static final class Rewriter
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Lookup lookup;
        private final Metadata metadata;
        private final DependentJoinAlgebra algebra;

        private Rewriter(PlannerContext plannerContext, Context context, DependentJoinAlgebra algebra)
        {
            this.idAllocator = context.getIdAllocator();
            this.symbolAllocator = context.getSymbolAllocator();
            this.lookup = context.getLookup();
            this.metadata = plannerContext.getMetadata();
            this.algebra = algebra;
        }

        private Optional<PlanNode> pushDownOneStep(CorrelatedJoinNode correlatedJoin)
        {
            if (correlatedJoin.getType() != JoinType.INNER
                    || !correlatedJoin.getFilter().equals(TRUE)) {
                return Optional.empty();
            }
            PlanNode subquery = correlatedJoin.getSubquery();
            if (isScalarSubqueryShape(subquery, lookup) || isScalarGlobalAggregation(subquery, lookup)) {
                return Optional.empty();
            }
            PlanNode input = correlatedJoin.getInput();
            Set<Symbol> correlation = ImmutableSet.copyOf(correlatedJoin.getCorrelation());

            // Every correlation decision in the framework (here, the residual emission's base
            // case, referencesCorrelation, the set-operation walk-stop) hangs on
            // SymbolsExtractor.extractUnique, which sees expressions but is structurally blind —
            // it does not see symbols carried only as window/partition/grouping/unnest-mapping
            // structure. The soundness invariant is that correlation is always expression-visible
            // except for two structural carriers, both guarded at admission: a correlation symbol
            // carried as a join output (correlationCarriedAsJoinOutput) and a TopN ordered by
            // correlation (topNOrdersByCorrelation); the rebinder verifies its own coverage
            // independently. A planner change introducing a new structural carrier must extend
            // those guards.
            PlanNode resolved = lookup.resolve(subquery);
            Set<Symbol> usedInSubquery = SymbolsExtractor.extractUnique(resolved, lookup);
            Optional<PlanNode> result = Sets.intersection(usedInSubquery, correlation).isEmpty()
                    ? Optional.of(algebra.crossJoin(input, subquery))
                    : resolved.accept(new PushDownVisitor(this, input, correlation), null);
            return result.map(plan -> algebra.restrictOutputs(plan, correlatedJoin.getOutputSymbols()));
        }

        private static final class PushDownVisitor
                extends PlanVisitor<Optional<PlanNode>, Void>
        {
            private final PlanNodeIdAllocator idAllocator;
            private final SymbolAllocator symbolAllocator;
            private final Lookup lookup;
            private final Metadata metadata;
            private final DependentJoinAlgebra algebra;
            private final PlanNode input;
            private final Set<Symbol> correlation;

            private PushDownVisitor(Rewriter rewriter, PlanNode input, Set<Symbol> correlation)
            {
                this.idAllocator = rewriter.idAllocator;
                this.symbolAllocator = rewriter.symbolAllocator;
                this.lookup = rewriter.lookup;
                this.metadata = rewriter.metadata;
                this.algebra = rewriter.algebra;
                this.input = input;
                this.correlation = correlation;
            }

            @Override
            protected Optional<PlanNode> visitPlan(PlanNode node, Void context)
            {
                // Unhandled shape — the rule declines; the correlated join fails as unsupported
                // unless another rule accepts it.
                return Optional.empty();
            }

            @Override
            public Optional<PlanNode> visitFilter(FilterNode node, Void context)
            {
                // σ pushdown: `T₁ ⋈^D σ_p(T) ≡ σ_p(T₁ ⋈^D T)`. The predicate `p` may reference both
                // input and subquery symbols after the join — it becomes a filter above the residual
                // dependent join. Any sub-expression of `p` that references correlation is now a
                // theta-join predicate (the source of inequality-correlation support).
                return Optional.of(new FilterNode(idAllocator.getNextId(), algebra.dependentJoin(input, node.getSource(), correlation), node.getPredicate()));
            }

            @Override
            public Optional<PlanNode> visitProject(ProjectNode node, Void context)
            {
                // π pushdown: `T₁ ⋈^D π_e(T) ≡ π_{A(T₁) ∪ e}(T₁ ⋈^D T)`. Add identity assignments for
                // the input symbols so they remain accessible above the projection.
                Assignments augmented = Assignments.builder()
                        .putIdentities(input.getOutputSymbols())
                        .putAll(node.getAssignments())
                        .build();
                return Optional.of(new ProjectNode(idAllocator.getNextId(), algebra.dependentJoin(input, node.getSource(), correlation), augmented));
            }

            @Override
            public Optional<PlanNode> visitAggregation(AggregationNode node, Void context)
            {
                // Γ pushdown: add a unique id of the input (and the input symbols) to the grouping
                // keys so the aggregation runs once per outer row — the unique id keeps duplicate
                // input rows from merging into one group. Multiple grouping sets (an aggregation over
                // a GroupIdNode) are supported when none of the sets is global: T₁'s columns join
                // every set (here and in visitGroupId), each set computed once per outer row.
                // Pre-grouped symbols and a non-SINGLE step (PARTIAL/FINAL splits) are post-optimizer
                // constructs we don't expect here, but bail conservatively if seen.
                if (!node.getPreGroupedSymbols().isEmpty() || node.getStep() != AggregationNode.Step.SINGLE) {
                    return Optional.empty();
                }
                // A GLOBAL aggregation produces a default row (count = 0, other aggregates NULL) over an
                // empty source. Re-keying it on the input columns loses exactly that row: an outer row
                // with no matches would disappear instead of seeing the default value — wrong for e.g.
                // `HAVING count(*) = 0` or a filter over the count. The supported global form is the
                // top-of-subquery scalar aggregate (see decorrelateGlobalAggregation, which restores empty
                // groups with a mask); reached mid-pushdown there is no sound rewrite here, so bail —
                // the legacy decorrelator rejects global grouping the same way.
                if (node.hasEmptyGroupingSet()) {
                    return Optional.empty();
                }
                // A set operation below distributes the input over branch clones, which cannot carry a
                // synthetic unique id (a clone re-generates it). Evaluate the whole aggregation per
                // distinct correlation value instead and INNER-join the input back — empty groups
                // produce no rows either way.
                if (!isDistinctRelation(input, lookup) && containsCorrelatedSetOperation(node.getSource(), correlation, lookup)) {
                    return algebra.magicSetJoin(JoinType.INNER, input, node, ImmutableList.copyOf(correlation), TRUE, node.getOutputSymbols());
                }
                PlanNode pushDownInput = isDistinctRelation(input, lookup)
                        // Duplicate-free input rows are identified by their own columns; no id needed.
                        ? input
                        : new AssignUniqueId(idAllocator.getNextId(), input, symbolAllocator.newSymbol("unique", BIGINT));
                // De-duplicated: a grouping key can itself be a correlation symbol that also
                // survives in the pushed input's outputs, and duplicate grouping keys violate an
                // invariant most of the planner never produces.
                List<Symbol> augmentedKeys = ImmutableSet.<Symbol>builder()
                        .addAll(pushDownInput.getOutputSymbols())
                        .addAll(node.getGroupingKeys())
                        .build()
                        .asList();
                return Optional.of(new AggregationNode(
                        idAllocator.getNextId(),
                        algebra.dependentJoin(pushDownInput, node.getSource(), correlation),
                        node.getAggregations(),
                        AggregationNode.groupingSets(augmentedKeys, node.getGroupingSetCount(), ImmutableSet.of()),
                        ImmutableList.of(),
                        node.getStep(),
                        node.getGroupIdSymbol()));
            }

            @Override
            public Optional<PlanNode> visitGroupId(GroupIdNode node, Void context)
            {
                // GroupId pushdown (the basis of GROUPING SETS / ROLLUP / CUBE, which plan as an
                // AggregationNode over a GroupIdNode): add T₁'s columns to every grouping set as
                // identity grouping columns, so each set is computed once per outer row and T₁'s columns
                // survive to the aggregation above (GroupId only outputs grouping-set/aggregation/groupId
                // columns, so columns not added here would be dropped). Only reached for grouping sets
                // without a global set — an aggregation carrying one (e.g. ROLLUP, or an explicit `()`
                // set, whose default row over an empty per-outer-row source the re-keying would lose)
                // bails in visitAggregation before recursing here. Non-global sets produce no rows over
                // an empty source, which the re-keyed form preserves.
                List<Symbol> inputSymbols = input.getOutputSymbols();
                List<List<Symbol>> augmentedSets = node.getGroupingSets().stream()
                        .map(set -> ImmutableList.<Symbol>builder().addAll(inputSymbols).addAll(set).build())
                        .collect(toImmutableList());
                ImmutableMap.Builder<Symbol, Symbol> augmentedColumns = ImmutableMap.builder();
                augmentedColumns.putAll(node.getGroupingColumns());
                for (Symbol symbol : inputSymbols) {
                    augmentedColumns.put(symbol, symbol);
                }
                return Optional.of(new GroupIdNode(
                        idAllocator.getNextId(),
                        algebra.dependentJoin(input, node.getSource(), correlation),
                        augmentedSets,
                        augmentedColumns.buildOrThrow(),
                        node.getAggregationArguments(),
                        node.getGroupIdSymbol()));
            }

            @Override
            public Optional<PlanNode> visitLimit(LimitNode node, Void context)
            {
                // Partial limits and pre-sorted-input requirements are post-optimizer constructs
                // we don't expect here, but bail conservatively if seen (mirroring
                // visitAggregation's non-SINGLE-step bail).
                if (node.isPartial() || node.requiresPreSortedInputs()) {
                    return Optional.empty();
                }
                // lim pushdown: `T₁ ⋈^D Lim_n(T) ≡ σ_{rn ≤ n}(rowNumber OVER (PARTITION BY uid(T₁))(T₁ ⋈^D T))`.
                // Tag T₁'s rows with a unique id so "first n per outer row" partitions correctly even
                // when T₁ contains duplicate rows (a duplicate-free input partitions by its own
                // columns). WITH TIES uses rank() (which keeps the nth row plus all rows tying with
                // it) ordered by the ties-resolving scheme, instead of row_number().
                // A set operation below cannot distribute a unique-id-tagged input (branch clones
                // re-generate the ids) — evaluate the limit per distinct correlation value instead.
                if (!isDistinctRelation(input, lookup) && containsCorrelatedSetOperation(node.getSource(), correlation, lookup)) {
                    return algebra.magicSetJoin(JoinType.INNER, input, node, ImmutableList.copyOf(correlation), TRUE, node.getOutputSymbols());
                }
                PlanNode pushDownInput = input;
                List<Symbol> partitionBy = input.getOutputSymbols();
                if (!isDistinctRelation(input, lookup)) {
                    Symbol uniqueSymbol = symbolAllocator.newSymbol("unique", BIGINT);
                    pushDownInput = new AssignUniqueId(idAllocator.getNextId(), input, uniqueSymbol);
                    partitionBy = ImmutableList.of(uniqueSymbol);
                }
                PlanNode inner = algebra.dependentJoin(pushDownInput, node.getSource(), correlation);
                Symbol counter = symbolAllocator.newSymbol(node.isWithTies() ? "rank_num" : "row_number", BIGINT);
                PlanNode numbered = node.isWithTies()
                        ? rankWindow(inner, partitionBy, counter, node.getTiesResolvingScheme())
                        : new RowNumberNode(idAllocator.getNextId(), inner, partitionBy, false, counter, Optional.empty());
                return Optional.of(new FilterNode(
                        idAllocator.getNextId(),
                        numbered,
                        new Comparison(LESS_THAN_OR_EQUAL, counter.toSymbolReference(), new Constant(BIGINT, node.getCount()))));
            }

            private WindowNode rankWindow(PlanNode source, List<Symbol> partition, Symbol rank, Optional<OrderingScheme> ordering)
            {
                WindowNode.Function rankFunction = new WindowNode.Function(
                        metadata.resolveBuiltinFunction("rank", ImmutableList.of()),
                        ImmutableList.of(),
                        Optional.empty(),
                        DEFAULT_FRAME,
                        false,
                        false);
                return new WindowNode(
                        idAllocator.getNextId(),
                        source,
                        new DataOrganizationSpecification(partition, ordering),
                        ImmutableMap.of(rank, rankFunction),
                        ImmutableSet.of(),
                        0);
            }

            @Override
            public Optional<PlanNode> visitTopN(TopNNode node, Void context)
            {
                // A non-SINGLE TopN is a post-optimizer split — bail conservatively, as for
                // aggregations and limits.
                if (node.getStep() != TopNNode.Step.SINGLE) {
                    return Optional.empty();
                }
                // TopN pushdown: like LIMIT but row numbering follows the TopN ordering, evaluated
                // per outer row via a window partitioned by the unique input id (or the input columns
                // when the input is duplicate-free). (TopN ordered by a correlation symbol is declined
                // at admission — see topNOrdersByCorrelation.)
                if (!isDistinctRelation(input, lookup) && containsCorrelatedSetOperation(node.getSource(), correlation, lookup)) {
                    return algebra.magicSetJoin(JoinType.INNER, input, node, ImmutableList.copyOf(correlation), TRUE, node.getOutputSymbols());
                }
                PlanNode pushDownInput = input;
                List<Symbol> partitionBy = input.getOutputSymbols();
                if (!isDistinctRelation(input, lookup)) {
                    Symbol uniqueSymbol = symbolAllocator.newSymbol("unique", BIGINT);
                    pushDownInput = new AssignUniqueId(idAllocator.getNextId(), input, uniqueSymbol);
                    partitionBy = ImmutableList.of(uniqueSymbol);
                }
                Symbol rowNumber = symbolAllocator.newSymbol("row_number", BIGINT);
                WindowNode.Function rowNumberFunction = new WindowNode.Function(
                        metadata.resolveBuiltinFunction("row_number", ImmutableList.of()),
                        ImmutableList.of(),
                        Optional.empty(),
                        DEFAULT_FRAME,
                        false,
                        false);
                WindowNode window = new WindowNode(
                        idAllocator.getNextId(),
                        algebra.dependentJoin(pushDownInput, node.getSource(), correlation),
                        new DataOrganizationSpecification(partitionBy, Optional.of(node.getOrderingScheme())),
                        ImmutableMap.of(rowNumber, rowNumberFunction),
                        ImmutableSet.of(),
                        0);
                return Optional.of(new FilterNode(
                        idAllocator.getNextId(),
                        window,
                        new Comparison(LESS_THAN_OR_EQUAL, rowNumber.toSymbolReference(), new Constant(BIGINT, node.getCount()))));
            }

            @Override
            public Optional<PlanNode> visitWindow(WindowNode node, Void context)
            {
                // Window pushdown: `T₁ ⋈^D W_{P;O}(T) ≡ W_{uid(T₁) ∪ P; O}(T₁ ⋈^D T)`. Adding a unique
                // id of the input to the PARTITION BY makes each window function evaluate once per outer
                // row (the unique id, not the input columns, so duplicate outer rows don't merge), with
                // the original partitioning/ordering preserved within each outer row. A duplicate-free
                // input partitions by its own columns instead.
                if (!isDistinctRelation(input, lookup) && containsCorrelatedSetOperation(node.getSource(), correlation, lookup)) {
                    return algebra.magicSetJoin(JoinType.INNER, input, node, ImmutableList.copyOf(correlation), TRUE, node.getOutputSymbols());
                }
                PlanNode pushDownInput = input;
                List<Symbol> partitionBy = input.getOutputSymbols();
                if (!isDistinctRelation(input, lookup)) {
                    Symbol uniqueSymbol = symbolAllocator.newSymbol("unique", BIGINT);
                    pushDownInput = new AssignUniqueId(idAllocator.getNextId(), input, uniqueSymbol);
                    partitionBy = ImmutableList.of(uniqueSymbol);
                }
                return Optional.of(new WindowNode(
                        idAllocator.getNextId(),
                        algebra.dependentJoin(pushDownInput, node.getSource(), correlation),
                        new DataOrganizationSpecification(
                                ImmutableList.<Symbol>builder().addAll(partitionBy).addAll(node.getPartitionBy()).build(),
                                node.getOrderingScheme()),
                        node.getWindowFunctions(),
                        ImmutableSet.of(),
                        0));
            }

            @Override
            public Optional<PlanNode> visitJoin(JoinNode node, Void context)
            {
                boolean leftDepends = referencesCorrelation(node.getLeft());
                boolean rightDepends = referencesCorrelation(node.getRight());

                return switch (node.getType()) {
                    case INNER -> pushInnerJoin(node, leftDepends, rightDepends);
                    // Outer joins. When the dependency is confined to the PRESERVING side (or rides only
                    // on the predicate), the outer join distributes — push T₁ into the preserving side:
                    //   T₁ ⋈^D (L ⟕_p R) ≡ (T₁ ⋈^D L) ⟕_p R   when R is correlation-free
                    //   T₁ ⋈^D (L ⟖_p R) ≡ L ⟖_p (T₁ ⋈^D R)   when L is correlation-free
                    // When the NULL-SUPPLYING side carries the correlation (and the preserving side is
                    // correlation-free), cross-join the preserving side into T₁ and decorrelate via the
                    // LEFT magic-set, since (e.g.) T₁ ⋈^D (L ⟕_p R) ≡ (T₁ × L) ⟕^D_p R. Both sides
                    // depending, or FULL outer, still bail.
                    case LEFT -> {
                        if (!rightDepends) {
                            yield Optional.of(rebuildJoin(node, algebra.dependentJoin(input, node.getLeft(), correlation), node.getRight()));
                        }
                        yield leftDepends
                                ? Optional.empty()
                                : algebra.magicSetJoin(JoinType.LEFT, algebra.crossJoin(input, node.getLeft()), node.getRight(), ImmutableList.copyOf(correlation), joinConditionOf(node), node.getRight().getOutputSymbols());
                    }
                    case RIGHT -> {
                        if (!leftDepends) {
                            yield Optional.of(rebuildJoin(node, node.getLeft(), algebra.dependentJoin(input, node.getRight(), correlation)));
                        }
                        yield rightDepends
                                ? Optional.empty()
                                : algebra.magicSetJoin(JoinType.LEFT, algebra.crossJoin(input, node.getRight()), node.getLeft(), ImmutableList.copyOf(correlation), joinConditionOf(node), node.getLeft().getOutputSymbols());
                    }
                    // FULL: both sides are null-supplying — neither push is sound.
                    case FULL -> Optional.empty();
                };
            }

            private Optional<PlanNode> pushInnerJoin(JoinNode node, boolean leftDepends, boolean rightDepends)
            {
                if (leftDepends && rightDepends) {
                    // Both sides depend on correlation:
                    //   T₁ ⋈^D (L ⋈_q R) ≡ σ_q((T₁ ⋈^D L) ⋈^D R)
                    // Push T₁ into L, then treat the whole (T₁ ⋈^D L) — which now carries T₁'s
                    // correlation columns — as the input for a dependent join with R. The original
                    // join condition q (criteria + filter) becomes a filter on the combined result.
                    // No AssignUniqueId is needed: T₁'s rows ride along in both pushdowns and the
                    // final filter q selects the matching (L, R) pairs per T₁ row.
                    PlanNode leftResult = algebra.dependentJoin(input, node.getLeft(), correlation);
                    PlanNode combined = algebra.dependentJoin(leftResult, node.getRight(), correlation);
                    Expression joinCondition = joinConditionOf(node);
                    return Optional.of(joinCondition.equals(TRUE) ? combined : new FilterNode(idAllocator.getNextId(), combined, joinCondition));
                }
                if (leftDepends) {
                    return Optional.of(rebuildJoin(node, algebra.dependentJoin(input, node.getLeft(), correlation), node.getRight()));
                }
                // rightDepends — or neither, but then we shouldn't have entered the visitor.
                return Optional.of(rebuildJoin(node, node.getLeft(), algebra.dependentJoin(input, node.getRight(), correlation)));
            }

            private Expression joinConditionOf(JoinNode node)
            {
                ImmutableList.Builder<Expression> conjuncts = ImmutableList.builder();
                for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                    conjuncts.add(new Comparison(Comparison.Operator.EQUAL, clause.getLeft().toSymbolReference(), clause.getRight().toSymbolReference()));
                }
                node.getFilter().ifPresent(conjuncts::add);
                List<Expression> all = conjuncts.build();
                return and(all);
            }

            @Override
            public Optional<PlanNode> visitUnnest(UnnestNode node, Void context)
            {
                // UNNEST pushdown: `T₁ ⋈^D Unnest_{R; arrs}(S) ≡ Unnest_{R ∪ A(T₁); arrs}(T₁ ⋈^D S)`.
                // In a correlated subquery the unnested arrays are free correlation references and the
                // unnest's own source is a trivial single-row, so pushing T₁ into the source brings the
                // array columns into scope; T₁'s columns are added to the replicate list to pass through.
                // The unnest's own join type (INNER/LEFT, controlling empty-array null-extension) and
                // ordinality are preserved.
                return Optional.of(new UnnestNode(
                        idAllocator.getNextId(),
                        algebra.dependentJoin(input, node.getSource(), correlation),
                        ImmutableList.<Symbol>builder()
                                .addAll(input.getOutputSymbols())
                                .addAll(node.getReplicateSymbols())
                                .build(),
                        node.getMappings(),
                        node.getOrdinalitySymbol(),
                        node.getJoinType()));
            }

            @Override
            public Optional<PlanNode> visitUnion(UnionNode node, Void context)
            {
                return distributeSetOperation(node);
            }

            @Override
            public Optional<PlanNode> visitIntersect(IntersectNode node, Void context)
            {
                return distributeSetOperation(node);
            }

            @Override
            public Optional<PlanNode> visitExcept(ExceptNode node, Void context)
            {
                return distributeSetOperation(node);
            }

            /// ⊕ pushdown: `T₁ ⋈^D (B_1 ⊕ … ⊕ B_n) ≡ (T₁ ⋈^D B_1) ⊕ … ⊕ (T₁ ⋈^D B_n)`, valid for
            /// `UNION`/`INTERSECT`/`EXCEPT` alike. Each distributed branch carries
            /// `T₁`'s columns, so they join the set-op key and partition the set semantics by outer
            /// row — for a fixed `t`, the distributed set-op reduces to `B_1(t) ⊕ … ⊕ B_n(t)`.
            ///
            /// A plan is a tree, not a DAG, so `T₁` can't be physically shared across branches:
            /// every branch after the first gets a fresh clone of `T₁` (via [DependentJoinAlgebra#copyInput])
            /// with its correlation references rebound onto the clone. If `PlanCopier` can't copy
            /// the input, the rule declines and the shape fails as unsupported.
            private Optional<PlanNode> distributeSetOperation(SetOperationNode node)
            {
                // Distribution evaluates branches 2+ against a CLONE of the input and matches rows
                // across branches by the input columns. That is sound for an ALL-flavored set-op over
                // an input free of synthetic per-row ids (concatenation and multiplicity arithmetic
                // distribute over duplicated inputs), and for any flavor over a duplicate-free input.
                // A distinct-flavored set-op over a bag input would merge duplicate input rows, and an
                // AssignUniqueId-tagged input cannot be matched across clones (each clone re-generates
                // the ids) — evaluate those per distinct correlation value and INNER-join the input
                // back. (UNION is always planned as ALL; a distinct UNION arrives as a dedup
                // aggregation above, whose pushdown routes here with the magic-set's distinct input.)
                boolean distinctFlavored = switch (node) {
                    case IntersectNode intersect -> intersect.isDistinct();
                    case ExceptNode except -> except.isDistinct();
                    default -> false;
                };
                if (!isDistinctRelation(input, lookup) && (distinctFlavored || containsAssignUniqueId(input, lookup))) {
                    return algebra.magicSetJoin(JoinType.INNER, input, node, ImmutableList.copyOf(correlation), TRUE, node.getOutputSymbols());
                }
                List<Symbol> correlationList = ImmutableList.copyOf(correlation);
                List<Symbol> inputSymbols = input.getOutputSymbols();
                List<PlanNode> branches = node.getSources();

                ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
                // perBranchInputSymbols.get(i) is parallel to inputSymbols: branch i's copy of T₁'s columns.
                List<List<Symbol>> perBranchInputSymbols = new ArrayList<>();

                for (int i = 0; i < branches.size(); i++) {
                    PlanNode branch = branches.get(i);
                    PlanNode branchInput;
                    List<Symbol> branchInputSymbols;
                    PlanNode branchSubquery;
                    Set<Symbol> branchCorrelation;
                    if (i == 0) {
                        // The first branch reuses the original input directly — no clone needed.
                        branchInput = input;
                        branchInputSymbols = inputSymbols;
                        branchSubquery = branch;
                        branchCorrelation = correlation;
                    }
                    else {
                        Optional<NodeAndMappings> cloneOptional = algebra.copyInput(input, inputSymbols);
                        if (cloneOptional.isEmpty()) {
                            return Optional.empty();
                        }
                        NodeAndMappings clone = cloneOptional.get();
                        branchInput = clone.getNode();
                        branchInputSymbols = clone.getFields();
                        Map<Symbol, Symbol> rebind = correlationMapping(inputSymbols, branchInputSymbols);
                        Optional<PlanNode> reboundBranch = algebra.rebindCorrelation(branch, rebind);
                        if (reboundBranch.isEmpty()) {
                            return Optional.empty();
                        }
                        branchSubquery = reboundBranch.get();
                        branchCorrelation = correlationList.stream()
                                .map(rebind::get)
                                .collect(toImmutableSet());
                    }
                    newSources.add(algebra.dependentJoin(branchInput, branchSubquery, branchCorrelation));
                    perBranchInputSymbols.add(branchInputSymbols);
                }

                // Canonical outputs for T₁'s columns are the original input symbols (branch 0's copy);
                // the original set-op outputs keep their per-source mapping (the subquery side is never
                // cloned, so its symbols survive the pushdown unchanged).
                ImmutableListMultimap.Builder<Symbol, Symbol> outputToInputs = ImmutableListMultimap.builder();
                for (int column = 0; column < inputSymbols.size(); column++) {
                    for (List<Symbol> branchSymbols : perBranchInputSymbols) {
                        outputToInputs.put(inputSymbols.get(column), branchSymbols.get(column));
                    }
                }
                for (Symbol output : node.getOutputSymbols()) {
                    node.getSymbolMapping().get(output).forEach(source -> outputToInputs.put(output, source));
                }
                List<Symbol> outputs = ImmutableList.<Symbol>builder()
                        .addAll(inputSymbols)
                        .addAll(node.getOutputSymbols())
                        .build();

                return Optional.of(rebuildSetOperation(node, newSources.build(), outputToInputs.build(), outputs));
            }

            private PlanNode rebuildSetOperation(SetOperationNode original, List<PlanNode> sources, ListMultimap<Symbol, Symbol> outputToInputs, List<Symbol> outputs)
            {
                return switch (original) {
                    case UnionNode _ -> new UnionNode(idAllocator.getNextId(), sources, outputToInputs, outputs);
                    case IntersectNode intersect -> new IntersectNode(idAllocator.getNextId(), sources, outputToInputs, outputs, intersect.isDistinct());
                    case ExceptNode except -> new ExceptNode(idAllocator.getNextId(), sources, outputToInputs, outputs, except.isDistinct());
                    default -> throw new IllegalStateException("Unexpected set operation: " + original.getClass().getSimpleName());
                };
            }

            private boolean referencesCorrelation(PlanNode node)
            {
                return !Sets.intersection(SymbolsExtractor.extractUnique(node, lookup), correlation).isEmpty();
            }

            private JoinNode rebuildJoin(JoinNode original, PlanNode newLeft, PlanNode newRight)
            {
                return new JoinNode(
                        idAllocator.getNextId(),
                        original.getType(),
                        newLeft,
                        newRight,
                        original.getCriteria(),
                        chooseOutputs(newLeft, original.getLeftOutputSymbols()),
                        chooseOutputs(newRight, original.getRightOutputSymbols()),
                        original.isMaySkipOutputDuplicates(),
                        original.getFilter(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty());
            }

            private List<Symbol> chooseOutputs(PlanNode side, List<Symbol> originalOutputs)
            {
                // After decorrelating one side, that side carries T₁'s input columns (pushed in from the
                // cross-join base case). The rebuilt join must surface them alongside the still-available
                // original outputs, so a consumer above the join — ultimately restrictOutputs to the
                // correlated join's declared outputs — can see the correlation columns. The undecorrelated
                // side carries none of T₁'s columns, so the availability filter leaves it unchanged.
                Set<Symbol> available = ImmutableSet.copyOf(side.getOutputSymbols());
                ImmutableList.Builder<Symbol> chosen = ImmutableList.builder();
                for (Symbol symbol : input.getOutputSymbols()) {
                    if (available.contains(symbol)) {
                        chosen.add(symbol);
                    }
                }
                for (Symbol symbol : originalOutputs) {
                    if (available.contains(symbol)) {
                        chosen.add(symbol);
                    }
                }
                return chosen.build();
            }
        }
    }
}
