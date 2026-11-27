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

package io.trino.sql.planner.optimizations.decorrelation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.ExpressionExtractor;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.PlanCopier;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.sanity.ValidateDependenciesChecker;
import io.trino.sql.tree.Node;
import io.trino.type.CharVarcharCoercion;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.iterative.Plans.resolveGroupReferences;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static java.util.Objects.requireNonNull;

/// The shared dependent-join primitives of the decorrelation rule family (Neumann & Kemper,
/// BTW 2015): the residual dependent join itself ([#dependentJoin] — a
/// `CorrelatedJoinNode` the iterative engine feeds back to the rules, degenerating to a
/// cross join once the subquery no longer references the correlation), and the magic-set
/// decomposition ([#magicSetJoin]) with its input-cloning and correlation-rebinding
/// mechanics. One instance serves one rule invocation; the origin subquery is carried onto every
/// residual so an unsupported leftover reports the right source location.
public final class DependentJoinAlgebra
{
    private final Metadata metadata;
    private final CharVarcharCoercion charVarcharCoercion;
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final Lookup lookup;
    private final Node originSubquery;

    public DependentJoinAlgebra(Metadata metadata, CharVarcharCoercion charVarcharCoercion, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Lookup lookup, Node originSubquery)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.charVarcharCoercion = requireNonNull(charVarcharCoercion, "charVarcharCoercion is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.originSubquery = requireNonNull(originSubquery, "originSubquery is null");
    }

    /// Magic-set decomposition of a dependent join `left ⟗^D_{extra` dependent} (LEFT or
    /// INNER join-back):
    /// ```
    ///   left ⟗_{C ≡ C' AND extra} (D ⋈^D dependent[C→C']),   D = δ(π_{C'}(left'))
    /// ```
    /// Each `left` row sees its dependent rows (matched on the correlation columns via a
    /// NULL-safe `IS NOT DISTINCT FROM` plus the `extra` condition); a LEFT join-back
    /// null-extends rows with no match, an INNER join-back drops them. The LEFT form serves an
    /// outer [CorrelatedJoinNode] of type `LEFT` (with the correlated-join filter as
    /// `extra`) and a left/right outer join inside the subquery whose null-supplying side
    /// carries the correlation (with the join's ON predicate as `extra`; the correlation-free
    /// preserving side is cross-joined into `left` first). The INNER form serves pushdowns
    /// that cannot run against a bag input directly — a set operation that must match rows across
    /// branch clones, and an aggregation or window/limit above one — by evaluating them against the
    /// duplicate-free `D`.
    ///
    /// Two Trino-specific mechanics the abstract algebra does not need: the input is cloned (it is
    /// both `D`'s source and the join's left side, and a plan is a tree), and the dependent
    /// side's correlation references are rebound to the clone's symbols (symbols are
    /// single-definition). Joining back on the correlation columns (the paper's construction) —
    /// rather than a synthetic unique id — keeps the unique id out of any predicate, sidestepping
    /// `PredicatePushDown`; NULL correlation values are handled by the `IS NOT DISTINCT FROM` condition. Returns empty if the input can't be cloned or the rebind can't be verified.
    ///
    /// Distinct-D plus the IDENTICAL join-back means all IDENTICAL-equivalent correlation values
    /// share one subquery evaluation. For NaN this is fully consistent (grouping, join-back, and
    /// `=`-predicates use the same equivalence). The residual seam is `+0.0`/`-0.0`: they form
    /// one class, so a subquery that renders the value (e.g. `CAST(c AS varchar)`) sees one
    /// representative — consistent with the engine treating `=`-equal values as interchangeable,
    /// but observable on projection-correlated shapes.
    public Optional<PlanNode> magicSetJoin(JoinType joinType, PlanNode left, PlanNode dependent, List<Symbol> correlation, Expression extra, List<Symbol> dependentOutputs)
    {
        if (selectsCorrelation(dependentOutputs, correlation)) {
            return Optional.empty();
        }
        // Clone `left` for the distinct-correlation branch; fields = the correlation columns,
        // so getFields() returns their clones (C').
        Optional<NodeAndMappings> cloneOptional = copyInput(left, correlation);
        if (cloneOptional.isEmpty()) {
            return Optional.empty();
        }
        NodeAndMappings clone = cloneOptional.get();
        List<Symbol> clonedCorrelation = clone.getFields();

        // D = δ(π_{C'}(left')) — distinct correlation values, output = C' only.
        PlanNode distinctCorrelation = singleAggregation(
                idAllocator.getNextId(),
                clone.getNode(),
                ImmutableMap.of(),
                singleGroupingSet(clonedCorrelation));

        // Rebind the dependent side's correlation references C → C', then evaluate it per value.
        Optional<PlanNode> reboundDependent = rebindCorrelation(dependent, correlationMapping(correlation, clonedCorrelation));
        if (reboundDependent.isEmpty()) {
            return Optional.empty();
        }
        PlanNode dependentPerD = dependentJoin(distinctCorrelation, reboundDependent.get(), ImmutableSet.copyOf(clonedCorrelation));

        // LEFT-join `left` back on C ≡ C' (NULL-safe), plus the extra condition.
        Expression correlationMatch = correlationIdentical(correlation, clonedCorrelation);
        Expression condition = extra.equals(TRUE)
                ? correlationMatch
                : new Logical(Logical.Operator.AND, ImmutableList.of(correlationMatch, extra));

        return Optional.of(new JoinNode(
                idAllocator.getNextId(),
                joinType,
                left,
                dependentPerD,
                ImmutableList.of(),
                left.getOutputSymbols(),
                dependentOutputs,
                false,
                Optional.of(condition),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty()));
    }

    /// Shared tail of the scalar-global-aggregate paths: re-applies the projections stripped from
    /// above the aggregate (innermost first; each references the aggregate outputs — preserved —
    /// and may reference correlation, which survives in the decorrelated result), adding identities
    /// for the input columns so they pass through to the correlated-join output, then applies a
    /// non-trivial correlated-join filter (INNER-only) selecting which outer rows survive.
    public PlanNode reapplyProjections(PlanNode decorrelated, List<ProjectNode> projections, PlanNode input, Expression joinFilter)
    {
        PlanNode result = decorrelated;
        for (int i = projections.size() - 1; i >= 0; i--) {
            Assignments projectionAssignments = projections.get(i).getAssignments();
            Assignments.Builder augmented = Assignments.builder().putAll(projectionAssignments);
            for (Symbol symbol : input.getOutputSymbols()) {
                if (!projectionAssignments.outputs().contains(symbol)) {
                    augmented.putIdentity(symbol);
                }
            }
            result = new ProjectNode(idAllocator.getNextId(), result, augmented.build());
        }
        if (!joinFilter.equals(TRUE)) {
            result = new FilterNode(idAllocator.getNextId(), result, joinFilter);
        }
        return result;
    }

    public Expression correlationIdentical(List<Symbol> correlation, List<Symbol> clonedCorrelation)
    {
        ImmutableList.Builder<Expression> conjuncts = ImmutableList.builder();
        for (int i = 0; i < correlation.size(); i++) {
            conjuncts.add(comparison(metadata, charVarcharCoercion, IDENTICAL, correlation.get(i).toSymbolReference(), clonedCorrelation.get(i).toSymbolReference()));
        }
        return and(conjuncts.build());
    }

    /// True when a lifted join condition references only symbols the two join sides produce. A
    /// subquery correlated to two levels can carry, in a lifted predicate, a reference to an enclosing
    /// correlated join's input, which neither side provides; such a lift declines.
    public static boolean conditionAvailable(Expression condition, PlanNode left, PlanNode right)
    {
        Set<Symbol> available = ImmutableSet.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();
        return available.containsAll(SymbolsExtractor.extractUnique(condition));
    }

    /// True when a dependent side outputs a correlation symbol directly. A magic-set join back to
    /// the input would output that symbol from both sides, and the dependent side's copy is
    /// NULL-extended where the input's is not, so such a decomposition declines.
    public static boolean selectsCorrelation(List<Symbol> dependentOutputs, List<Symbol> correlation)
    {
        return dependentOutputs.stream().anyMatch(correlation::contains);
    }

    public static Map<Symbol, Symbol> correlationMapping(List<Symbol> from, List<Symbol> to)
    {
        ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();
        for (int i = 0; i < from.size(); i++) {
            mapping.put(from.get(i), to.get(i));
        }
        return mapping.buildOrThrow();
    }

    /// Clones the input subtree (the magic-set needs the input twice: as the distinct-correlation
    /// source and the join's left side). Declines when the input is not deterministic: the clone
    /// re-evaluates it, and a value that differs between the copies breaks every consumer — the
    /// magic-set joins the clone back on value equality, and set-operation distribution matches
    /// rows across branch clones by the input columns. Also declines when [PlanCopier] throws
    /// for a node type it doesn't implement (e.g. a `SemiJoinNode` or `MarkDistinctNode`
    /// in the outer plan). Either way the rule leaves the correlated join in place, and the shape
    /// fails as an unsupported correlated subquery unless another rule accepts it.
    public Optional<NodeAndMappings> copyInput(PlanNode input, List<Symbol> correlation)
    {
        if (!isDeterministic(input)) {
            return Optional.empty();
        }
        // The copy feeds only the distinct correlation values and is matched to the input by them, so
        // the copy may assign its own unique ids unless a correlation column is one.
        if (correlation.stream().anyMatch(uniqueIdColumns(input)::contains)) {
            return Optional.empty();
        }
        try {
            return Optional.of(PlanCopier.copyPlanAssigningNewUniqueIds(input, correlation, symbolAllocator, idAllocator, lookup));
        }
        catch (UnsupportedOperationException _) {
            return Optional.empty();
        }
    }

    /// The columns of the plan that hold unique ids, directly or through projections that select them.
    private Set<Symbol> uniqueIdColumns(PlanNode plan)
    {
        Set<Symbol> columns = new HashSet<>();
        PlanNodeSearcher.searchFrom(plan, lookup)
                .whereIsInstanceOfAny(AssignUniqueId.class)
                .findAll()
                .forEach(node -> columns.add(((AssignUniqueId) node).getIdColumn()));
        boolean added = true;
        while (added) {
            added = false;
            for (PlanNode node : PlanNodeSearcher.searchFrom(plan, lookup).whereIsInstanceOfAny(ProjectNode.class).findAll()) {
                for (Map.Entry<Symbol, Expression> assignment : ((ProjectNode) node).getAssignments().entrySet()) {
                    if (assignment.getValue() instanceof Reference reference && columns.contains(Symbol.from(reference))) {
                        added |= columns.add(assignment.getKey());
                    }
                }
            }
        }
        return columns;
    }

    /// True when re-evaluating the subtree is guaranteed to reproduce the same multiset of rows:
    /// every expression is deterministic, and no operator picks rows non-deterministically — a
    /// `SampleNode` draws an independent sample per evaluation, and an unordered
    /// `LimitNode` may keep a different row subset on a multi-split source. (An ordered
    /// `TopNNode` is accepted; its result is unstable only across ties, which the engine
    /// treats as interchangeable.)
    private boolean isDeterministic(PlanNode node)
    {
        if (PlanNodeSearcher.searchFrom(node, lookup).whereIsInstanceOfAny(SampleNode.class, LimitNode.class).matches()) {
            return false;
        }
        return ExpressionExtractor.extractExpressions(node, lookup).stream()
                .allMatch(DeterminismEvaluator::isDeterministic);
    }

    /// Rewrites every occurrence of the correlation symbols in the subquery — references in
    /// expressions, structural symbols (grouping keys, join outputs, unnest inputs), and projections
    /// that pass them through — to the clone's symbols, keeping the subquery's output symbols. A
    /// subquery that still defined a correlation symbol would produce the input's symbol with the
    /// clone's value, so a later join back to the input could no longer tell the two apart. When a
    /// reference to an old correlation symbol survives the rewrite (a carrier the rewrite does not
    /// cover, e.g. VALUES rows), this returns empty and the caller declines.
    public Optional<PlanNode> rebindCorrelation(PlanNode subquery, Map<Symbol, Symbol> mapping)
    {
        PlanNode resolved = resolveGroupReferences(subquery, lookup);
        // A reference to an enclosing correlated join's input (a subquery correlated to two levels)
        // is not rebound, and the evaluation per distinct correlation value cannot provide it.
        if (!dependsOnlyOn(resolved, mapping.keySet())) {
            return Optional.empty();
        }
        PlanNode rebound = UnaliasSymbolReferences.rebind(resolved, mapping, symbolAllocator, idAllocator);
        if (!Sets.intersection(SymbolsExtractor.extractUnique(rebound, lookup), mapping.keySet()).isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rebound);
    }

    /// True when the plan references no symbols but its own and the given ones.
    private boolean dependsOnlyOn(PlanNode plan, Set<Symbol> symbols)
    {
        List<Symbol> bound = ImmutableList.copyOf(symbols);
        PlanNode scope = new CorrelatedJoinNode(
                idAllocator.getNextId(),
                new ValuesNode(idAllocator.getNextId(), bound, ImmutableList.of()),
                plan,
                bound,
                JoinType.INNER,
                TRUE,
                originSubquery);
        try {
            ValidateDependenciesChecker.validate(scope);
            return true;
        }
        catch (IllegalArgumentException | IllegalStateException | UnsupportedOperationException _) {
            return false;
        }
    }

    // Some pushdowns (LIMIT/TopN) introduce helper columns (a unique id, a row number) that the
    // rule's output-equivalence check rejects. Restrict to the correlated join's declared
    // outputs, which downstream consumers expect.
    public PlanNode restrictOutputs(PlanNode plan, List<Symbol> outputs)
    {
        if (plan.getOutputSymbols().equals(outputs)) {
            return plan;
        }
        return new ProjectNode(idAllocator.getNextId(), plan, Assignments.identity(outputs));
    }

    /// The dependent join `input ⋈^D subquery` that remains after a rewrite step. When the subquery
    /// no longer references the correlation, the base equivalence degenerates it to a cross product
    /// on the spot (any correlation-using predicates have risen to filters above by then); otherwise
    /// a residual [CorrelatedJoinNode] is emitted for the iterative engine, where the next
    /// rule firing pushes it through one more operator. A residual no rule accepts stays a
    /// `CorrelatedJoinNode` and fails as an unsupported correlated subquery — the same error
    /// the monolithic pushdown's atomic bail produced.
    public PlanNode dependentJoin(PlanNode input, PlanNode subquery, Set<Symbol> correlation)
    {
        PlanNode resolved = lookup.resolve(subquery);
        Set<Symbol> usedInSubquery = SymbolsExtractor.extractUnique(resolved, lookup);
        if (Sets.intersection(usedInSubquery, correlation).isEmpty()) {
            return crossJoin(input, subquery);
        }
        return new CorrelatedJoinNode(
                idAllocator.getNextId(),
                input,
                subquery,
                ImmutableList.copyOf(correlation),
                JoinType.INNER,
                TRUE,
                originSubquery);
    }

    public JoinNode crossJoin(PlanNode left, PlanNode right)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                JoinType.INNER,
                left,
                right,
                ImmutableList.of(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    /// Re-applies a chain of stripped projections above `source`, augmenting each with
    /// identity assignments for `conditionInputs` so the join-condition columns stay in
    /// scope through the chain.
    public static PlanNode reapplyPassingConditionInputs(PlanNodeIdAllocator idAllocator, PlanNode source, List<ProjectNode> projections, Set<Symbol> conditionInputs)
    {
        PlanNode result = source;
        for (int i = projections.size() - 1; i >= 0; i--) {
            Assignments assignments = projections.get(i).getAssignments();
            Assignments.Builder augmented = Assignments.builder().putAll(assignments);
            for (Symbol symbol : result.getOutputSymbols()) {
                if (conditionInputs.contains(symbol) && !assignments.outputs().contains(symbol)) {
                    augmented.putIdentity(symbol);
                }
            }
            result = new ProjectNode(idAllocator.getNextId(), result, augmented.build());
        }
        return result;
    }
}
