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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnnestNode;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;

/// Pure plan-shape analyses shared by the dependent-join decorrelation rules: scalar and
/// existence shapes, distinctness and synthetic-id detection, and the deferral tests for shapes
/// other rules (or no rule) must own.
public final class CorrelatedSubqueryShapes
{
    private static final CatalogSchemaFunctionName BOOL_OR = builtinFunctionName("bool_or");

    private CorrelatedSubqueryShapes() {}

    /// Is the subquery a non-aggregate scalar — an `EnforceSingleRowNode` under optional
    /// projections? Such shapes belong exclusively to the scalar-subquery rule: the other rules skip
    /// them, so a sub-shape that rule defers on stays a `CorrelatedJoinNode` and fails as
    /// unsupported instead of being half-rewritten by a more generic form.
    public static boolean isScalarSubqueryShape(PlanNode subquery, Lookup lookup)
    {
        PlanNode node = lookup.resolve(subquery);
        while (node instanceof ProjectNode project) {
            node = lookup.resolve(project.getSource());
        }
        return node instanceof EnforceSingleRowNode;
    }

    /// True when the correlation symbols are referenced only inside `FilterNode` predicates
    /// anywhere in the subtree — never in a projection, aggregate argument, join condition, etc.
    /// This is precisely the correlation class the legacy `PlanNodeDecorrelator` can pull up.
    public static boolean correlationOnlyInFilters(PlanNode node, Set<Symbol> correlation, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);
        if (!(resolved instanceof FilterNode)
                && !Sets.intersection(SymbolsExtractor.extractUniqueNonRecursive(resolved), correlation).isEmpty()) {
            return false;
        }
        return resolved.getSources().stream().allMatch(source -> correlationOnlyInFilters(source, correlation, lookup));
    }

    /// True when the subtree is a chain of projections over an `UnnestNode` that unnests the
    /// correlation — the class the `Decorrelate(Inner|Left)UnnestWithGlobalAggregation` rules
    /// handle with a single join-free unnest. (Those rules do not recurse through filters, so a
    /// filter stops the walk here too and the shape stays with the framework.)
    public static boolean isUnnestWithGlobalAggregationShape(PlanNode node, Set<Symbol> correlation, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);
        while (resolved instanceof ProjectNode project) {
            resolved = lookup.resolve(project.getSource());
        }
        return resolved instanceof UnnestNode unnest && isSupportedCorrelatedUnnest(unnest, correlation, lookup);
    }

    /// True when the subquery is what `DecorrelateUnnest` rewrites to a single join-free
    /// unnest with ordinality-based bookkeeping: an optional top `EnforceSingleRowNode`, then
    /// projections and bounds (`LIMIT` including `WITH TIES`, `TopN`) over a
    /// supported correlated unnest. Mirrors that rule's searcher exactly (it does not recurse
    /// through filters, so a correlated filter inside keeps the shape with the framework).
    public static boolean isDecorrelateUnnestShape(PlanNode subquery, Set<Symbol> correlation, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(subquery);
        if (resolved instanceof EnforceSingleRowNode enforce) {
            resolved = lookup.resolve(enforce.getSource());
        }
        while (resolved instanceof ProjectNode
                || (resolved instanceof LimitNode limit && limit.getCount() > 0)
                || (resolved instanceof TopNNode topN && topN.getCount() > 0)) {
            resolved = lookup.resolve(resolved.getSources().getFirst());
        }
        return resolved instanceof UnnestNode unnest && isSupportedCorrelatedUnnest(unnest, correlation, lookup);
    }

    /// The unnest shape the shape-local unnest rules (`DecorrelateUnnest` and the
    /// `Decorrelate*UnnestWithGlobalAggregation` pair) support (their
    /// `isSupportedUnnest`): the unnested arrays are the correlation — correlation symbols
    /// directly, or symbols a projection computes from correlation only — over a single-row source
    /// with no replicate symbols.
    public static boolean isSupportedCorrelatedUnnest(UnnestNode unnest, Set<Symbol> correlation, Lookup lookup)
    {
        if (!unnest.getReplicateSymbols().isEmpty()
                || (unnest.getJoinType() != JoinType.INNER && unnest.getJoinType() != JoinType.LEFT)) {
            return false;
        }
        List<Symbol> unnestSymbols = unnest.getMappings().stream()
                .map(UnnestNode.Mapping::getInput)
                .collect(toImmutableList());
        PlanNode unnestSource = lookup.resolve(unnest.getSource());
        boolean basedOnCorrelation = correlation.containsAll(unnestSymbols)
                || (unnestSource instanceof ProjectNode projection
                && correlation.containsAll(SymbolsExtractor.extractUnique(projection.getAssignments().expressions())));
        return basedOnCorrelation && isScalar(unnest.getSource(), lookup);
    }

    /// True when a `JoinNode` inside the subquery compares, in its criteria or filter, a correlation
    /// symbol that one of its sources produces — i.e. the planner reuses the outer symbol as a column
    /// of a joined relation (e.g. `… JOIN (SELECT outer.col) r ON u.x = r.col`). The comparison
    /// then reads as one between the outer value and itself, which later rules simplify across the
    /// join sides into an invalid plan. Such shapes are declined and fail as unsupported (the legacy
    /// decorrelator also rejects them). A correlation symbol a join merely passes through is
    /// rebound like any other occurrence and is not matched.
    public static boolean correlationCarriedAsJoinOutput(PlanNode node, Set<Symbol> correlation, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);
        if (resolved instanceof JoinNode join) {
            Set<Symbol> produced = Sets.intersection(
                    ImmutableSet.<Symbol>builder()
                            .addAll(lookup.resolve(join.getLeft()).getOutputSymbols())
                            .addAll(lookup.resolve(join.getRight()).getOutputSymbols())
                            .build(),
                    correlation);
            if (!produced.isEmpty()) {
                Set<Symbol> compared = new HashSet<>(SymbolsExtractor.extractUniqueNonRecursive(join));
                join.getCriteria().forEach(clause -> {
                    compared.add(clause.getLeft());
                    compared.add(clause.getRight());
                });
                if (!Sets.intersection(produced, compared).isEmpty()) {
                    return true;
                }
            }
        }
        return resolved.getSources().stream().anyMatch(source -> correlationCarriedAsJoinOutput(source, correlation, lookup));
    }

    /// True when a `TopNNode`, or a `LimitNode` with ties, in the subquery orders by a correlation
    /// symbol, directly or through a projection that selects it. Such a key is constant within each
    /// outer row, so the bounded result has no meaningful per-outer-row order and is non-deterministic;
    /// Trino treats this as unsupported rather than picking arbitrary rows, so decline — the shape
    /// fails as unsupported, exactly as the legacy decorrelator treats it. (Ordering by the
    /// subquery's own symbols decorrelates fine and is not matched.)
    public static boolean topNOrdersByCorrelation(PlanNode node, Set<Symbol> correlation, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);
        Optional<OrderingScheme> ordering = switch (resolved) {
            case TopNNode topN -> Optional.of(topN.getOrderingScheme());
            case LimitNode limit when limit.isWithTies() -> limit.getTiesResolvingScheme();
            default -> Optional.empty();
        };
        if (ordering.isPresent()) {
            Set<Symbol> constant = correlationAliases(getOnlyElement(resolved.getSources()), correlation, lookup);
            if (ordering.get().orderBy().stream().anyMatch(constant::contains)) {
                return true;
            }
        }
        return resolved.getSources().stream().anyMatch(source -> topNOrdersByCorrelation(source, correlation, lookup));
    }

    /// The correlation symbols, and the symbols that projections over `node` assign them to directly.
    private static Set<Symbol> correlationAliases(PlanNode node, Set<Symbol> correlation, Lookup lookup)
    {
        ImmutableSet.Builder<Symbol> aliases = ImmutableSet.<Symbol>builder().addAll(correlation);
        PlanNode resolved = lookup.resolve(node);
        while (resolved instanceof ProjectNode project) {
            project.getAssignments().forEach((symbol, expression) -> {
                if (expression instanceof Reference reference && correlation.contains(Symbol.from(reference))) {
                    aliases.add(symbol);
                }
            });
            resolved = lookup.resolve(project.getSource());
        }
        return aliases.build();
    }

    /// True for a distinct operator — a pure dedup: no aggregate functions, one non-empty grouping
    /// set (mirrors `AggregationDecorrelation#isDistinctOperator`).
    public static boolean isDistinctOperator(AggregationNode node)
    {
        return node.getAggregations().isEmpty()
                && node.getGroupingSetCount() == 1
                && node.hasNonEmptyGroupingSet()
                && node.getStep() == AggregationNode.Step.SINGLE;
    }

    /// True for the existence aggregate — a single unmasked, unfiltered `bool_or(reference)`
    /// (what `TransformExistsApplyToCorrelatedJoin`'s default rewrite emits). It is
    /// null-insensitive: NULL over an empty group and over all-NULL inputs alike, so decorrelation
    /// needs no `non_null` mask when the argument null-extends with the join. Mirrors the
    /// legacy `TransformCorrelatedGlobalAggregation*` check.
    public static boolean isExistenceAggregation(AggregationNode node)
    {
        if (node.getAggregations().size() != 1) {
            return false;
        }
        AggregationNode.Aggregation aggregation = getOnlyElement(node.getAggregations().values());
        if (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()) {
            return false;
        }
        return aggregation.getResolvedFunction().name().equals(BOOL_OR)
                && aggregation.getArguments().getFirst() instanceof Reference;
    }

    /// True when the relation is duplicate-free by construction — a distinct aggregation (no
    /// aggregate functions, single grouping set), the shape of the magic-set's distinct correlation
    /// set `D`. Such an input needs no synthetic per-row id (its own columns identify a row),
    /// which also keeps a set operation below distributable: clones of a distinct aggregation have
    /// identical content, while clones of `AssignUniqueId` re-generate their ids.
    public static boolean isDistinctRelation(PlanNode node, Lookup lookup)
    {
        return lookup.resolve(node) instanceof AggregationNode aggregation
                && aggregation.getAggregations().isEmpty()
                && aggregation.getGroupingSetCount() == 1
                && aggregation.getStep() == AggregationNode.Step.SINGLE;
    }

    /// True when the subtree contains an `AssignUniqueId`. Its ids identify rows only within
    /// one execution of the node — a clone re-generates them — so an input carrying one cannot
    /// participate in any rewrite that matches its rows across branch clones (set-op distribution).
    public static boolean containsAssignUniqueId(PlanNode node, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);
        if (resolved instanceof AssignUniqueId) {
            return true;
        }
        return resolved.getSources().stream().anyMatch(source -> containsAssignUniqueId(source, lookup));
    }

    /// True when a set operation sits on the correlated path of the subtree — i.e. the pushdown
    /// would reach it and distribute the input over its branches. The walk stops where a subtree no
    /// longer references correlation, mirroring the cross-join base case of [DependentJoinAlgebra#dependentJoin].
    public static boolean containsCorrelatedSetOperation(PlanNode node, Set<Symbol> correlation, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);
        if (Sets.intersection(SymbolsExtractor.extractUnique(resolved, lookup), correlation).isEmpty()) {
            return false;
        }
        if (resolved instanceof SetOperationNode) {
            return true;
        }
        return resolved.getSources().stream().anyMatch(source -> containsCorrelatedSetOperation(source, correlation, lookup));
    }

    public static boolean isScalarGlobalAggregation(PlanNode subquery, Lookup lookup)
    {
        PlanNode node = lookup.resolve(subquery);
        while (node instanceof ProjectNode project) {
            node = lookup.resolve(project.getSource());
        }
        // A *scalar* global aggregate is a single global grouping set (the empty set). An
        // aggregation with several grouping sets one of which happens to be empty (GROUPING SETS)
        // is not scalar — it must not take the non_null-mask path.
        return node instanceof AggregationNode aggregation
                && aggregation.getGroupingSetCount() == 1
                && aggregation.hasEmptyGroupingSet();
    }
}
