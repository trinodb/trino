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
package io.trino.sql.planner.iterative.rule.fuse;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.LogicalExpression.or;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implements simplified version of fusion operation from the
 * "Computation Reuse via Fusion in Amazon Athena" paper.
 */
public class PlanNodeFuser
{
    private final Rule.Context context;

    public static Optional<FusedPlanNode> fuse(Rule.Context context, PlanNode left, PlanNode right)
    {
        return new PlanNodeFuser(context).fuse(left, right).map(fused -> {
            PlanNode result = fused.plan();

            // verify if some symbol need to be aliased in the result
            Map<Symbol, SymbolReference> additionalProjections = new HashMap<>();
            Set<Symbol> resultOutputSymbols = ImmutableSet.copyOf(result.getOutputSymbols());
            for (Symbol originalRightOutputSymbol : right.getOutputSymbols()) {
                if (!resultOutputSymbols.contains(originalRightOutputSymbol)) {
                    additionalProjections.put(originalRightOutputSymbol, fused.symbolMapping().requiredMap(originalRightOutputSymbol).toSymbolReference());
                }
            }
            if (!additionalProjections.isEmpty()) {
                result = new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        result,
                        Assignments.builder()
                                .putIdentities(result.getOutputSymbols())
                                .putAll(additionalProjections)
                                .build());
            }
            return new FusedPlanNode(result, fused.symbolMapping(), fused.leftFilter(), fused.rightFilter());
        });
    }

    private PlanNodeFuser(Rule.Context context)
    {
        this.context = requireNonNull(context, "context is null");
    }

    private Optional<FusedPlanNode> fuse(PlanNode left, PlanNode right)
    {
        left = context.getLookup().resolve(left);
        right = context.getLookup().resolve(right);

        if (left instanceof FilterNode || right instanceof FilterNode) {
            return fuseFilter(left, right);
        }

        if (left instanceof TableScanNode leftScan && right instanceof TableScanNode rightScan) {
            return fuseTableScan(leftScan, rightScan);
        }

        if (left instanceof AggregationNode leftAggregation && right instanceof AggregationNode rightAggregation) {
            return fuseAggregation(leftAggregation, rightAggregation);
        }

        return Optional.empty();
    }

    private Optional<FusedPlanNode> fuseFilter(PlanNode left, PlanNode right)
    {
        if (left instanceof FilterNode leftFilter && right instanceof FilterNode rightFilter) {
            return fuseFilter(leftFilter.getSource(), leftFilter.getPredicate(), rightFilter.getSource(), rightFilter.getPredicate());
        }
        if (left instanceof FilterNode leftFilter) {
            return fuseFilter(leftFilter.getSource(), leftFilter.getPredicate(), right, TRUE_LITERAL);
        }
        if (right instanceof FilterNode rightFilter) {
            return fuseFilter(left, TRUE_LITERAL, rightFilter.getSource(), rightFilter.getPredicate());
        }

        throw new IllegalArgumentException(format("expected at least one FilterNode but got %s and %s", left, right));
    }

    /**
     * Fuses two filter nodes.
     * <p>
     * Consider F1=Filter(leftSource, leftPredicate) and F2=Filter(rightSource, rightPredicate).
     * We first recursively fuse the sub-plans.
     * If this is not successful, we return {@link Optional#empty()}.
     * Else, if Fuse(leftSource, rightSource)=(P, M, L, R) we define Fuse(F1, F2) as:
     * {@code (Filter(P, leftPredicate OR M(rightPredicate)), M, L AND leftPredicate, R AND M(rightPredicate))}
     * Note that if leftPredicate is equivalent to M(rightPredicate), we simplify the fused
     * result as (Filter(P, leftPredicate), M, L, R).
     * </p>
     * <p>
     * As an example, fusing two query fragments that scan the same table with different filters:
     * <pre>
     * SELECT i_item_desc
     * FROM item
     * WHERE i_category = 'Music' AND i_brand_id > 1000
     *
     * SELECT i_item_desc
     * FROM item
     * WHERE i_category = 'Music' AND i_brand_id < 50
     * </pre>
     * results in (P, ∅, L, R), where:
     * <pre>
     * P: SELECT i_item_desc FROM item
     *  WHERE i_category = 'Music' AND
     *  (i_brand_id < 50 OR i_brand_id > 1000)
     * L: i_brand_id > 1000 AND i_category = 'Music'
     * R: i_brand_id < 50 AND i_category = 'Music'
     * </pre>
     * </p>
     */
    private Optional<FusedPlanNode> fuseFilter(PlanNode leftSource, Expression leftPredicate, PlanNode rightSource, Expression rightPredicate)
    {
        Optional<FusedPlanNode> maybeFusedSource = fuse(leftSource, rightSource);
        if (maybeFusedSource.isEmpty()) {
            return Optional.empty();
        }
        FusedPlanNode fusedSource = maybeFusedSource.get();

        Expression mappedRightPredicate = fusedSource.symbolMapping().map(rightPredicate);
        if (leftPredicate.equals(mappedRightPredicate)) {
            // since the predicates match, and are applied in the fused FilterNode as predicate,
            // they don't have to be added to the left and right filters.
            return Optional.of(new FusedPlanNode(
                    new FilterNode(
                            context.getIdAllocator().getNextId(),
                            fusedSource.plan(),
                            leftPredicate),
                    fusedSource.symbolMapping(),
                    fusedSource.leftFilter(),
                    fusedSource.rightFilter()));
        }

        PlanNode fused;
        if (leftPredicate.equals(TRUE_LITERAL) || mappedRightPredicate.equals(TRUE_LITERAL)) {
            // Normally we would add FilterNode with (leftPredicate or rightPredicate) predicate but since
            // this evaluates to TRUE if either side is TRUE we can drop FilterNode altogether
            fused = fusedSource.plan();
        }
        else {
            fused = new FilterNode(
                    context.getIdAllocator().getNextId(),
                    fusedSource.plan(),
                    or(leftPredicate, mappedRightPredicate));
        }
        return Optional.of(new FusedPlanNode(
                fused,
                fusedSource.symbolMapping(),
                and(fusedSource.leftFilter(), leftPredicate),
                and(fusedSource.rightFilter(), mappedRightPredicate)));
    }

    /**
     * Fused two global aggregation nodes.
     * <p>
     * Consider left=GroupBy(leftAggregations, leftSource), where leftAggregations is the list
     * of aggregate functions [ci:=(ai,mi)] (ai is the aggregate function and mi is the mask),
     * and analogously consider right=GroupBy(rightAggregations, rightSource).
     * If the fuse(leftSource, rightSource) returns {@link Optional#empty()} we return {@link Optional#empty()}.
     * Otherwise, fuse(leftSource, rightSource) returns (P, M, L, R).
     * We initialize the new mapping Mnew = M, and assemble the new
     * aggregations Anew. To that end, for each ci:=(ai,mi) in {@code leftAggregations}, we
     * add to Anew an aggregation with a tighter mask ci:=(ai, mi AND L).
     * Then, for each cj:=(aj,mj) in A2, we check whether (M(aj), M(mj AND R)) already exists in Anew.
     * If it does (and is assigned to some variable cnew) we add cj→cnew to the new mapping Mnew.
     * If it does not, we add a new aggregate cj :=(M(aj), M(mj AND R)) to Anew.
     * <p>
     * The result then is equal to {@code (GroupBy(Anew, P), Mnew, TRUE, TRUE)}.
     * </p>
     * <p>
     * An example:
     * <pre>
     *  SELECT
     *  MIN(i_brand_id) AS mi
     * FROM item
     * WHERE i_color = 'red'
     *
     * SELECT
     *  AVG(i_category_id) AS avgc
     * FROM item
     * </pre>
     * <p>
     * results in (P, ∅, TRUE, TRUE) where:
     * <pre>
     * P: SELECT
     *  MIN(i_brand_id) FILTER (WHERE i_color = 'red') AS mi,
     *  AVG(i_category_id) AS avgc
     * FROM item
     * </pre>
     * </p>
     */
    private Optional<FusedPlanNode> fuseAggregation(AggregationNode left, AggregationNode right)
    {
        if (!left.hasSingleGlobalAggregation() || !right.hasSingleGlobalAggregation()) {
            // we only handle global aggregations at the moment
            return Optional.empty();
        }

        // for now, do not support distinct aggregations
        if (left.getAggregations().values().stream().anyMatch(Aggregation::isDistinct)) {
            return Optional.empty();
        }
        if (right.getAggregations().values().stream().anyMatch(Aggregation::isDistinct)) {
            return Optional.empty();
        }

        // since fusing must be run before AddExchanges and thus before rules that create partial aggregations, we can assume that aggregation is single
        checkState(left.getStep().equals(SINGLE), "expected SINGLE aggregation in the left node but got %s", left.getStep());
        checkState(right.getStep().equals(SINGLE), "expected SINGLE aggregation in the right node but got %s", right.getStep());
        if (!left.getPreGroupedSymbols().equals(right.getPreGroupedSymbols()) ||
                !left.getHashSymbol().equals(right.getHashSymbol()) ||
                !left.getGroupIdSymbol().equals(right.getGroupIdSymbol())) {
            return Optional.empty();
        }

        Optional<FusedPlanNode> maybeFusedSource = fuse(left.getSource(), right.getSource());
        if (maybeFusedSource.isEmpty()) {
            return Optional.empty();
        }
        FusedPlanNode fusedSource = maybeFusedSource.get();
        Map<Symbol, Aggregation> rewrittenLeftAggregations = new HashMap<>();
        Map<Symbol, Expression> additionalAggregationMasks = new HashMap<>();
        // rewrite left aggregations using fused leftFilter
        for (Map.Entry<Symbol, Aggregation> aggregationEntry : left.getAggregations().entrySet()) {
            Aggregation aggregation = aggregationEntry.getValue();
            Optional<Symbol> aggregationMask = aggregation.getMask();
            if (!fusedSource.leftFilter().equals(TRUE_LITERAL)) {
                Expression newMask = aggregation.getMask()
                        .map(filter -> and(filter.toSymbolReference(), fusedSource.leftFilter()))
                        .orElse(fusedSource.leftFilter());
                Symbol newMaskSymbol = context.getSymbolAllocator().newSymbol("aggr_mask", BOOLEAN);
                additionalAggregationMasks.put(newMaskSymbol, newMask);
                aggregationMask = Optional.of(newMaskSymbol);
            }

            rewrittenLeftAggregations.put(aggregationEntry.getKey(), new Aggregation(
                    aggregation.getResolvedFunction(),
                    aggregation.getArguments(),
                    aggregation.isDistinct(),
                    aggregation.getFilter(),
                    aggregation.getOrderingScheme(),
                    aggregationMask));
        }

        FusedSymbolMapping.Builder newMapping = FusedSymbolMapping.builder().withMapping(fusedSource.symbolMapping());

        // rewrite right aggregations using fused rightFilter or use matching left aggregation
        HashMap<Symbol, Aggregation> rightAggregations = new HashMap<>();
        for (Map.Entry<Symbol, Aggregation> aggregationEntry : right.getAggregations().entrySet()) {
            Aggregation rightAggregation = aggregationEntry.getValue();

            Optional<Symbol> mappedFilter = rightAggregation.getFilter().map(fusedSource.symbolMapping()::map);
            List<Expression> mappedArguments = rightAggregation.getArguments().stream().map(fusedSource.symbolMapping()::map).collect(toImmutableList());
            Optional<Symbol> mappedMask = rightAggregation.getMask().map(fusedSource.symbolMapping()::map);

            Optional<Expression> newMask = Optional.empty();
            if (!fusedSource.rightFilter().equals(TRUE_LITERAL)) {
                newMask = Optional.of(mappedMask
                        .map(mask -> and(mask.toSymbolReference(), fusedSource.rightFilter()))
                        .orElse(fusedSource.rightFilter()));
            }

            // find mappedAggregation in leftAggregations
            boolean found = false;
            for (Map.Entry<Symbol, Aggregation> entry : rewrittenLeftAggregations.entrySet()) {
                Aggregation leftAggregation = entry.getValue();
                if (leftAggregation.getResolvedFunction().equals(rightAggregation.getResolvedFunction()) &&
                        leftAggregation.getArguments().equals(mappedArguments) &&
                        leftAggregation.isDistinct() == rightAggregation.isDistinct() &&
                        leftAggregation.getOrderingScheme().equals(rightAggregation.getOrderingScheme()) &&
                        leftAggregation.getFilter().equals(mappedFilter) &&
                        maskMatches(leftAggregation.getMask(), additionalAggregationMasks, mappedMask, newMask)) {
                    newMapping.add(aggregationEntry.getKey(), entry.getKey());
                    found = true;
                    break;
                }
            }

            if (!found) {
                // only add aggregation from the right side, if it's not already on the left
                Optional<Symbol> aggregationMask = mappedMask;
                if (newMask.isPresent()) {
                    // non-trivial right filter, we need to add mask that aggregation will filter on
                    Symbol newMaskSymbol = context.getSymbolAllocator().newSymbol("aggr_mask", BOOLEAN);
                    additionalAggregationMasks.put(newMaskSymbol, newMask.get());
                    aggregationMask = Optional.of(newMaskSymbol);
                }

                rightAggregations.put(aggregationEntry.getKey(), new Aggregation(
                        rightAggregation.getResolvedFunction(),
                        mappedArguments,
                        rightAggregation.isDistinct(),
                        mappedFilter,
                        rightAggregation.getOrderingScheme(),
                        aggregationMask));
            }
        }

        PlanNode source = fusedSource.plan();
        if (!additionalAggregationMasks.isEmpty()) {
            // if we have some additionalAggregationMasks we need projection to produce those masks
            source = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    source,
                    Assignments.builder()
                            .putIdentities(source.getOutputSymbols())
                            .putAll(additionalAggregationMasks)
                            .build());
        }

        return Optional.of(new FusedPlanNode(
                new AggregationNode(
                        left.getId(),
                        source,
                        ImmutableMap.<Symbol, Aggregation>builder()
                                .putAll(rewrittenLeftAggregations)
                                .putAll(rightAggregations)
                                .buildOrThrow(),
                        left.getGroupingSets(),
                        left.getPreGroupedSymbols(),
                        SINGLE,
                        left.getHashSymbol(),
                        left.getGroupIdSymbol()),
                newMapping.build(),
                TRUE_LITERAL,
                TRUE_LITERAL));
    }

    private static boolean maskMatches(Optional<Symbol> maskSymbol, Map<Symbol, Expression> masks, Optional<Symbol> otherMaskSymbol, Optional<Expression> otherMask)
    {
        Optional<Expression> mask = maskSymbol.map(masks::get);
        if (mask.isEmpty()) {
            // no fused mask, symbols must match if any
            return otherMask.isEmpty() && maskSymbol.equals(otherMaskSymbol);
        }
        return mask.equals(otherMask);
    }

    /**
     * Fuses two table scan nodes.
     * Fusing two table scans succeeds only if both scan the same table (T).
     * The fused result is defined as:
     * (Scan(T), columnMap(right, left), TRUE, TRUE)
     * where columnMap(right, left) maps right symbol to the existing left symbol if it exists.
     *
     * <p>
     * As an example, fusing two query fragments that scan the same table:
     * <pre>
     * SELECT i_item_sk AS sk, i_brand AS brand
     * FROM item
     *
     * SELECT i_brand AS brand2, i_size AS size
     * FROM item
     * </pre>
     * results in a fused result (P, M, TRUE, TRUE) where:
     * <pre>
     * P: SELECT
     *   i_item_sk AS sk,
     *   i_brand AS brand,
     *   i_size AS size
     * FROM item
     * M: brand2 → brand
     * </pre>
     * </p>
     */
    private Optional<FusedPlanNode> fuseTableScan(TableScanNode left, TableScanNode right)
    {
        if (!left.getTable().equals(right.getTable())) {
            return Optional.empty();
        }

        // the fusing rules have to be run before predicate push-down into table scan
        checkState(left.getEnforcedConstraint().isAll(), "Left node has non trivial enforcedConstraint %s, %s", left.getEnforcedConstraint(), left);
        checkState(right.getEnforcedConstraint().isAll(), "Right node has non trivial enforcedConstraint %s, %s", right.getEnforcedConstraint(), right);
        checkState(left.getUseConnectorNodePartitioning().equals(right.getUseConnectorNodePartitioning()),
                "Fused table scans must have equal useConnectorNodePartitioning but got left %s in %s, right %s in %s",
                left.getUseConnectorNodePartitioning(),
                left, right.getUseConnectorNodePartitioning(),
                right);
        checkState(left.getStatistics().isEmpty(), "Left node has non empty statistics %s, %s", left.getStatistics(), left);
        checkState(right.getStatistics().isEmpty(), "Right node has non empty statistics %s, %s", right.getStatistics(), right);

        if (!left.isUpdateTarget() == right.isUpdateTarget()) {
            return Optional.empty();
        }

        Set<Symbol> outputs = new HashSet<>(left.getOutputSymbols());
        Map<Symbol, ColumnHandle> assignments = new HashMap<>(left.getAssignments());
        FusedSymbolMapping.Builder symbolMappingBuilder = FusedSymbolMapping.builder();
        Set<Symbol> rightOutputSymbols = ImmutableSet.copyOf(right.getOutputSymbols());

        // add unique assignments and outputs from the right side or map to the left side symbols
        Map<ColumnHandle, Symbol> leftInverseAssignments = ImmutableBiMap.copyOf(left.getAssignments()).inverse();
        right.getAssignments().forEach((rightSymbol, columnHandle) -> {
            Symbol leftSymbol = leftInverseAssignments.get(columnHandle);
            if (leftSymbol != null) {
                // left assignment found, add the mapping and output symbol if necessary
                symbolMappingBuilder.add(rightSymbol, leftSymbol);
                if (rightOutputSymbols.contains(rightSymbol)) {
                    outputs.add(leftSymbol);
                }
            }
            else {
                // left assignment not found, add a new assignment and output symbol if necessary
                assignments.put(rightSymbol, columnHandle);
                if (rightOutputSymbols.contains(rightSymbol)) {
                    outputs.add(rightSymbol);
                }
            }
        });
        return Optional.of(new FusedPlanNode(
                new TableScanNode(
                        left.getId(),
                        left.getTable(),
                        ImmutableList.copyOf(outputs),
                        assignments,
                        left.getEnforcedConstraint(),
                        Optional.empty(),
                        left.isUpdateTarget(),
                        left.getUseConnectorNodePartitioning()),
                symbolMappingBuilder.build(),
                TRUE_LITERAL,
                TRUE_LITERAL));
    }

    private Expression and(Expression left, Expression right)
    {
        if (left.equals(TRUE_LITERAL)) {
            return right;
        }
        if (right.equals(TRUE_LITERAL)) {
            return left;
        }
        return LogicalExpression.and(left, right);
    }

    /**
     * Fusion result of two plan nodes P1 and P2.
     *
     * @param plan Fused resulting plan. Output symbols contain all output symbols from P1 and all additional output symbols from P2.
     * @param symbolMapping mapping of output symbols from P2 duplicated in P1
     * @param leftFilter predicate using {@code plan} output symbols to restore P1
     * @param rightFilter predicate using {@code plan} output symbols to restore P2
     */
    public record FusedPlanNode(PlanNode plan, FusedSymbolMapping symbolMapping, Expression leftFilter, Expression rightFilter)
    {
        public FusedPlanNode(PlanNode plan, FusedSymbolMapping symbolMapping, Expression leftFilter, Expression rightFilter)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.symbolMapping = requireNonNull(symbolMapping, "symbolMapping is null");
            this.leftFilter = requireNonNull(leftFilter, "leftFilter is null");
            this.rightFilter = requireNonNull(rightFilter, "rightFilter is null");
        }
    }

    public static class FusedSymbolMapping
    {
        private final Map<Symbol, Symbol> mapping;
        private final SymbolMapper symbolMapper;

        public FusedSymbolMapping(Map<Symbol, Symbol> mapping)
        {
            this.mapping = requireNonNull(mapping, "mapping is null");
            this.symbolMapper = new SymbolMapper(this::map);
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public Symbol map(Symbol symbol)
        {
            Symbol mapped = mapping.get(symbol);
            return mapped != null ? mapped : symbol;
        }

        public Symbol requiredMap(Symbol symbol)
        {
            return requireNonNull(mapping.get(symbol), () -> format("symbol '%s' not found in %s", symbol, this));
        }

        public Expression map(Expression argument)
        {
            return symbolMapper.map(argument);
        }

        public static class Builder
        {
            private final ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();

            public FusedSymbolMapping build()
            {
                return new FusedSymbolMapping(mapping.buildOrThrow());
            }

            public Builder withMapping(FusedSymbolMapping... mappings)
            {
                for (FusedSymbolMapping fusedSymbolMapping : mappings) {
                    mapping.putAll(fusedSymbolMapping.mapping);
                }

                return this;
            }

            public void add(Symbol from, Symbol to)
            {
                mapping.put(from, to);
            }
        }
    }
}
