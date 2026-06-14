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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.PlanCopier;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.Patterns.aggregation;

/**
 * Transforms plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT a0, a1, ...)
 *        F2(DISTINCT b0, b1, ...)
 *        F3(DISTINCT c0, c1, ...)
 *     - X
 * </pre>
 * into
 * <pre>
 * - Join
 *     on left.k = right.k
 *     - Aggregation
 *         GROUP BY (k)
 *         F1(DISTINCT a0, a1, ...)
 *         F2(DISTINCT b0, b1, ...)
 *       - X
 *     - Aggregation
 *         GROUP BY (k)
 *         F3(DISTINCT c0, c1, ...)
 *       - X
 * </pre>
 * <p>
 * This improves plan parallelism and allows {@link SingleDistinctAggregationToGroupBy} to optimize the single input distinct aggregation further.
 * The cost is we calculate X and GROUP BY (k) multiple times, so this rule is only beneficial if the calculations are cheap compared to
 * other distinct aggregation strategies.
 */
public class MultipleDistinctAggregationsToSubqueries
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MultipleDistinctAggregationsToSubqueries::isAggregationCandidateForSplittingToSubqueries);

    // In addition to this check, DistinctAggregationController.isAggregationSourceSupportedForSubqueries, that accesses Metadata,
    // needs also pass, for the plan to be applicable for this rule,
    public static boolean isAggregationCandidateForSplittingToSubqueries(AggregationNode aggregationNode)
    {
        // TODO: we could support non-distinct aggregations if SingleDistinctAggregationToGroupBy supports it
        return SingleDistinctAggregationToGroupBy.allDistinctAggregates(aggregationNode) &&
                OptimizeMixedDistinctAggregations.hasMultipleDistincts(aggregationNode) &&
                // if we have more than one grouping set, we can have duplicated grouping sets and handling this is complex
                aggregationNode.getGroupingSetCount() == 1;
    }

    private final DistinctAggregationStrategyChooser distinctAggregationStrategyChooser;

    public MultipleDistinctAggregationsToSubqueries(TaskCountEstimator taskCountEstimator, Metadata metadata)
    {
        this.distinctAggregationStrategyChooser = createDistinctAggregationStrategyChooser(taskCountEstimator, metadata);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        if (!distinctAggregationStrategyChooser.shouldSplitToSubqueries(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())) {
            return Result.empty();
        }

        // Null-safe join on the grouping keys requires a non-null sentinel value per key type
        // (see buildJoin for details). If any grouping key has a type without a sentinel, skip
        // the rewrite: a cross join would be required to preserve NULL groups, and that is
        // not obviously better than the other distinct aggregation strategies.
        for (Symbol groupingKey : aggregationNode.getGroupingKeys()) {
            if (nullSentinel(groupingKey.type()).isEmpty()) {
                return Result.empty();
            }
        }

        // group aggregations by arguments
        Map<Set<Expression>, Map<Symbol, Aggregation>> aggregationsByArguments = new LinkedHashMap<>(aggregationNode.getAggregations().size());
        // sort the aggregation by output symbol to have consistent join layout
        List<Entry<Symbol, Aggregation>> sortedAggregations = aggregationNode.getAggregations().entrySet()
                .stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().name()))
                .collect(toImmutableList());
        for (Entry<Symbol, Aggregation> entry : sortedAggregations) {
            aggregationsByArguments.compute(ImmutableSet.copyOf(entry.getValue().getArguments()), (_, current) -> {
                if (current == null) {
                    current = new HashMap<>();
                }
                current.put(entry.getKey(), entry.getValue());
                return current;
            });
        }

        PlanNode right = null;
        List<Symbol> rightJoinSymbols = null;
        Assignments.Builder assignments = Assignments.builder();
        List<Map<Symbol, Aggregation>> aggregationsByArgumentsList = ImmutableList.copyOf(aggregationsByArguments.values());
        for (int i = aggregationsByArgumentsList.size() - 1; i > 0; i--) {
            // go from right to left and build the right side of the join
            Map<Symbol, Aggregation> aggregations = aggregationsByArgumentsList.get(i);
            AggregationNode subAggregationNode = buildSubAggregation(aggregationNode, aggregations, assignments, context);

            if (right == null) {
                right = subAggregationNode;
                rightJoinSymbols = subAggregationNode.getGroupingKeys();
            }
            else {
                right = buildJoin(subAggregationNode, subAggregationNode.getGroupingKeys(), right, rightJoinSymbols, context);
            }
        }

        // the first aggregation is the left side of the top join
        AggregationNode left = buildSubAggregation(aggregationNode, aggregationsByArgumentsList.getFirst(), assignments, context);

        for (int i = 0; i < left.getGroupingKeys().size(); i++) {
            assignments.put(aggregationNode.getGroupingKeys().get(i), left.getGroupingKeys().get(i).toSymbolReference());
        }
        JoinNode topJoin = buildJoin(left, left.getGroupingKeys(), right, rightJoinSymbols, context);
        ProjectNode result = new ProjectNode(aggregationNode.getId(), topJoin, assignments.build());
        return Result.ofPlanNode(result);
    }

    private AggregationNode buildSubAggregation(AggregationNode aggregationNode, Map<Symbol, Aggregation> aggregations, Assignments.Builder assignments, Context context)
    {
        List<Symbol> originalAggregationOutputSymbols = ImmutableList.copyOf(aggregations.keySet());
        // copy the plan so that both plan node ids and symbols are not duplicated between sub aggregations
        NodeAndMappings copied = PlanCopier.copyPlan(
                AggregationNode.builderFrom(aggregationNode).setAggregations(aggregations).build(),
                originalAggregationOutputSymbols,
                context.getSymbolAllocator(),
                context.getIdAllocator(),
                context.getLookup());
        AggregationNode subAggregationNode = (AggregationNode) copied.getNode();
        // add the mapping from the new output symbols to original ones
        for (int i = 0; i < originalAggregationOutputSymbols.size(); i++) {
            assignments.put(originalAggregationOutputSymbols.get(i), copied.getFields().get(i).toSymbolReference());
        }
        return subAggregationNode;
    }

    private JoinNode buildJoin(PlanNode left, List<Symbol> leftJoinSymbols, PlanNode right, List<Symbol> rightJoinSymbols, Context context)
    {
        checkArgument(leftJoinSymbols.size() == rightJoinSymbols.size());

        if (leftJoinSymbols.isEmpty()) {
            // Global aggregation: both sides produce exactly one row, so a plain cross join suffices.
            // TODO: we dont need dynamic filters for this join at all. We could add skipDf field to the JoinNode and make use of it in PredicatePushDown
            return new JoinNode(
                    context.getIdAllocator().getNextId(),
                    INNER,
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

        // The join must match NULL grouping-key rows correctly. GROUP BY collapses NULL
        // values into a single group, but SQL `=` (EquiJoinClause) evaluates NULL = NULL
        // as UNKNOWN, dropping the NULL group.
        //
        // To preserve NULL groups while keeping hash-join O(N) performance, each grouping
        // key k is projected to two non-nullable columns:
        //   k_is_null  = (k IS NULL)                            -- BOOLEAN, always non-null
        //   k_coalesced = COALESCE(k, <type_zero>)              -- same type as k, always non-null
        //
        // The join criteria are then (k_is_null_left = k_is_null_right) AND
        // (k_coalesced_left = k_coalesced_right) for each key.  No collision is possible:
        // null rows give (TRUE, zero) while a row with k = zero gives (FALSE, zero).
        //
        // apply() already guarantees that nullSentinel() is present for every grouping key
        // type; otherwise the rule bails out.
        ImmutableList.Builder<EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        Assignments.Builder leftNullSafeKeyProjections = Assignments.builder();
        Assignments.Builder rightNullSafeKeyProjections = Assignments.builder();
        for (int i = 0; i < leftJoinSymbols.size(); i++) {
            Symbol leftKey = leftJoinSymbols.get(i);
            Symbol rightKey = rightJoinSymbols.get(i);
            Object sentinel = nullSentinel(leftKey.type())
                    .orElseThrow(() -> new IllegalStateException("Missing null sentinel for grouping key type: " + leftKey.type()));

            Symbol leftIsNullKey = context.getSymbolAllocator().newSymbol("null_key", BOOLEAN);
            Symbol rightIsNullKey = context.getSymbolAllocator().newSymbol("null_key", BOOLEAN);
            Symbol leftCoalescedKey = context.getSymbolAllocator().newSymbol("coalesced_key", leftKey.type());
            Symbol rightCoalescedKey = context.getSymbolAllocator().newSymbol("coalesced_key", rightKey.type());

            leftNullSafeKeyProjections.put(leftIsNullKey, new IsNull(leftKey.toSymbolReference()));
            leftNullSafeKeyProjections.put(leftCoalescedKey, new Coalesce(ImmutableList.of(leftKey.toSymbolReference(), new Constant(leftKey.type(), sentinel))));
            rightNullSafeKeyProjections.put(rightIsNullKey, new IsNull(rightKey.toSymbolReference()));
            rightNullSafeKeyProjections.put(rightCoalescedKey, new Coalesce(ImmutableList.of(rightKey.toSymbolReference(), new Constant(rightKey.type(), sentinel))));

            criteriaBuilder.add(new EquiJoinClause(leftIsNullKey, rightIsNullKey));
            criteriaBuilder.add(new EquiJoinClause(leftCoalescedKey, rightCoalescedKey));
        }

        PlanNode leftProjected = new ProjectNode(
                context.getIdAllocator().getNextId(),
                left,
                Assignments.builder().putIdentities(left.getOutputSymbols()).putAll(leftNullSafeKeyProjections.build()).build());
        PlanNode rightProjected = new ProjectNode(
                context.getIdAllocator().getNextId(),
                right,
                Assignments.builder().putIdentities(right.getOutputSymbols()).putAll(rightNullSafeKeyProjections.build()).build());
        // TODO: we dont need dynamic filters for this join at all. We could add skipDf field to the JoinNode and make use of it in PredicatePushDown
        return new JoinNode(
                context.getIdAllocator().getNextId(),
                INNER,
                leftProjected,
                rightProjected,
                criteriaBuilder.build(),
                left.getOutputSymbols(),   // original symbols only in output
                right.getOutputSymbols(),
                false, // since we only work on global aggregation or grouped rows, there are no duplicates, so we don't have to skip it
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    /**
     * Returns a non-null sentinel value for {@code type} suitable for use in
     * {@code COALESCE(k, sentinel)} when building null-safe equi-join keys.
     * Returns an empty {@link Optional} if no sentinel is available for this type.
     */
    static Optional<Object> nullSentinel(Type type)
    {
        // AbstractIntType covers DateType, IntegerType, RealType (REAL stores float as int bits packed in a long -> 0L == 0.0f)
        // AbstractLongType covers BigintType and TimeType
        if (type instanceof AbstractIntType || type instanceof AbstractLongType
                || type instanceof SmallintType || type instanceof TinyintType) {
            return Optional.of(0L);
        }
        if (type instanceof BooleanType) {
            return Optional.of(false);
        }
        if (type instanceof DoubleType) {
            return Optional.of(0.0d);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return Optional.of(Slices.EMPTY_SLICE);
        }
        if (type instanceof TimestampType timestampType) {
            // Short precision (<=6) is stored as a long (epoch micros).
            // Long precision (>6) is stored as LongTimestamp(epochMicros, picosOfMicro).
            return Optional.of(timestampType.isShort() ? 0L : new LongTimestamp(0, 0));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            // Short precision (<=3) is stored as a packed long (millis + zone key).
            // Long precision (>3) is stored as LongTimestampWithTimeZone.
            return Optional.of(timestampWithTimeZoneType.isShort() ? 0L
                    : LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, TimeZoneKey.UTC_KEY));
        }
        if (type instanceof DecimalType decimalType) {
            return Optional.of(decimalType.isShort() ? 0L : Int128.ZERO);
        }
        return Optional.empty();
    }
}
