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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.isPushAggregationThroughOuterJoin;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolMapper;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * This optimizer pushes aggregations below outer joins when: the aggregation
 * is on top of the outer join, it groups by all columns in the outer table, and
 * the outer rows are guaranteed to be distinct.
 * <p>
 * When the aggregation is pushed down, we still need to perform aggregations
 * on the null values that come out of the absent values in an outer
 * join. We add a cross join with a row of aggregations on null literals,
 * and coalesce the aggregation that results from the left outer join with
 * the result of the aggregation over nulls.
 * <p>
 * Example:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - Aggregate(Group by: all columns from the left table, aggregation:
 *    avg("n2.nationkey"))
 *      - LeftJoin("regionkey" = "regionkey")
 *          - AssignUniqueId (nation)
 *              - Tablescan (nation)
 *          - Tablescan (nation)
 * </pre>
 * </p>
 * Is rewritten to:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - project(regionkey, coalesce("avg", "avg_over_null")
 *      - CrossJoin
 *          - LeftJoin("regionkey" = "regionkey")
 *              - AssignUniqueId (nation)
 *                  - Tablescan (nation)
 *              - Aggregate(Group by: regionkey, aggregation:
 *                avg(nationkey))
 *                  - Tablescan (nation)
 *          - Aggregate
 *            avg(null_literal)
 *              - Values (null_literal)
 * </pre>
 */
public class PushAggregationThroughOuterJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(join().capturedAs(JOIN)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushAggregationThroughOuterJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        // This rule doesn't deal with AggregationNode's hash symbol. Hash symbols are not yet present at this stage of optimization.
        checkArgument(aggregation.getHashSymbol().isEmpty(), "unexpected hash symbol");

        JoinNode join = captures.get(JOIN);

        if (join.getFilter().isPresent()
                || !(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT)
                || !groupsOnAllColumns(aggregation, getOuterTable(join).getOutputSymbols())
                || !isDistinct(context.getLookup().resolve(getOuterTable(join)), context.getLookup()::resolve)
                || !isAggregationOnSymbols(aggregation, getInnerTable(join))) {
            return Result.empty();
        }

        List<Symbol> groupingKeys = join.getCriteria().stream()
                .map(join.getType() == JoinNode.Type.RIGHT ? JoinNode.EquiJoinClause::getLeft : JoinNode.EquiJoinClause::getRight)
                .collect(toImmutableList());
        AggregationNode rewrittenAggregation = new AggregationNode(
                aggregation.getId(),
                getInnerTable(join),
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol());

        JoinNode rewrittenJoin;
        if (join.getType() == JoinNode.Type.LEFT) {
            rewrittenJoin = new JoinNode(
                    join.getId(),
                    join.getType(),
                    join.getLeft(),
                    rewrittenAggregation,
                    join.getCriteria(),
                    join.getLeft().getOutputSymbols(),
                    ImmutableList.copyOf(rewrittenAggregation.getAggregations().keySet()),
                    // there are no duplicate rows possible since outer rows were guaranteed to be distinct
                    false,
                    join.getFilter(),
                    join.getLeftHashSymbol(),
                    join.getRightHashSymbol(),
                    join.getDistributionType(),
                    join.isSpillable(),
                    join.getDynamicFilters(),
                    join.getReorderJoinStatsAndCost());
        }
        else {
            rewrittenJoin = new JoinNode(
                    join.getId(),
                    join.getType(),
                    rewrittenAggregation,
                    join.getRight(),
                    join.getCriteria(),
                    ImmutableList.copyOf(rewrittenAggregation.getAggregations().keySet()),
                    join.getRight().getOutputSymbols(),
                    // there are no duplicate rows possible since outer rows were guaranteed to be distinct
                    false,
                    join.getFilter(),
                    join.getLeftHashSymbol(),
                    join.getRightHashSymbol(),
                    join.getDistributionType(),
                    join.isSpillable(),
                    join.getDynamicFilters(),
                    join.getReorderJoinStatsAndCost());
        }

        Optional<PlanNode> resultNode = coalesceWithNullAggregation(rewrittenAggregation, rewrittenJoin, context.getSymbolAllocator(), context.getIdAllocator());
        if (resultNode.isEmpty()) {
            return Result.empty();
        }

        return Result.ofPlanNode(resultNode.get());
    }

    private static PlanNode getInnerTable(JoinNode join)
    {
        checkState(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode innerNode;
        if (join.getType() == JoinNode.Type.LEFT) {
            innerNode = join.getRight();
        }
        else {
            innerNode = join.getLeft();
        }
        return innerNode;
    }

    private static PlanNode getOuterTable(JoinNode join)
    {
        checkState(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode outerNode;
        if (join.getType() == JoinNode.Type.LEFT) {
            outerNode = join.getLeft();
        }
        else {
            outerNode = join.getRight();
        }
        return outerNode;
    }

    private static boolean groupsOnAllColumns(AggregationNode node, List<Symbol> columns)
    {
        return node.getGroupingSetCount() == 1 && new HashSet<>(node.getGroupingKeys()).equals(new HashSet<>(columns));
    }

    // When the aggregation is done after the join, there will be a null value that gets aggregated over
    // where rows did not exist in the inner table.  For some aggregate functions, such as count, the result
    // of an aggregation over a single null row is one or zero rather than null. In order to ensure correct results,
    // we add a coalesce function with the output of the new outer join and the aggregation performed over a single
    // null row.
    private Optional<PlanNode> coalesceWithNullAggregation(AggregationNode aggregationNode, PlanNode outerJoin, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        // Create an aggregation node over a row of nulls.
        MappedAggregationInfo aggregationOverNullInfo = createAggregationOverNull(
                aggregationNode,
                symbolAllocator,
                idAllocator);

        AggregationNode aggregationOverNull = aggregationOverNullInfo.getAggregation();
        Map<Symbol, Symbol> sourceAggregationToOverNullMapping = aggregationOverNullInfo.getSymbolMapping();

        // Do a cross join with the aggregation over null
        JoinNode crossJoin = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                outerJoin,
                aggregationOverNull,
                ImmutableList.of(),
                outerJoin.getOutputSymbols(),
                aggregationOverNull.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        // Add coalesce expressions for all aggregation functions
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (Symbol symbol : outerJoin.getOutputSymbols()) {
            if (aggregationNode.getAggregations().containsKey(symbol)) {
                assignmentsBuilder.put(symbol, new CoalesceExpression(symbol.toSymbolReference(), sourceAggregationToOverNullMapping.get(symbol).toSymbolReference()));
            }
            else {
                assignmentsBuilder.put(symbol, symbol.toSymbolReference());
            }
        }
        return Optional.of(new ProjectNode(idAllocator.getNextId(), crossJoin, assignmentsBuilder.build()));
    }

    private MappedAggregationInfo createAggregationOverNull(AggregationNode referenceAggregation, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        // Create a values node that consists of a single row of nulls.
        // Map the output symbols from the referenceAggregation's source
        // to symbol references for the new values node.
        ImmutableList.Builder<Symbol> nullSymbols = ImmutableList.builder();
        ImmutableList.Builder<Expression> nullLiterals = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, Symbol> sourcesSymbolMappingBuilder = ImmutableMap.builder();
        for (Symbol sourceSymbol : referenceAggregation.getSource().getOutputSymbols()) {
            Type type = symbolAllocator.getTypes().get(sourceSymbol);
            nullLiterals.add(new Cast(new NullLiteral(), toSqlType(type)));
            Symbol nullSymbol = symbolAllocator.newSymbol("null", type);
            nullSymbols.add(nullSymbol);
            sourcesSymbolMappingBuilder.put(sourceSymbol, nullSymbol);
        }
        ValuesNode nullRow = new ValuesNode(
                idAllocator.getNextId(),
                nullSymbols.build(),
                ImmutableList.of(new Row(nullLiterals.build())));

        // For each aggregation function in the reference node, create a corresponding aggregation function
        // that points to the nullRow. Map the symbols from the aggregations in referenceAggregation to the
        // symbols in these new aggregations.
        ImmutableMap.Builder<Symbol, Symbol> aggregationsSymbolMappingBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsOverNullBuilder = ImmutableMap.builder();
        SymbolMapper mapper = symbolMapper(sourcesSymbolMappingBuilder.buildOrThrow());
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : referenceAggregation.getAggregations().entrySet()) {
            Symbol aggregationSymbol = entry.getKey();
            Aggregation overNullAggregation = mapper.map(entry.getValue());
            Symbol overNullSymbol = symbolAllocator.newSymbol(overNullAggregation.getResolvedFunction().getSignature().getName(), symbolAllocator.getTypes().get(aggregationSymbol));
            aggregationsOverNullBuilder.put(overNullSymbol, overNullAggregation);
            aggregationsSymbolMappingBuilder.put(aggregationSymbol, overNullSymbol);
        }
        Map<Symbol, Symbol> aggregationsSymbolMapping = aggregationsSymbolMappingBuilder.buildOrThrow();

        // create an aggregation node whose source is the null row.
        AggregationNode aggregationOverNullRow = new AggregationNode(
                idAllocator.getNextId(),
                nullRow,
                aggregationsOverNullBuilder.buildOrThrow(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        return new MappedAggregationInfo(aggregationOverNullRow, aggregationsSymbolMapping);
    }

    private static boolean isAggregationOnSymbols(AggregationNode aggregationNode, PlanNode source)
    {
        Set<Symbol> sourceSymbols = ImmutableSet.copyOf(source.getOutputSymbols());
        return aggregationNode.getAggregations().values().stream()
                .allMatch(aggregation -> sourceSymbols.containsAll(SymbolsExtractor.extractUnique(aggregation)));
    }

    private static class MappedAggregationInfo
    {
        private final AggregationNode aggregationNode;
        private final Map<Symbol, Symbol> symbolMapping;

        public MappedAggregationInfo(AggregationNode aggregationNode, Map<Symbol, Symbol> symbolMapping)
        {
            this.aggregationNode = aggregationNode;
            this.symbolMapping = symbolMapping;
        }

        public Map<Symbol, Symbol> getSymbolMapping()
        {
            return symbolMapping;
        }

        public AggregationNode getAggregation()
        {
            return aggregationNode;
        }
    }
}
