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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.WhenClause;

import java.util.Optional;

import static io.prestosql.matching.Pattern.nonEmpty;
import static io.prestosql.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.prestosql.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.prestosql.sql.planner.plan.Patterns.correlatedJoin;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * Scalar filter scan query is something like:
 * <pre>
 *     SELECT a,b,c FROM rel WHERE a = correlated1 AND b = correlated2
 * </pre>
 * <p>
 * This optimizer can rewrite to mark distinct and filter over a left outer join:
 * <p>
 * From:
 * <pre>
 * - CorrelatedJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (scalar subquery) Project F
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Filter(CASE isDistinct WHEN true THEN true ELSE fail('Scalar sub-query has returned multiple rows'))
 *   - MarkDistinct(isDistinct)
 *     - CorrelatedJoin (with correlation list: [C])
 *       - AssignUniqueId(adds symbol U)
 *         - (input) plan which produces symbols: [A, B, C]
 *       - non scalar subquery
 * </pre>
 * <p>
 * This must be run after {@link TransformCorrelatedScalarAggregationToJoin}
 */
public class TransformCorrelatedScalarSubquery
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(correlation()))
            .with(filter().equalTo(TRUE_LITERAL));

    private final Metadata metadata;

    public TransformCorrelatedScalarSubquery(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        PlanNode subquery = context.getLookup().resolve(correlatedJoinNode.getSubquery());

        if (!searchFrom(subquery, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .matches()) {
            return Result.empty();
        }

        PlanNode rewrittenSubquery = searchFrom(subquery, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .removeFirst();

        Range<Long> subqueryCardinality = extractCardinality(rewrittenSubquery, context.getLookup());
        boolean producesAtMostOneRow = Range.closed(0L, 1L).encloses(subqueryCardinality);
        if (producesAtMostOneRow) {
            boolean producesSingleRow = Range.singleton(1L).encloses(subqueryCardinality);
            return Result.ofPlanNode(new CorrelatedJoinNode(
                    context.getIdAllocator().getNextId(),
                    correlatedJoinNode.getInput(),
                    rewrittenSubquery,
                    correlatedJoinNode.getCorrelation(),
                    producesSingleRow ? correlatedJoinNode.getType() : LEFT,
                    correlatedJoinNode.getFilter(),
                    correlatedJoinNode.getOriginSubquery()));
        }

        Symbol unique = context.getSymbolAllocator().newSymbol("unique", BigintType.BIGINT);

        CorrelatedJoinNode rewrittenCorrelatedJoinNode = new CorrelatedJoinNode(
                context.getIdAllocator().getNextId(),
                new AssignUniqueId(
                        context.getIdAllocator().getNextId(),
                        correlatedJoinNode.getInput(),
                        unique),
                rewrittenSubquery,
                correlatedJoinNode.getCorrelation(),
                LEFT,
                correlatedJoinNode.getFilter(),
                correlatedJoinNode.getOriginSubquery());

        Symbol isDistinct = context.getSymbolAllocator().newSymbol("is_distinct", BOOLEAN);
        MarkDistinctNode markDistinctNode = new MarkDistinctNode(
                context.getIdAllocator().getNextId(),
                rewrittenCorrelatedJoinNode,
                isDistinct,
                rewrittenCorrelatedJoinNode.getInput().getOutputSymbols(),
                Optional.empty());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                markDistinctNode,
                new SimpleCaseExpression(
                        isDistinct.toSymbolReference(),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                        Optional.of(new Cast(
                                new FunctionCallBuilder(metadata)
                                        .setName(QualifiedName.of("fail"))
                                        .addArgument(INTEGER, new LongLiteral(Integer.toString(SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode())))
                                        .addArgument(VARCHAR, new StringLiteral("Scalar sub-query has returned multiple rows"))
                                        .build(),
                                toSqlType(BOOLEAN)))));

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                Assignments.identity(correlatedJoinNode.getOutputSymbols())));
    }
}
