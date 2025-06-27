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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.type.BigintType;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.Cardinality;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
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
 * This must be run after aggregation decorrelation rules.
 */
public class TransformCorrelatedScalarSubquery
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(correlation()))
            .with(filter().equalTo(TRUE));

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
        // lateral references are only allowed for INNER or LEFT correlated join
        checkArgument(correlatedJoinNode.getType() == INNER || correlatedJoinNode.getType() == LEFT, "unexpected correlated join type: %s", correlatedJoinNode.getType());
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

        Cardinality subqueryCardinality = extractCardinality(rewrittenSubquery, context.getLookup());
        boolean producesAtMostOneRow = subqueryCardinality.isAtMostScalar();
        if (producesAtMostOneRow) {
            boolean producesSingleRow = subqueryCardinality.isScalar();
            return Result.ofPlanNode(new CorrelatedJoinNode(
                    context.getIdAllocator().getNextId(),
                    correlatedJoinNode.getInput(),
                    rewrittenSubquery,
                    correlatedJoinNode.getCorrelation(),
                    // EnforceSingleRowNode guarantees that exactly single matching row is produced
                    // for every input row (independently of correlated join type). Decorrelated plan
                    // must preserve this semantics.
                    producesSingleRow ? INNER : LEFT,
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
                rewrittenCorrelatedJoinNode.getInput().getOutputSymbols());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                markDistinctNode,
                new Switch(
                        isDistinct.toSymbolReference(),
                        ImmutableList.of(
                                new WhenClause(TRUE, TRUE)),
                        new Cast(
                                failFunction(metadata, SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows"),
                                BOOLEAN)));

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                Assignments.identity(correlatedJoinNode.getOutputSymbols())));
    }
}
