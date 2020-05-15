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
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.PlanNodeDecorrelator;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.planner.plan.AggregationNode.globalAggregation;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static java.util.Objects.requireNonNull;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - CorrelatedJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 * or:
 * <pre>
 *     - CorrelatedJoin(LEFT)
 *       - input
 *       - Project($0 > 0)
 *         - Aggregation(COUNT(*))
 *           - subquery
 * </pre>
 * otherwise
 */
public class TransformExistsApplyToCorrelatedJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private static final QualifiedName COUNT = QualifiedName.of("count");
    private final Metadata metadata;
    private final ResolvedFunction countFunction;

    public TransformExistsApplyToCorrelatedJoin(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
        countFunction = metadata.resolveFunction(COUNT, ImmutableList.of());
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode parent, Captures captures, Context context)
    {
        if (parent.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = getOnlyElement(parent.getSubqueryAssignments().getExpressions());
        if (!(expression instanceof ExistsPredicate)) {
            return Result.empty();
        }

        /*
        Empty correlation list indicates that the subquery contains no correlation symbols from the
        immediate outer scope. The subquery might be either not correlated at all, or correlated with
        symbols from further outer scope.
        Currently, the two cases are indistinguishable.
        To support the latter case, the ApplyNode with empty correlation list is rewritten to default
        aggregation, which is inefficient in the rare case of uncorrelated EXISTS subquery,
        but currently allows to successfully decorrelate a correlated EXISTS subquery.

        TODO: remove this condition when exploratory optimizer is implemented or support for decorrelating joins is implemented in PlanNodeDecorrelator
        */
        if (parent.getCorrelation().isEmpty()) {
            return Result.ofPlanNode(rewriteToDefaultAggregation(parent, context));
        }

        Optional<PlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
        return nonDefaultAggregation
                .map(Result::ofPlanNode)
                .orElseGet(() -> Result.ofPlanNode(rewriteToDefaultAggregation(parent, context)));
    }

    private Optional<PlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputSymbols().isEmpty(), "Expected subquery output symbols to be pruned");

        Symbol subqueryTrue = context.getSymbolAllocator().newSymbol("subqueryTrue", BOOLEAN);

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        false),
                Assignments.of(subqueryTrue, TRUE_LITERAL));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(metadata, context.getSymbolAllocator(), context.getLookup());
        if (decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isEmpty()) {
            return Optional.empty();
        }

        Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());
        Assignments.Builder assignments = Assignments.builder()
                .putIdentities(applyNode.getInput().getOutputSymbols())
                .put(exists, new CoalesceExpression(ImmutableList.of(subqueryTrue.toSymbolReference(), BooleanLiteral.FALSE_LITERAL)));

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(),
                new CorrelatedJoinNode(
                        applyNode.getId(),
                        applyNode.getInput(),
                        subquery,
                        applyNode.getCorrelation(),
                        LEFT,
                        TRUE_LITERAL,
                        applyNode.getOriginSubquery()),
                assignments.build()));
    }

    private PlanNode rewriteToDefaultAggregation(ApplyNode applyNode, Context context)
    {
        Symbol count = context.getSymbolAllocator().newSymbol(COUNT.toString(), BIGINT);
        Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());

        return new CorrelatedJoinNode(
                applyNode.getId(),
                applyNode.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                applyNode.getSubquery(),
                                ImmutableMap.of(count, new Aggregation(
                                        countFunction,
                                        ImmutableList.of(),
                                        false,
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty())),
                                globalAggregation(),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty()),
                        Assignments.of(exists, new ComparisonExpression(GREATER_THAN, count.toSymbolReference(), new Cast(new LongLiteral("0"), toSqlType(BIGINT))))),
                applyNode.getCorrelation(),
                INNER,
                TRUE_LITERAL,
                applyNode.getOriginSubquery());
    }
}
