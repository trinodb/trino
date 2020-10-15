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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InPredicate;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.matching.Pattern.empty;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.plan.Patterns.Apply.correlation;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.function.Predicate.not;

/**
 * Rewrites uncorrelated filtering semi join to inner join
 * <p/>
 * Transforms:
 * <pre>
 * - Filter ((semiJoinSymbol AND predicate)
 *    - Project (pruning)
 *       - Apply (semiJoinSymbol <- (a IN b))
 *           correlation: []  // empty
 *           input: plan A producing symbol a
 *           subquery: plan B producing symbol b
 * </pre>
 * <p/>
 * Into:
 * <pre>
 * - Filter (predicate)
 *    - Project (pruning and (semiJoinSymbol <- TRUE))
 *       - Join INNER on (a = b)
 *          - ApplyNode's input
 *          - Aggregation distinct(b)
 *             - ApplyNode's subquery
 * </pre>
 */
public class ImplementUncorrelatedFilteringSemiJoinWithProject
        implements Rule<FilterNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<ApplyNode> APPLY = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(project()
                    .matching(ProjectNode::isIdentity)
                    .with(source().matching(applyNode()
                            .with(empty(correlation()))
                            .capturedAs(APPLY)))
                    .capturedAs(PROJECT)));

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT);
        ApplyNode applyNode = captures.get(APPLY);

        if (applyNode.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }
        Expression applyExpression = getOnlyElement(applyNode.getSubqueryAssignments().getExpressions());
        if (!(applyExpression instanceof InPredicate)) {
            return Result.empty();
        }
        InPredicate inPredicate = (InPredicate) applyExpression;

        Symbol semiJoinSymbol = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());
        Predicate<Expression> isSemiJoinSymbol = expression -> expression.equals(semiJoinSymbol.toSymbolReference());

        List<Expression> conjuncts = extractConjuncts(filterNode.getPredicate());
        if (conjuncts.stream().noneMatch(isSemiJoinSymbol)) {
            return Result.empty();
        }
        Expression filteredPredicate = and(conjuncts.stream()
                .filter(not(isSemiJoinSymbol))
                .collect(toImmutableList()));

        Expression equalityFilter = new ComparisonExpression(EQUAL, inPredicate.getValue(), inPredicate.getValueList());

        PlanNode filteringSource = distinct(
                applyNode.getSubquery(),
                Symbol.from(inPredicate.getValueList()),
                context.getIdAllocator());

        JoinNode innerJoin = new JoinNode(
                applyNode.getId(),
                JoinNode.Type.INNER,
                applyNode.getInput(),
                filteringSource,
                ImmutableList.of(),
                applyNode.getInput().getOutputSymbols(),
                ImmutableList.of(),
                Optional.of(equalityFilter),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        ProjectNode projectTrue = projectTrue(
                innerJoin,
                projectNode.getId(),
                projectNode.getOutputSymbols().stream()
                        .filter(symbol -> !symbol.equals(semiJoinSymbol))
                        .collect(toImmutableList()),
                semiJoinSymbol);

        FilterNode result = new FilterNode(
                filterNode.getId(),
                projectTrue,
                filteredPredicate);

        return Result.ofPlanNode(result);
    }

    private static PlanNode distinct(PlanNode node, Symbol symbol, PlanNodeIdAllocator idAllocator)
    {
        return new AggregationNode(
                idAllocator.getNextId(),
                node,
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(symbol)),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private static ProjectNode projectTrue(PlanNode node, PlanNodeId id, List<Symbol> outputSymbols, Symbol alwaysTrueSymbol)
    {
        return new ProjectNode(
                id,
                node,
                Assignments.builder()
                        .putIdentities(outputSymbols)
                        .put(alwaysTrueSymbol, TRUE_LITERAL)
                        .build());
    }
}
