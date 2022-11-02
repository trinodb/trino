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
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LogicalExpression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

public final class NormalizeOrExpressionRewriter
{
    public static Expression normalizeOrExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private NormalizeOrExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteLogicalExpression(LogicalExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            List<Expression> terms = node.getTerms().stream()
                    .map(expression -> treeRewriter.rewrite(expression, context))
                    .collect(toImmutableList());

            if (node.getOperator() == AND) {
                return and(terms);
            }

            List<InPredicate> comparisons = terms.stream()
                    .filter(NormalizeOrExpressionRewriter::isEqualityComparisonExpression)
                    .map(ComparisonExpression.class::cast)
                    .collect(groupingBy(
                            ComparisonExpression::getLeft,
                            LinkedHashMap::new,
                            mapping(ComparisonExpression::getRight, Collectors.toList())))
                    .entrySet().stream()
                    .filter(entry -> entry.getValue().size() > 1)
                    .map(entry -> new InPredicate(entry.getKey(), new InListExpression(entry.getValue())))
                    .collect(Collectors.toList());

            Set<Expression> expressionToSkip = comparisons.stream()
                    .map(InPredicate::getValue)
                    .collect(toImmutableSet());

            List<Expression> others = terms.stream()
                    .filter(expression -> !isEqualityComparisonExpression(expression) || !expressionToSkip.contains(((ComparisonExpression) expression).getLeft()))
                    .collect(Collectors.toList());

            return or(ImmutableList.<Expression>builder()
                    .addAll(others)
                    .addAll(comparisons)
                    .build());
        }
    }

    private static boolean isEqualityComparisonExpression(Expression expression)
    {
        return expression instanceof ComparisonExpression && ((ComparisonExpression) expression).getOperator() == EQUAL;
    }
}
