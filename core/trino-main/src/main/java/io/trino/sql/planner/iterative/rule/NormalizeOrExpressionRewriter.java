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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LogicalExpression;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;

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

            ImmutableList.Builder<InPredicate> inPredicateBuilder = ImmutableList.builder();
            ImmutableSet.Builder<Expression> expressionToSkipBuilder = ImmutableSet.builder();
            ImmutableList.Builder<Expression> othersExpressionBuilder = ImmutableList.builder();
            groupComparisonAndInPredicate(terms).forEach((expression, values) -> {
                if (values.size() > 1) {
                    inPredicateBuilder.add(new InPredicate(expression, mergeToInListExpression(values)));
                    expressionToSkipBuilder.add(expression);
                }
            });

            Set<Expression> expressionToSkip = expressionToSkipBuilder.build();
            for (Expression expression : terms) {
                if (expression instanceof ComparisonExpression comparisonExpression && comparisonExpression.getOperator() == EQUAL) {
                    if (!expressionToSkip.contains(comparisonExpression.getLeft())) {
                        othersExpressionBuilder.add(expression);
                    }
                }
                else if (expression instanceof InPredicate inPredicate && inPredicate.getValueList() instanceof InListExpression) {
                    if (!expressionToSkip.contains(inPredicate.getValue())) {
                        othersExpressionBuilder.add(expression);
                    }
                }
                else {
                    othersExpressionBuilder.add(expression);
                }
            }

            return or(ImmutableList.<Expression>builder()
                    .addAll(othersExpressionBuilder.build())
                    .addAll(inPredicateBuilder.build())
                    .build());
        }

        private InListExpression mergeToInListExpression(Collection<Expression> expressions)
        {
            LinkedHashSet<Expression> expressionValues = new LinkedHashSet<>();
            for (Expression expression : expressions) {
                if (expression instanceof ComparisonExpression comparisonExpression && comparisonExpression.getOperator() == EQUAL) {
                    expressionValues.add(comparisonExpression.getRight());
                }
                else if (expression instanceof InPredicate inPredicate && inPredicate.getValueList() instanceof InListExpression valueList) {
                    expressionValues.addAll(valueList.getValues());
                }
                else {
                    throw new IllegalStateException("Unexpected expression: " + expression);
                }
            }

            return new InListExpression(ImmutableList.copyOf(expressionValues));
        }

        private Map<Expression, Collection<Expression>> groupComparisonAndInPredicate(List<Expression> terms)
        {
            ImmutableMultimap.Builder<Expression, Expression> expressionBuilder = ImmutableMultimap.builder();
            for (Expression expression : terms) {
                if (expression instanceof ComparisonExpression comparisonExpression && comparisonExpression.getOperator() == EQUAL) {
                    expressionBuilder.put(comparisonExpression.getLeft(), comparisonExpression);
                }
                else if (expression instanceof InPredicate inPredicate && inPredicate.getValueList() instanceof InListExpression) {
                    expressionBuilder.put(inPredicate.getValue(), inPredicate);
                }
            }

            return expressionBuilder.build().asMap();
        }
    }
}
