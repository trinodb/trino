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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.planner.DeterminismEvaluator;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.IrUtils.combinePredicates;
import static io.trino.sql.ir.IrUtils.extractPredicates;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class ExtractCommonPredicatesExpressionRewriter
{
    public static Expression extractCommonPredicates(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression, NodeContext.ROOT_NODE);
    }

    private ExtractCommonPredicatesExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<NodeContext>
    {
        @Override
        protected Expression rewriteExpression(Expression node, NodeContext context, ExpressionTreeRewriter<NodeContext> treeRewriter)
        {
            if (context.isRootNode()) {
                return treeRewriter.rewrite(node, NodeContext.NOT_ROOT_NODE);
            }

            return null;
        }

        @Override
        public Expression rewriteLogical(Logical node, NodeContext context, ExpressionTreeRewriter<NodeContext> treeRewriter)
        {
            Expression expression = combinePredicates(
                    node.operator(),
                    extractPredicates(node.operator(), node).stream()
                            .map(subExpression -> treeRewriter.rewrite(subExpression, NodeContext.NOT_ROOT_NODE))
                            .collect(toImmutableList()));

            if (!(expression instanceof Logical)) {
                return expression;
            }

            Expression simplified = extractCommonPredicates((Logical) expression);

            // Prefer AND LogicalBinaryExpression at the root if possible
            if (context.isRootNode() && simplified instanceof Logical && ((Logical) simplified).operator() == OR) {
                return distributeIfPossible((Logical) simplified);
            }

            return simplified;
        }

        private Expression extractCommonPredicates(Logical node)
        {
            List<List<Expression>> subPredicates = getSubPredicates(node);

            Set<Expression> commonPredicates = ImmutableSet.copyOf(subPredicates.stream()
                    .map(this::filterDeterministicPredicates)
                    .reduce(Sets::intersection)
                    .orElse(emptySet()));

            List<List<Expression>> uncorrelatedSubPredicates = subPredicates.stream()
                    .map(predicateList -> removeAll(predicateList, commonPredicates))
                    .collect(toImmutableList());

            Logical.Operator flippedOperator = node.operator().flip();

            List<Expression> uncorrelatedPredicates = uncorrelatedSubPredicates.stream()
                    .map(predicate -> combinePredicates(flippedOperator, predicate))
                    .collect(toImmutableList());
            Expression combinedUncorrelatedPredicates = combinePredicates(node.operator(), uncorrelatedPredicates);

            return combinePredicates(flippedOperator, ImmutableList.<Expression>builder()
                    .addAll(commonPredicates)
                    .add(combinedUncorrelatedPredicates)
                    .build());
        }

        private static List<List<Expression>> getSubPredicates(Logical expression)
        {
            return extractPredicates(expression.operator(), expression).stream()
                    .map(predicate -> predicate instanceof Logical ?
                            extractPredicates((Logical) predicate) : ImmutableList.of(predicate))
                    .collect(toImmutableList());
        }

        /**
         * Applies the boolean distributive property.
         * <p>
         * For example:
         * ( A & B ) | ( C & D ) => ( A | C ) & ( A | D ) & ( B | C ) & ( B | D)
         * <p>
         * Returns the original expression if the expression is non-deterministic or if the distribution will
         * expand the expression by too much.
         */
        private Expression distributeIfPossible(Logical expression)
        {
            if (!isDeterministic(expression)) {
                // Do not distribute boolean expressions if there are any non-deterministic elements
                // TODO: This can be optimized further if non-deterministic elements are not repeated
                return expression;
            }
            List<Set<Expression>> subPredicates = getSubPredicates(expression).stream()
                    .map(ImmutableSet::copyOf)
                    .collect(toList());

            int originalBaseExpressions = subPredicates.stream()
                    .mapToInt(Set::size)
                    .sum();

            int newBaseExpressions;
            try {
                newBaseExpressions = Math.multiplyExact(subPredicates.stream()
                        .mapToInt(Set::size)
                        .reduce(Math::multiplyExact)
                        .getAsInt(), subPredicates.size());
            }
            catch (ArithmeticException e) {
                // Integer overflow from multiplication means there are too many expressions
                return expression;
            }

            if (newBaseExpressions > originalBaseExpressions * 2) {
                // Do not distribute boolean expressions if it would create 2x more base expressions
                // (e.g. A, B, C, D from the above example). This is just an arbitrary heuristic to
                // avoid cross product expression explosion.
                return expression;
            }

            Set<List<Expression>> crossProduct = Sets.cartesianProduct(subPredicates);

            return combinePredicates(
                    expression.operator().flip(),
                    crossProduct.stream()
                            .map(expressions -> combinePredicates(expression.operator(), expressions))
                            .collect(toImmutableList()));
        }

        private Set<Expression> filterDeterministicPredicates(List<Expression> predicates)
        {
            return predicates.stream()
                    .filter(DeterminismEvaluator::isDeterministic)
                    .collect(toSet());
        }

        private static <T> List<T> removeAll(Collection<T> collection, Collection<T> elementsToRemove)
        {
            return collection.stream()
                    .filter(element -> !elementsToRemove.contains(element))
                    .collect(toImmutableList());
        }
    }

    private enum NodeContext
    {
        ROOT_NODE,
        NOT_ROOT_NODE;

        boolean isRootNode()
        {
            return this == ROOT_NODE;
        }
    }
}
