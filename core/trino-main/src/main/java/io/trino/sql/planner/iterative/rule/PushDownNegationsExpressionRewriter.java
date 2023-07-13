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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.Metadata;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.ComparisonExpression.Operator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ExpressionUtils.combinePredicates;
import static io.trino.sql.ExpressionUtils.extractPredicates;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

public final class PushDownNegationsExpressionRewriter
{
    public static Expression pushDownNegations(Metadata metadata, Expression expression, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, expressionTypes), expression);
    }

    private PushDownNegationsExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Metadata metadata;
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Metadata metadata, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        }

        @Override
        public Expression rewriteNotExpression(NotExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getValue() instanceof LogicalExpression child) {
                List<Expression> predicates = extractPredicates(child);
                List<Expression> negatedPredicates = predicates.stream().map(predicate -> treeRewriter.rewrite((Expression) new NotExpression(predicate), context)).collect(toImmutableList());
                return combinePredicates(metadata, child.getOperator().flip(), negatedPredicates);
            }
            if (node.getValue() instanceof ComparisonExpression child && child.getOperator() != IS_DISTINCT_FROM) {
                Operator operator = child.getOperator();
                Expression left = child.getLeft();
                Expression right = child.getRight();
                Type leftType = expressionTypes.get(NodeRef.of(left));
                Type rightType = expressionTypes.get(NodeRef.of(right));
                checkState(leftType != null && rightType != null, "missing type for expression");
                if ((typeHasNaN(leftType) || typeHasNaN(rightType)) && (
                        operator == GREATER_THAN_OR_EQUAL ||
                                operator == GREATER_THAN ||
                                operator == LESS_THAN_OR_EQUAL ||
                                operator == LESS_THAN)) {
                    return new NotExpression(new ComparisonExpression(operator, treeRewriter.rewrite(left, context), treeRewriter.rewrite(right, context)));
                }
                return new ComparisonExpression(operator.negate(), treeRewriter.rewrite(left, context), treeRewriter.rewrite(right, context));
            }
            if (node.getValue() instanceof NotExpression child) {
                return treeRewriter.rewrite(child.getValue(), context);
            }

            return new NotExpression(treeRewriter.rewrite(node.getValue(), context));
        }

        private boolean typeHasNaN(Type type)
        {
            return type instanceof DoubleType || type instanceof RealType;
        }
    }
}
