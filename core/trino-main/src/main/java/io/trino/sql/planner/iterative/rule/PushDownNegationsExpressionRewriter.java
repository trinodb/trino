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

import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Comparison.Operator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.combinePredicates;
import static io.trino.sql.ir.IrUtils.extractPredicates;

public final class PushDownNegationsExpressionRewriter
{
    public static Expression pushDownNegations(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private PushDownNegationsExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteNot(Not node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.value() instanceof Logical child) {
                List<Expression> predicates = extractPredicates(child);
                List<Expression> negatedPredicates = predicates.stream().map(predicate -> treeRewriter.rewrite((Expression) new Not(predicate), context)).collect(toImmutableList());
                return combinePredicates(child.operator().flip(), negatedPredicates);
            }
            if (node.value() instanceof Comparison child && child.operator() != IS_DISTINCT_FROM) {
                Operator operator = child.operator();
                Expression left = child.left();
                Expression right = child.right();
                Type leftType = left.type();
                Type rightType = right.type();
                if ((typeHasNaN(leftType) || typeHasNaN(rightType)) && (
                        operator == GREATER_THAN_OR_EQUAL ||
                                operator == GREATER_THAN ||
                                operator == LESS_THAN_OR_EQUAL ||
                                operator == LESS_THAN)) {
                    return new Not(new Comparison(operator, treeRewriter.rewrite(left, context), treeRewriter.rewrite(right, context)));
                }
                return new Comparison(operator.negate(), treeRewriter.rewrite(left, context), treeRewriter.rewrite(right, context));
            }
            if (node.value() instanceof Not child) {
                return treeRewriter.rewrite(child.value(), context);
            }

            return new Not(treeRewriter.rewrite(node.value(), context));
        }

        private boolean typeHasNaN(Type type)
        {
            return type instanceof DoubleType || type instanceof RealType;
        }
    }
}
