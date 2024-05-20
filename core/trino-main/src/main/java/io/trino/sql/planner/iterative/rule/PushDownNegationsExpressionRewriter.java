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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Comparison.Operator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
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
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.function().name().equals(builtinFunctionName("$not"))) {
                ResolvedFunction function = node.function();
                Expression argument = node.arguments().getFirst();

                if (argument instanceof Logical child) {
                    List<Expression> predicates = extractPredicates(child);
                    List<Expression> negatedPredicates = predicates.stream().map(predicate -> treeRewriter.rewrite((Expression) new Call(function, ImmutableList.of(predicate)), context)).collect(toImmutableList());
                    return combinePredicates(child.operator().flip(), negatedPredicates);
                }
                if (argument instanceof Comparison child && child.operator() != IDENTICAL) {
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
                        return new Call(function, ImmutableList.of(new Comparison(operator, treeRewriter.rewrite(left, context), treeRewriter.rewrite(right, context))));
                    }
                    return new Comparison(operator.negate(), treeRewriter.rewrite(left, context), treeRewriter.rewrite(right, context));
                }
                if (argument instanceof Call child && child.function().name().equals(builtinFunctionName("$not"))) {
                    return treeRewriter.rewrite(child.arguments().getFirst(), context);
                }

                return new Call(function, ImmutableList.of(treeRewriter.rewrite(argument, context)));
            }

            return node;
        }

        private boolean typeHasNaN(Type type)
        {
            return type instanceof DoubleType || type instanceof RealType;
        }
    }
}
