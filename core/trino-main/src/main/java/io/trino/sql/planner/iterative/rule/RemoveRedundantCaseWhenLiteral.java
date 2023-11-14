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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.SearchedCaseExpression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.ir.ExpressionTreeRewriter.rewriteWith;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

public class RemoveRedundantCaseWhenLiteral
        implements Rule<FilterNode>
{
    private final PlannerContext plannerContext;
    private final IrTypeAnalyzer typeAnalyzer;

    public RemoveRedundantCaseWhenLiteral(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return filter();
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        Expression rewritten = rewriteWith(new Visitor(plannerContext, context.getSession(), context.getSymbolAllocator(), typeAnalyzer), node.getPredicate());
        if (node.getPredicate().equals(rewritten)) {
            return Result.empty();
        }

        return Result.ofPlanNode(new FilterNode(node.getId(), node.getSource(), rewritten));
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final Session session;
        private final SymbolAllocator symbolAllocator;
        private final IrTypeAnalyzer typeAnalyzer;

        public Visitor(PlannerContext plannerContext, Session session, SymbolAllocator symbolAllocator, IrTypeAnalyzer typeAnalyzer)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        private Predicate<Expression> valueMatcher(ComparisonExpression.Operator operator, Object value, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            Predicate<Expression> matcher = expr -> value.equals(new IrExpressionInterpreter(expr, plannerContext, session, expressionTypes).evaluate());

            return switch (operator) {
                case EQUAL -> matcher;
                case NOT_EQUAL -> matcher.negate();
                default -> throw new IllegalArgumentException("Unsupported comparison: " + operator);
            };
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if ((node.getOperator() == EQUAL || node.getOperator() == NOT_EQUAL)) {
                if (node.getLeft() instanceof SearchedCaseExpression left &&
                        node.getRight() instanceof Constant right &&
                        left.getWhenClauses().stream().map(WhenClause::getResult).allMatch(Constant.class::isInstance) &&
                        left.getDefaultValue().map(Constant.class::isInstance).orElse(false)) {
                    Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(symbolAllocator.getTypes(), node);
                    Object rightValue = right.getValue();

                    Predicate<Expression> matcher = valueMatcher(node.getOperator(), rightValue, expressionTypes);

                    List<Expression> positives = left.getWhenClauses().stream()
                            .filter(wc -> matcher.test(wc.getResult()))
                            .map(WhenClause::getOperand)
                            .collect(toImmutableList());

                    List<Expression> negatives = ImmutableList.of();
                    if (left.getDefaultValue().map(matcher::test).orElse(false)) {
                        negatives = left.getWhenClauses().stream()
                                .filter(wc -> !matcher.test(wc.getResult()))
                                .map(WhenClause::getOperand)
                                .collect(toImmutableList());
                    }

                    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                    builder.addAll(positives);

                    if (negatives.size() > 1) {
                        builder.add(new NotExpression(new LogicalExpression(OR, negatives)));
                    }
                    else if (negatives.size() == 1) {
                        builder.add(new NotExpression(negatives.get(0)));
                    }

                    List<Expression> filter = builder.build();
                    if (filter.isEmpty()) {
                        return FALSE_LITERAL;
                    }

                    if (filter.size() == 1) {
                        return filter.get(0);
                    }

                    return new LogicalExpression(OR, filter);
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }
    }
}
