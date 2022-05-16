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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.WhenClause;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

public class CaseExpressionPredicateRewriter
{
    private CaseExpressionPredicateRewriter() {}

    public static Expression rewrite(
            Expression expression,
            Rule.Context context,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(context, "context is null");
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, typeAnalyzer, context.getSession(), context.getSymbolAllocator()), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final LiteralEncoder literalEncoder;
        private final TypeAnalyzer typeAnalyzer;
        private final Session session;
        private final SymbolAllocator symbolAllocator;

        public Visitor(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, Session session, SymbolAllocator symbolAllocator)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.literalEncoder = new LiteralEncoder(this.plannerContext);
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression rewritten = node;
            if (!DeterminismEvaluator.isDeterministic(node, plannerContext.getMetadata())) {
                return treeRewriter.defaultRewrite(rewritten, context);
            }
            else if (isCaseExpression(node.getLeft()) && isEffectivelyLiteral(plannerContext, session, node.getRight())) {
                rewritten = processCaseExpression(node.getLeft(), node.getRight(), node.getOperator()).orElse(node);
            }
            else if (isCaseExpression(node.getRight()) && isEffectivelyLiteral(plannerContext, session, node.getLeft())) {
                rewritten = processCaseExpression(node.getRight(), node.getLeft(), node.getOperator().flip()).orElse(node);
            }
            return treeRewriter.defaultRewrite(rewritten, context);
        }

        private boolean isCaseExpression(Expression expression)
        {
            if (expression instanceof Cast) {
                expression = ((Cast) expression).getExpression();
            }
            return expression instanceof SimpleCaseExpression || expression instanceof SearchedCaseExpression;
        }

        private Optional<Expression> processCaseExpression(Expression expression, Expression otherExpression, ComparisonExpression.Operator operator)
        {
            Expression caseExpression = expression;
            Optional<Cast> castExpression = Optional.empty();
            if (expression instanceof Cast) {
                castExpression = Optional.of((Cast) expression);
                caseExpression = castExpression.get().getExpression();
            }
            return caseExpression instanceof SimpleCaseExpression ?
                    processSimpleCaseExpression((SimpleCaseExpression) caseExpression, castExpression, otherExpression, operator) :
                    processSearchedCaseExpression((SearchedCaseExpression) caseExpression, castExpression, otherExpression, operator);
        }

        private Optional<Expression> processSimpleCaseExpression(
                SimpleCaseExpression caseExpression,
                Optional<Cast> castExpression,
                Expression otherExpression,
                ComparisonExpression.Operator operator)
        {
            if (!canRewriteSimpleCaseExpression(caseExpression)) {
                return Optional.empty();
            }
            return processCaseExpression(
                    castExpression,
                    caseExpression.getWhenClauses(),
                    caseExpression.getDefaultValue(),
                    whenClause -> new ComparisonExpression(EQUAL, caseExpression.getOperand(), whenClause.getOperand()),
                    otherExpression,
                    operator,
                    caseExpression.getOperand());
        }

        private boolean canRewriteSimpleCaseExpression(SimpleCaseExpression caseExpression)
        {
            List<Expression> whenOperands = caseExpression.getWhenClauses().stream()
                    .map(WhenClause::getOperand)
                    .collect(Collectors.toList());
            return checkNonNullUniqueLiterals(whenOperands, getType(whenOperands));
        }

        private Type getType(List<Expression> expressions)
        {
            Expression expression = expressions.stream().findFirst().orElseThrow();
            return typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression).get(NodeRef.of(expression));
        }

        private Optional<Expression> processSearchedCaseExpression(
                SearchedCaseExpression caseExpression,
                Optional<Cast> castExpression,
                Expression otherExpression,
                ComparisonExpression.Operator operator)
        {
            if (!canRewriteSearchedCaseExpression(caseExpression)) {
                return Optional.empty();
            }
            return processCaseExpression(
                    castExpression,
                    caseExpression.getWhenClauses(),
                    caseExpression.getDefaultValue(),
                    WhenClause::getOperand,
                    otherExpression,
                    operator,
                    getCommonOperand(caseExpression));
        }

        private Expression getCommonOperand(SearchedCaseExpression caseExpression)
        {
            return caseExpression.getWhenClauses().stream()
                    .map(x -> ((ComparisonExpression) x.getOperand()).getLeft())
                    .findFirst().orElseThrow();
        }

        private boolean canRewriteSearchedCaseExpression(SearchedCaseExpression caseExpression)
        {
            ImmutableList.Builder<Expression> rightHandSideExpressions = ImmutableList.builder();
            ImmutableList.Builder<Expression> leftHandSideExpressions = ImmutableList.builder();
            for (WhenClause whenClause : caseExpression.getWhenClauses()) {
                Expression whenOperand = whenClause.getOperand();
                if (!(whenOperand instanceof ComparisonExpression)) {
                    return false;
                }
                ComparisonExpression whenComparisonFunction = (ComparisonExpression) whenOperand;
                Expression left = whenComparisonFunction.getLeft();
                Expression right = whenComparisonFunction.getRight();

                if (!whenComparisonFunction.getOperator().equals(EQUAL)) {
                    return false;
                }
                leftHandSideExpressions.add(left);
                rightHandSideExpressions.add(right);
            }
            List<Expression> rightHandExpressions = rightHandSideExpressions.build();
            return checkAllAreSimilar(leftHandSideExpressions.build()) &&
                    checkNonNullUniqueLiterals(rightHandExpressions, getType(rightHandExpressions));
        }

        private boolean checkAllAreSimilar(List<Expression> expressions)
        {
            return expressions.stream().distinct().count() <= 1;
        }

        private boolean checkNonNullUniqueLiterals(List<Expression> expressions, Type type)
        {
            Set<Object> literals = new HashSet<>();
            for (Expression expression : expressions) {
                if (!isEffectivelyLiteral(plannerContext, session, expression)) {
                    return false;
                }
                Object constantExpression = evaluateConstantExpression(expression, type, plannerContext, session, new AllowAllAccessControl(), ImmutableMap.of());
                if (constantExpression == null || literals.contains(constantExpression)) {
                    return false;
                }
                literals.add(constantExpression);
            }
            return true;
        }

        private Optional<Expression> processCaseExpression(
                Optional<Cast> castExpression,
                List<WhenClause> whenClauses,
                Optional<Expression> defaultValue,
                Function<WhenClause, Expression> operandExtractor,
                Expression otherExpression,
                ComparisonExpression.Operator operator,
                Expression commonOperand)
        {
            ImmutableList.Builder<Expression> andExpressions = ImmutableList.builder();
            ImmutableList.Builder<Expression> invertedOperands = ImmutableList.builder();

            for (WhenClause whenClause : whenClauses) {
                Expression whenOperand = operandExtractor.apply(whenClause);
                Expression whenResult = getCastExpression(castExpression, whenClause.getResult());

                andExpressions.add(and(
                        new ComparisonExpression(operator, whenResult, otherExpression),
                        new IsNotNullPredicate(commonOperand),
                        whenOperand));
                invertedOperands.add(new NotExpression(whenOperand));
            }

            Expression defaultExpression = defaultValue
                    .map(value -> getCastExpression(castExpression, value))
                    .orElse(getNullLiteral(otherExpression));

            Expression elseResult = new ComparisonExpression(operator, defaultExpression, otherExpression);

            andExpressions.add(and(elseResult, or(new IsNullPredicate(commonOperand), and(invertedOperands.build()))));

            return Optional.of(or(andExpressions.build()));
        }

        private Expression getCastExpression(Optional<Cast> castExpression, Expression expression)
        {
            return castExpression
                    .map(cast -> (Expression) new Cast(expression, cast.getType(), cast.isSafe(), cast.isTypeOnly()))
                    .orElse(expression);
        }

        private Expression getNullLiteral(Expression expression)
        {
            Type type = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression).get(NodeRef.of(expression));
            return this.literalEncoder.toExpression(session, null, type);
        }
    }
}
