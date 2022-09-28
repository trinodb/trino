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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.WhenClause;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * Simplify conditional expressions in filter predicate.
 * <p>
 * Replaces conditional expression with an expression evaluating to TRUE
 * if and only if the original expression evaluates to TRUE.
 * The rewritten expression might not be equivalent to the original
 * expression.
 */
public class SimplifyFilterPredicate
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final Metadata metadata;

    public SimplifyFilterPredicate(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        List<Expression> conjuncts = extractConjuncts(node.getPredicate());
        ImmutableList.Builder<Expression> newConjuncts = ImmutableList.builder();

        boolean simplified = false;
        for (Expression conjunct : conjuncts) {
            Optional<Expression> simplifiedConjunct = simplifyFilterExpression(conjunct);
            if (simplifiedConjunct.isPresent()) {
                simplified = true;
                newConjuncts.add(simplifiedConjunct.get());
            }
            else {
                newConjuncts.add(conjunct);
            }
        }
        if (!simplified) {
            return Result.empty();
        }

        return Result.ofPlanNode(new FilterNode(
                node.getId(),
                node.getSource(),
                combineConjuncts(metadata, newConjuncts.build())));
    }

    private Optional<Expression> simplifyFilterExpression(Expression expression)
    {
        if (expression instanceof IfExpression) {
            IfExpression ifExpression = (IfExpression) expression;
            Expression condition = ifExpression.getCondition();
            Expression trueValue = ifExpression.getTrueValue();
            Optional<Expression> falseValue = ifExpression.getFalseValue();

            if (trueValue.equals(TRUE_LITERAL) && (falseValue.isEmpty() || isNotTrue(falseValue.get()))) {
                return Optional.of(condition);
            }
            if (isNotTrue(trueValue) && falseValue.isPresent() && falseValue.get().equals(TRUE_LITERAL)) {
                return Optional.of(isFalseOrNullPredicate(condition));
            }
            if (falseValue.isPresent() && falseValue.get().equals(trueValue) && isDeterministic(trueValue, metadata)) {
                return Optional.of(trueValue);
            }
            if (isNotTrue(trueValue) && (falseValue.isEmpty() || isNotTrue(falseValue.get()))) {
                return Optional.of(FALSE_LITERAL);
            }
            if (condition.equals(TRUE_LITERAL)) {
                return Optional.of(trueValue);
            }
            if (isNotTrue(condition)) {
                return Optional.of(falseValue.orElse(FALSE_LITERAL));
            }
            return Optional.empty();
        }

        if (expression instanceof NullIfExpression) {
            NullIfExpression nullIfExpression = (NullIfExpression) expression;
            return Optional.of(LogicalExpression.and(nullIfExpression.getFirst(), isFalseOrNullPredicate(nullIfExpression.getSecond())));
        }

        if (expression instanceof SearchedCaseExpression) {
            SearchedCaseExpression caseExpression = (SearchedCaseExpression) expression;
            Optional<Expression> defaultValue = caseExpression.getDefaultValue();

            List<Expression> operands = caseExpression.getWhenClauses().stream()
                    .map(WhenClause::getOperand)
                    .collect(toImmutableList());

            List<Expression> results = caseExpression.getWhenClauses().stream()
                    .map(WhenClause::getResult)
                    .collect(toImmutableList());
            long trueResultsCount = results.stream()
                    .filter(result -> result.equals(TRUE_LITERAL))
                    .count();
            long notTrueResultsCount = results.stream()
                    .filter(SimplifyFilterPredicate::isNotTrue)
                    .count();
            // all results true
            if (trueResultsCount == results.size() && defaultValue.isPresent() && defaultValue.get().equals(TRUE_LITERAL)) {
                return Optional.of(TRUE_LITERAL);
            }
            // all results not true
            if (notTrueResultsCount == results.size() && (defaultValue.isEmpty() || isNotTrue(defaultValue.get()))) {
                return Optional.of(FALSE_LITERAL);
            }
            // one result true, and remaining results not true
            if (trueResultsCount == 1 && notTrueResultsCount == results.size() - 1 && (defaultValue.isEmpty() || isNotTrue(defaultValue.get()))) {
                ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                for (WhenClause whenClause : caseExpression.getWhenClauses()) {
                    Expression operand = whenClause.getOperand();
                    Expression result = whenClause.getResult();
                    if (isNotTrue(result)) {
                        builder.add(isFalseOrNullPredicate(operand));
                    }
                    else {
                        builder.add(operand);
                        return Optional.of(combineConjuncts(metadata, builder.build()));
                    }
                }
            }
            // all results not true, and default true
            if (notTrueResultsCount == results.size() && defaultValue.isPresent() && defaultValue.get().equals(TRUE_LITERAL)) {
                ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                operands.stream()
                        .forEach(operand -> builder.add(isFalseOrNullPredicate(operand)));
                return Optional.of(combineConjuncts(metadata, builder.build()));
            }
            // skip clauses with not true conditions
            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : caseExpression.getWhenClauses()) {
                Expression operand = whenClause.getOperand();
                if (operand.equals(TRUE_LITERAL)) {
                    if (whenClauses.isEmpty()) {
                        return Optional.of(whenClause.getResult());
                    }
                    return Optional.of(new SearchedCaseExpression(whenClauses, Optional.of(whenClause.getResult())));
                }
                if (!isNotTrue(operand)) {
                    whenClauses.add(whenClause);
                }
            }
            if (whenClauses.isEmpty()) {
                return Optional.of(defaultValue.orElse(FALSE_LITERAL));
            }
            if (whenClauses.size() < caseExpression.getWhenClauses().size()) {
                return Optional.of(new SearchedCaseExpression(whenClauses, defaultValue));
            }
            return Optional.empty();
        }

        if (expression instanceof SimpleCaseExpression) {
            SimpleCaseExpression caseExpression = (SimpleCaseExpression) expression;
            Optional<Expression> defaultValue = caseExpression.getDefaultValue();

            if (caseExpression.getOperand() instanceof NullLiteral) {
                return Optional.of(defaultValue.orElse(FALSE_LITERAL));
            }

            List<Expression> results = caseExpression.getWhenClauses().stream()
                    .map(WhenClause::getResult)
                    .collect(toImmutableList());
            if (results.stream().allMatch(result -> result.equals(TRUE_LITERAL)) && defaultValue.isPresent() && defaultValue.get().equals(TRUE_LITERAL)) {
                return Optional.of(TRUE_LITERAL);
            }
            if (results.stream().allMatch(SimplifyFilterPredicate::isNotTrue) && (defaultValue.isEmpty() || isNotTrue(defaultValue.get()))) {
                return Optional.of(FALSE_LITERAL);
            }
            return Optional.empty();
        }

        return Optional.empty();
    }

    private static boolean isNotTrue(Expression expression)
    {
        return expression.equals(FALSE_LITERAL) ||
                expression instanceof NullLiteral ||
                expression instanceof Cast && isNotTrue(((Cast) expression).getExpression());
    }

    private static Expression isFalseOrNullPredicate(Expression expression)
    {
        return LogicalExpression.or(new IsNullPredicate(expression), new NotExpression(expression));
    }
}
