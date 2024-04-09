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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.Patterns.filter;

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
            Optional<Expression> simplifiedConjunct = switch (conjunct) {
                case NullIf expression -> Optional.of(Logical.and(expression.first(), isFalseOrNullPredicate(expression.second())));
                case Case expression -> simplify(expression);
                case Switch expression -> simplify(expression);
                case null, default -> Optional.empty();
            };

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

        Expression predicate = combineConjuncts(newConjuncts.build());
        if (predicate instanceof Constant constant && constant.value() == null) {
            predicate = FALSE;
        }
        return Result.ofPlanNode(new FilterNode(
                node.getId(),
                node.getSource(),
                predicate));
    }

    private static Optional<Expression> simplify(Expression condition, Expression trueValue, Expression falseValue)
    {
        if (trueValue.equals(TRUE) && isNotTrue(falseValue)) {
            return Optional.of(condition);
        }
        if (isNotTrue(trueValue) && falseValue.equals(TRUE)) {
            return Optional.of(isFalseOrNullPredicate(condition));
        }
        if (falseValue.equals(trueValue) && isDeterministic(trueValue)) {
            return Optional.of(trueValue);
        }
        if (isNotTrue(trueValue) && isNotTrue(falseValue)) {
            return Optional.of(FALSE);
        }
        if (condition.equals(TRUE)) {
            return Optional.of(trueValue);
        }
        if (isNotTrue(condition)) {
            return Optional.of(falseValue);
        }
        return Optional.empty();
    }

    private static Optional<Expression> simplify(Case caseExpression)
    {
        if (caseExpression.whenClauses().size() == 1) {
            // if-like expression
            return simplify(
                    caseExpression.whenClauses().getFirst().getOperand(),
                    caseExpression.whenClauses().getFirst().getResult(),
                    caseExpression.defaultValue());
        }

        List<Expression> operands = caseExpression.whenClauses().stream()
                .map(WhenClause::getOperand)
                .collect(toImmutableList());

        List<Expression> results = caseExpression.whenClauses().stream()
                .map(WhenClause::getResult)
                .collect(toImmutableList());
        long trueResultsCount = results.stream()
                .filter(result -> result.equals(TRUE))
                .count();
        long notTrueResultsCount = results.stream()
                .filter(SimplifyFilterPredicate::isNotTrue)
                .count();
        // all results true
        if (trueResultsCount == results.size() && caseExpression.defaultValue().equals(TRUE)) {
            return Optional.of(TRUE);
        }
        // all results not true
        if (notTrueResultsCount == results.size() && isNotTrue(caseExpression.defaultValue())) {
            return Optional.of(FALSE);
        }
        // one result true, and remaining results not true
        if (trueResultsCount == 1 && notTrueResultsCount == results.size() - 1 && isNotTrue(caseExpression.defaultValue())) {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (WhenClause whenClause : caseExpression.whenClauses()) {
                Expression operand = whenClause.getOperand();
                Expression result = whenClause.getResult();
                if (isNotTrue(result)) {
                    builder.add(isFalseOrNullPredicate(operand));
                }
                else {
                    builder.add(operand);
                    return Optional.of(combineConjuncts(builder.build()));
                }
            }
        }
        // all results not true, and default true
        if (notTrueResultsCount == results.size() && caseExpression.defaultValue().equals(TRUE)) {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            operands.forEach(operand -> builder.add(isFalseOrNullPredicate(operand)));
            return Optional.of(combineConjuncts(builder.build()));
        }
        // skip clauses with not true conditions
        List<WhenClause> whenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseExpression.whenClauses()) {
            Expression operand = whenClause.getOperand();
            if (operand.equals(TRUE)) {
                if (whenClauses.isEmpty()) {
                    return Optional.of(whenClause.getResult());
                }
                return Optional.of(new Case(whenClauses, whenClause.getResult()));
            }
            if (!isNotTrue(operand)) {
                whenClauses.add(whenClause);
            }
        }
        if (whenClauses.isEmpty()) {
            return Optional.of(caseExpression.defaultValue());
        }
        if (whenClauses.size() < caseExpression.whenClauses().size()) {
            return Optional.of(new Case(whenClauses, caseExpression.defaultValue()));
        }
        return Optional.empty();
    }

    private static Optional<Expression> simplify(Switch caseExpression)
    {
        Optional<Expression> defaultValue = Optional.of(caseExpression.defaultValue());

        if (caseExpression.operand() instanceof Constant literal && literal.value() == null) {
            return defaultValue;
        }

        List<Expression> results = caseExpression.whenClauses().stream()
                .map(WhenClause::getResult)
                .collect(toImmutableList());
        if (results.stream().allMatch(result -> result.equals(TRUE)) && defaultValue.get().equals(TRUE)) {
            return Optional.of(TRUE);
        }
        if (results.stream().allMatch(SimplifyFilterPredicate::isNotTrue) && isNotTrue(defaultValue.get())) {
            return Optional.of(FALSE);
        }
        return Optional.empty();
    }

    private static boolean isNotTrue(Expression expression)
    {
        return expression.equals(FALSE) ||
                expression instanceof Constant literal && literal.value() == null;
    }

    private static Expression isFalseOrNullPredicate(Expression expression)
    {
        return Logical.or(new IsNull(expression), new Not(expression));
    }
}
