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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.Logical.Operator.AND;
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
    private final PlannerContext plannerContext;

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    public SimplifyFilterPredicate(PlannerContext plannerContext)
    {
        this.plannerContext = plannerContext;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        List<Expression> conjuncts = extractConjuncts(node.getPredicate());
        ImmutableList.Builder<Expression> newConjuncts = ImmutableList.builder();

        boolean simplified = false;
        Visitor visitor = new Visitor(plannerContext.getMetadata());
        for (Expression conjunct : conjuncts) {
            Expression simplifiedConjunct = visitor.process(conjunct, null);
            newConjuncts.add(simplifiedConjunct);
            if (conjunct != simplifiedConjunct) {
                simplified = true;
            }
        }
        if (!simplified) {
            return Result.empty();
        }

        Expression predicate = new IrExpressionInterpreter(combineConjuncts(newConjuncts.build()), plannerContext, context.getSession()).optimize();
        if (isNotTrue(predicate)) {
            predicate = FALSE;
        }
        return Result.ofPlanNode(new FilterNode(
                node.getId(),
                node.getSource(),
                predicate));
    }

    private static boolean isNotTrue(Expression expression)
    {
        return expression.equals(FALSE) ||
                expression instanceof Constant literal && literal.value() == null;
    }

    private static Expression isFalseOrNullPredicate(Metadata metadata, Expression expression)
    {
        if (expression instanceof IsNull) {
            return not(metadata, expression);
        }
        return Logical.or(new IsNull(expression), not(metadata, expression));
    }

    private static Optional<Expression> simplifyCase(Switch caseExpression)
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

    private static Optional<Expression> simplifyCase(Metadata metadata, Expression condition, Expression trueValue, Expression falseValue)
    {
        if (trueValue.equals(TRUE) && isNotTrue(falseValue)) {
            return Optional.of(condition);
        }
        if (isNotTrue(trueValue)) {
            if (falseValue.equals(TRUE)) {
                return Optional.of(isFalseOrNullPredicate(metadata, condition));
            }
            if (isNotTrue(falseValue)) {
                return Optional.of(FALSE);
            }
            return Optional.of(new Logical(AND,
                    ImmutableList.of(isFalseOrNullPredicate(metadata, condition), falseValue)));
        }
        if (falseValue.equals(trueValue) && isDeterministic(trueValue)) {
            return Optional.of(trueValue);
        }
        if (condition.equals(TRUE)) {
            return Optional.of(trueValue);
        }
        if (isNotTrue(condition)) {
            return Optional.of(falseValue);
        }
        return Optional.empty();
    }

    private static Optional<Expression> simplifyCase(Metadata metadata, Case caseExpression)
    {
        if (caseExpression.whenClauses().size() == 1) {
            // if-like expression
            return simplifyCase(
                    metadata,
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
                    builder.add(isFalseOrNullPredicate(metadata, operand));
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
            operands.forEach(operand -> builder.add(isFalseOrNullPredicate(metadata, operand)));
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

    private static Expression simplifyCompareCase(Comparison comparison)
    {
        // Simplify Comparison(Case, Constant)
        if (comparison.left() instanceof Case caseExpression && comparison.right() instanceof Constant) {
            return new Case(
                    caseExpression.whenClauses().stream()
                            .map(whenClause -> new WhenClause(whenClause.getOperand(), new Comparison(comparison.operator(), whenClause.getResult(), comparison.right())))
                            .collect(toImmutableList()),
                    new Comparison(comparison.operator(), caseExpression.defaultValue(), comparison.right()));
        }

        if (comparison.left() instanceof NullIf nullIf && comparison.right() instanceof Constant right) {
            return new Case(
                    ImmutableList.of(
                            new WhenClause(new Comparison(comparison.operator(), nullIf.first(), nullIf.second()), new IsNull(right))),
                    new Comparison(comparison.operator(), nullIf.first(), right));
        }

        return comparison;
    }

    private static class Visitor
            extends IrVisitor<Expression, Void>
    {
        private final Metadata metadata;

        public Visitor(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected Expression visitExpression(Expression expression, Void context)
        {
            return expression;
        }

        @Override
        public Expression visitNullIf(NullIf node, Void context)
        {
            return Logical.and(
                    process(node.first(), context),
                    isFalseOrNullPredicate(metadata, process(node.second(), context)));
        }

        @Override
        public Expression visitCase(Case node, Void context)
        {
            Expression defaultValue = process(node.defaultValue(), context);
            Case caseExpression = node;
            if (defaultValue != node.defaultValue()) {
                caseExpression = new Case(node.whenClauses(), defaultValue);
            }

            Optional<Expression> simplified = simplifyCase(metadata, caseExpression);
            return simplified.orElse(caseExpression);
        }

        @Override
        public Expression visitSwitch(Switch node, Void context)
        {
            return simplifyCase(node).orElse(node);
        }

        @Override
        public Expression visitComparison(Comparison node, Void context)
        {
            Expression simplified = simplifyCompareCase(node);
            if (simplified == node) {
                return node;
            }
            return process(simplified);
        }
    }
}
