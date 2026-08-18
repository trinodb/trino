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
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.matchNullIf;
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
    private final IrExpressionOptimizer expressionOptimizer;

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    public SimplifyFilterPredicate(PlannerContext plannerContext)
    {
        this.plannerContext = plannerContext;
        this.expressionOptimizer = plannerContext.getExpressionOptimizer();
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

        Expression predicate = combineConjuncts(newConjuncts.build());
        predicate = expressionOptimizer.process(predicate, context.getSession(), context.getSymbolAllocator(), ImmutableMap.of()).orElse(predicate);
        if (predicate instanceof Constant constant && constant.value() == null) {
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
                expression instanceof Constant constant && constant.value() == null;
    }

    private static Expression isFalseOrNullPredicate(Metadata metadata, Expression expression)
    {
        if (expression instanceof IsNull) {
            return not(metadata, expression);
        }
        return Logical.or(new IsNull(expression), not(metadata, expression));
    }

    private static Optional<Expression> simplifyCase(Match caseExpression)
    {
        Optional<Expression> defaultValue = Optional.of(caseExpression.defaultValue());

        if (caseExpression.operand() instanceof Constant constant && constant.value() == null) {
            return defaultValue;
        }

        List<Expression> results = caseExpression.clauses().stream()
                .map(MatchClause::result)
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
            return Optional.of(new Logical(
                    AND,
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

    /**
     * Push a comparison against a constant down into the branches of a CASE or NULLIF operand,
     * so that the per-branch comparisons can be folded by later simplification.
     * Returns the original expression when no rewrite applies.
     */
    private static Expression simplifyCompareCase(Metadata metadata, IrExpressions.Comparison comparison)
    {
        ComparisonOperator operator = comparison.operator();
        Expression left = comparison.left();
        Expression right = comparison.right();

        // Simplify Comparison(Case, Constant)
        if (left instanceof Case(List<WhenClause> whenClauses, Expression defaultValue) && right instanceof Constant) {
            return new Case(
                    whenClauses.stream()
                            .map(whenClause -> new WhenClause(
                                    whenClause.getOperand(),
                                    comparison(metadata, operator, whenClause.getResult(), right)))
                            .collect(toImmutableList()),
                    comparison(metadata, operator, defaultValue, right));
        }

        // Simplify Comparison(NullIf, Constant)
        IrExpressions.NullIf nullIf = matchNullIf(left);
        if (nullIf != null && right instanceof Constant) {
            return new Case(
                    ImmutableList.of(
                            new WhenClause(
                                    comparison(metadata, operator, nullIf.first(), nullIf.second()),
                                    new IsNull(right))),
                    comparison(metadata, operator, nullIf.first(), right));
        }

        return null;
    }

    private static final class Visitor
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
        protected Expression visitCall(Call node, Void context)
        {
            // A comparison against a constant is lowered to an operator Call; push it down into a
            // CASE or NULLIF operand so the per-branch comparisons can be folded.
            IrExpressions.Comparison comparison = matchComparison(node);
            if (comparison != null) {
                Expression simplified = simplifyCompareCase(metadata, comparison);
                if (simplified != null) {
                    return process(simplified, context);
                }
            }

            return node;
        }

        @Override
        protected Expression visitCase(Case node, Void context)
        {
            // NULLIF is lowered to an `if(first = second) then null else first` CASE: a NULLIF
            // conjunct is true iff first is true and first is distinct from second.
            IrExpressions.NullIf nullIf = matchNullIf(node);
            if (nullIf != null) {
                return Logical.and(
                        process(nullIf.first(), context),
                        isFalseOrNullPredicate(metadata, process(nullIf.second(), context)));
            }

            Expression defaultValue = process(node.defaultValue(), context);
            Case caseExpression = node;
            if (defaultValue != node.defaultValue()) {
                caseExpression = new Case(node.whenClauses(), defaultValue);
            }

            Optional<Expression> simplified = simplifyCase(metadata, caseExpression);
            return simplified.orElse(caseExpression);
        }

        @Override
        protected Expression visitLet(Let node, Void context)
        {
            // NULLIF over a non-trivial operand is lowered to a `Let`-wrapped `if(...)` CASE.
            IrExpressions.NullIf nullIf = matchNullIf(node);
            if (nullIf != null) {
                return Logical.and(
                        process(nullIf.first(), context),
                        isFalseOrNullPredicate(metadata, process(nullIf.second(), context)));
            }
            return node;
        }

        @Override
        protected Expression visitMatch(Match node, Void context)
        {
            return simplifyCase(node).orElse(node);
        }
    }
}
