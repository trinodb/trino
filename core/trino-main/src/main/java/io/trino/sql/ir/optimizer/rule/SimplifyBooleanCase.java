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
package io.trino.sql.ir.optimizer.rule;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static java.util.Objects.requireNonNull;

/// Dissolve a single-clause boolean Case with a constant branch into a logical expression, provided the
/// non-constant branch cannot fail:
///
/// - `Case([When(c, r)], false) -> And($identical(c, true), r)`
/// - `Case([When(c, r)], true) -> Or($not($identical(c, true)), r)`
/// - `Case([When(c, true)], d) -> Or($identical(c, true), d)`
/// - `Case([When(c, false)], d) -> And($not($identical(c, true)), d)`
///
/// A `Case` evaluates the non-constant branch only when its side is taken, while `And`/`Or` guarantee
/// neither evaluation order nor conditional evaluation. Restricting the rewrite to branches that cannot
/// fail makes the loss of that guarantee unobservable: evaluating the branch eagerly can only produce a
/// value, and for that value in {true, false, null} each form above equals the `Case` it replaces.
///
/// The `And` forms additionally require the condition to not fail: the planner places conjuncts
/// independently, so `$identical(c, true)` could descend (e.g. below a join) to rows the intact `Case`
/// never saw. An `Or` is a single conjunct with the same symbols as the `Case`, so it is evaluated
/// exactly where the `Case` was, and the `Case` evaluated its condition unconditionally there anyway.
///
/// The condition appears as `$identical(c, true)` rather than `c` because a `Case` routes a null
/// condition to the default branch, which is what `$identical` reproduces.
public class SimplifyBooleanCase
        implements IrOptimizerRule
{
    private final PlannerContext context;

    public SimplifyBooleanCase(PlannerContext context)
    {
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case(List<WhenClause> whenClauses, Expression defaultValue)) ||
                !BOOLEAN.equals(expression.type()) ||
                whenClauses.size() != 1) {
            return Optional.empty();
        }

        WhenClause clause = whenClauses.getFirst();

        // Exactly one branch must be constant: with both, SimplifyRedundantCase applies; with neither,
        // there is nothing to dissolve.
        if (isTrueOrFalse(clause.getResult()) == isTrueOrFalse(defaultValue)) {
            return Optional.empty();
        }

        Metadata metadata = context.getMetadata();
        Expression whenTaken = comparison(metadata, IDENTICAL, clause.getOperand(), TRUE);
        Expression elseTaken = not(metadata, whenTaken);

        if (isTrueOrFalse(defaultValue)) {
            return tryRewriteToLogicalExpression(defaultValue, clause.getResult(), whenTaken, elseTaken, clause.getOperand());
        }
        return tryRewriteToLogicalExpression(clause.getResult(), defaultValue, elseTaken, whenTaken, clause.getOperand());
    }

    /// Rewrite a Case that yields `constant` when `constantTaken` holds, and `branch` when `branchTaken`
    /// does. Yields empty if `branch` may fail, as it goes from conditional to unconditional evaluation,
    /// or if an And would expose a failing `condition` as an independently placed conjunct.
    private Optional<Expression> tryRewriteToLogicalExpression(Expression constant, Expression branch, Expression branchTaken, Expression constantTaken, Expression condition)
    {
        if (mayFail(context, branch)) {
            return Optional.empty();
        }

        // With a false constant, the Case holds only when the branch is taken and holds. With a true one,
        // it holds whenever the constant is taken, and otherwise falls through to the branch.
        if (constant.equals(FALSE)) {
            if (mayFail(context, condition)) {
                return Optional.empty();
            }
            return Optional.of(and(branchTaken, branch));
        }
        return Optional.of(or(constantTaken, branch));
    }

    private static boolean isTrueOrFalse(Expression expression)
    {
        return expression.equals(TRUE) || expression.equals(FALSE);
    }
}
