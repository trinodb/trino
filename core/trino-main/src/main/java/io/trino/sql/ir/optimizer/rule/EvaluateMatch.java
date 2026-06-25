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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/// Evaluates a Match expression whose operand is a constant.
///
/// For each clause whose predicate body evaluates to a definite TRUE under the operand
/// binding, returns the clause's result. If the predicate evaluates to FALSE, the clause
/// is skipped. If the predicate is non-deterministic or fails to fully evaluate, the rule
/// bails — leaving the Match in place so later, less-aggressive optimizations can decide.
public class EvaluateMatch
        implements IrOptimizerRule
{
    private final IrExpressionEvaluator evaluator;

    public EvaluateMatch(PlannerContext context)
    {
        this.evaluator = new IrExpressionEvaluator(context);
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Match(Expression operand, List<MatchClause> clauses, Expression defaultValue))) {
            return Optional.empty();
        }

        if (!(operand instanceof Constant constantOperand)) {
            return Optional.empty();
        }

        for (MatchClause clause : clauses) {
            Lambda lambda = clause.lambda();
            Bind bind = clause.bind();
            // A non-deterministic predicate body must not be folded at plan time: evaluating it
            // here would bake a single draw (e.g. one random() value) into the plan instead of
            // evaluating it per row. Bail and leave the Match for per-row evaluation.
            if (!isDeterministic(lambda.body())) {
                return Optional.empty();
            }
            List<Symbol> arguments = lambda.arguments();
            int captureCount = bind == null ? 0 : bind.values().size();
            // Bail unless every captured value is a Constant. Evaluating with empty bindings would
            // silently resolve outer-scope references to null and produce a bogus predicate result.
            for (int i = 0; i < captureCount; i++) {
                if (!(bind.values().get(i) instanceof Constant)) {
                    return Optional.empty();
                }
            }
            Map<String, Object> clauseBindings = new HashMap<>();
            for (int i = 0; i < captureCount; i++) {
                clauseBindings.put(arguments.get(i).name(), ((Constant) bind.values().get(i)).value());
            }
            clauseBindings.put(arguments.getLast().name(), constantOperand.value());

            Object result;
            try {
                result = evaluator.evaluate(lambda.body(), session, clauseBindings);
            }
            catch (RuntimeException e) {
                return Optional.empty();
            }

            if (Boolean.TRUE.equals(result)) {
                return Optional.of(clause.result());
            }
            // NULL predicates are treated as not-matched (SQL three-valued logic). FALSE flows on
            // to the next clause. Any other return (non-boolean, unbindable) means we couldn't
            // fully evaluate the clause; bail and let a later, less-aggressive optimization decide.
            if (result != null && !Boolean.FALSE.equals(result)) {
                return Optional.empty();
            }
        }

        return Optional.of(defaultValue);
    }
}
