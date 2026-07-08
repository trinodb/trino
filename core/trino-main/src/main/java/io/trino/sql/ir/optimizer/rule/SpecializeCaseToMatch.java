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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.IrExpressions.equalityClause;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/// Convert a searched Case whose clauses all compare the same expression for equality into a
/// Match over that expression. E.g.,
/// - `Case([When(x = 1, r1), When(2 = x, r2)], d) -> Match(x, [When(= 1, r1), When(= 2, r2)], d)`
///
/// Match evaluates the shared operand once instead of per clause, and the equality-shaped
/// clauses become visible to the Match rules (deduplication, merging, flattening). The operand
/// must be deterministic, since its per-clause evaluations collapse into one, and non-constant,
/// since a constant has no repeated evaluation to save. Single-clause expressions are left
/// alone for the same reason. Clause values that reference symbols get their captures lifted
/// into an explicit Bind, matching the post-desugaring lambda form the engine expects.
public class SpecializeCaseToMatch
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public SpecializeCaseToMatch(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case(List<WhenClause> whenClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        if (whenClauses.size() < 2) {
            return Optional.empty();
        }

        List<Comparison.Equal> equalities = new ArrayList<>();
        for (WhenClause clause : whenClauses) {
            if (!(matchComparison(clause.getOperand()) instanceof Comparison.Equal equality)) {
                return Optional.empty();
            }
            equalities.add(equality);
        }

        List<Expression> candidates = new ArrayList<>();
        candidates.add(equalities.getFirst().left());
        candidates.add(equalities.getFirst().right());
        for (Comparison.Equal equality : equalities) {
            candidates.removeIf(candidate -> !candidate.equals(equality.left()) && !candidate.equals(equality.right()));
            if (candidates.isEmpty()) {
                return Optional.empty();
            }
        }

        // A constant operand offers no repeated evaluation to save, so don't consider it.
        Optional<Expression> commonOperand = candidates.stream()
                .filter(candidate -> !(candidate instanceof Constant))
                .filter(DeterminismEvaluator::isDeterministic)
                .findFirst();
        if (commonOperand.isEmpty()) {
            return Optional.empty();
        }
        Expression operand = commonOperand.get();

        Symbol parameter = symbolAllocator.newSymbol("match", operand.type());
        ImmutableList.Builder<MatchClause> clauses = ImmutableList.builderWithExpectedSize(whenClauses.size());
        for (int i = 0; i < whenClauses.size(); i++) {
            Comparison.Equal equality = equalities.get(i);
            Expression value = equality.left().equals(operand) ? equality.right() : equality.left();
            MatchClause clause = equalityClause(metadata, parameter, value, whenClauses.get(i).getResult());
            // This rule runs after DesugarLambdaExpressions, when lambdas must no longer reference
            // outer-scope symbols directly; a value containing references needs its captures lifted
            // into an explicit Bind.
            clauses.add(new MatchClause(
                    LambdaCaptureDesugaringRewriter.rewrite(clause.predicate(), symbolAllocator),
                    clause.result()));
        }

        return Optional.of(new Match(operand, clauses.build(), defaultValue));
    }
}
