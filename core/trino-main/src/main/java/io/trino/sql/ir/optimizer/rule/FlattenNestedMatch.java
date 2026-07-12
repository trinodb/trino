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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/// Flatten a Match nested in the default clause of another Match over the same operand. E.g.,
/// - `Match(x, [When(p1, r1)], Match(x, [When(p2, r2)], d)) -> Match(x, [When(p1, r1), When(p2, r2)], d)`
///
/// The operand must be deterministic: the original expression evaluates it a second time when
/// no outer clause matches, and the rewrite is valid only when both evaluations are guaranteed
/// to produce the same value.
public class FlattenNestedMatch
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Match(Expression operand, List<MatchClause> clauses, Expression defaultValue))) {
            return Optional.empty();
        }

        if (!(defaultValue instanceof Match(Expression innerOperand, List<MatchClause> innerClauses, Expression innerDefault))) {
            return Optional.empty();
        }

        if (!operand.equals(innerOperand) || !isDeterministic(operand)) {
            return Optional.empty();
        }

        return Optional.of(new Match(
                operand,
                ImmutableList.<MatchClause>builderWithExpectedSize(clauses.size() + innerClauses.size())
                        .addAll(clauses)
                        .addAll(innerClauses)
                        .build(),
                innerDefault));
    }
}
