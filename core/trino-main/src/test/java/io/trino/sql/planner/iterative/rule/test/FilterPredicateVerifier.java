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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TestingRows;

import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.TestingRows.EVALUATION_FAILED;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Fail.fail;

/// Verifies the contract of a rule that rewrites the predicate of a filter: the rewritten predicate
/// evaluates to true for exactly the rows the original one does. The two need not evaluate to the
/// same value — a rule is free to turn an unknown into false — so this is weaker than the equivalence
/// [io.trino.sql.ir.optimizer.RewriteVerifier] checks for expression rewrites.
final class FilterPredicateVerifier
{
    private final TestingRows rows;

    public FilterPredicateVerifier(PlannerContext plannerContext, Session session)
    {
        this.rows = new TestingRows(plannerContext, session);
    }

    public void verify(Expression original, Expression rewritten)
    {
        if (!isDeterministic(original)) {
            // the two predicates are evaluated separately, so their outcomes are unrelated
            return;
        }

        Set<Symbol> symbols = ImmutableSet.<Symbol>builder()
                .addAll(SymbolsExtractor.extractUnique(original))
                // a rewrite may drop a symbol, and a row still has to bind every symbol it reads
                .addAll(SymbolsExtractor.extractUnique(rewritten))
                .build();

        if (TestingRows.hasConflictingTypes(symbols)) {
            // an ill-typed expression, which no plan can contain, has no rows to generate
            return;
        }

        for (Map<String, Object> bindings : rows.rows(symbols, ImmutableList.of(original, rewritten))) {
            Object value = rows.evaluate(original, bindings);
            if (value == EVALUATION_FAILED) {
                // A failure isn't guaranteed to be preserved, because a rewrite may drop or reorder
                // the work that fails. Only a row the original produces a value for is binding.
                continue;
            }

            Object rewrittenValue = rows.evaluate(rewritten, bindings);
            if (rewrittenValue == EVALUATION_FAILED) {
                fail("the rewritten predicate fails for a row the original evaluates%n  original:   %s%n  rewritten:  %s%n  row:        %s",
                        original,
                        rewritten,
                        TestingRows.formatRow(symbols, bindings));
            }
            if (TRUE.equals(value) != TRUE.equals(rewrittenValue)) {
                String problem = TRUE.equals(value) ?
                        "the original predicate passes a row the rewritten one does not" :
                        "the rewritten predicate passes a row the original one does not";
                fail("%s%n  original:   %s%n  rewritten:  %s%n  row:        %s",
                        problem,
                        original,
                        rewritten,
                        TestingRows.formatRow(symbols, bindings));
            }
        }
    }
}
