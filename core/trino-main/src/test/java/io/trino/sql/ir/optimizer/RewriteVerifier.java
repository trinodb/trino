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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.FunctionType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TestingRows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.TestingRows.EVALUATION_FAILED;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Fail.fail;

/// Verifies the contract of an [IrOptimizerRule] on a generated set of rows: the rewritten expression
/// evaluates to the same value as the original one for every row. This is what makes a rewrite
/// admissible at all, so it is complementary to asserting the rewritten expression, which pins down
/// which of the admissible rewrites the rule performs.
///
/// It is meant to be wrapped around the `optimize` helper of a rule's test, so that every case the
/// test states is checked against the contract as well.
public final class RewriteVerifier
{
    private final TestingRows rows;

    public RewriteVerifier(PlannerContext plannerContext)
    {
        // the rule tests apply their rule with testSession(), so the rows have to be evaluated with it too
        this.rows = new TestingRows(plannerContext, testSession());
    }

    /// Verifies `rewritten` against `original` and returns `rewritten`, so that a test can wrap the
    /// call to the rule with it.
    public Optional<Expression> verify(Expression original, Optional<Expression> rewritten)
    {
        rewritten.ifPresent(expression -> verify(original, expression));
        return rewritten;
    }

    private void verify(Expression original, Expression rewritten)
    {
        if (!isDeterministic(original)) {
            // the two expressions are evaluated separately, so their values are unrelated
            return;
        }
        if (original.type() instanceof FunctionType) {
            // a lambda evaluates to an invoker, which has no value to compare
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

        List<Map<String, Object>> generated = rows.rows(symbols, ImmutableList.of(original, rewritten));
        // Check each side against the compiled engine before comparing the sides against each other, so
        // that an engine defect is never reported as the rule being wrong.
        List<Object> originalValues = rows.evaluateAll(original, "the original expression", symbols, generated);
        List<Object> rewrittenValues = rows.evaluateAll(rewritten, "the rewritten expression", symbols, generated);

        for (int row = 0; row < generated.size(); row++) {
            Map<String, Object> bindings = generated.get(row);
            Object originalValue = originalValues.get(row);
            if (originalValue == EVALUATION_FAILED) {
                // A failure isn't guaranteed to be preserved, because a rewrite may drop or reorder
                // the work that fails. Only a row the original produces a value for is binding.
                continue;
            }

            Object rewrittenValue = rewrittenValues.get(row);
            if (rewrittenValue == EVALUATION_FAILED) {
                fail("the rewritten expression fails for a row the original evaluates%n  original:          %s%n  rewritten:         %s%n  row:               %s%n  original result:   %s",
                        original,
                        rewritten,
                        TestingRows.formatRow(symbols, bindings),
                        TestingRows.formatValue(original.type(), originalValue));
            }
            if (!TestingRows.valuesEqual(original.type(), originalValue, rewrittenValue)) {
                fail("the rewritten expression evaluates to a different value%n  original:          %s%n  rewritten:         %s%n  row:               %s%n  original result:   %s%n  rewritten result:  %s",
                        original,
                        rewritten,
                        TestingRows.formatRow(symbols, bindings),
                        TestingRows.formatValue(original.type(), originalValue),
                        TestingRows.formatValue(original.type(), rewrittenValue));
            }
        }
    }
}
