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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.TestingRows.EVALUATION_FAILED;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Fail.fail;

/// Verifies the contract of [DomainTranslator] on a generated set of rows: a row passes the original
/// predicate if and only if the extracted [TupleDomain] contains it and the remaining expression is
/// true for it. This holds regardless of how much the translator manages to extract, so it is
/// complementary to asserting the extracted domain, which pins down how much it extracts.
final class ExtractionResultVerifier
{
    private final TestingRows rows;

    public ExtractionResultVerifier(PlannerContext plannerContext, Session session)
    {
        this.rows = new TestingRows(plannerContext, session);
    }

    public void verify(Expression predicate, ExtractionResult result)
    {
        // for a non-deterministic predicate, the predicate and the remaining expression are evaluated separately, so their outcomes are unrelated
        checkArgument(isDeterministic(predicate), "predicate is not deterministic: %s", predicate);

        for (Map<String, Object> bindings : rows.rows(predicate)) {
            verifyRow(predicate, result, bindings);
        }
    }

    private void verifyRow(Expression predicate, ExtractionResult result, Map<String, Object> bindings)
    {
        Object value = rows.evaluate(predicate, bindings);
        boolean passes = containsRow(result.getTupleDomain(), bindings) && TRUE.equals(rows.evaluate(result.getRemainingExpression(), bindings));

        String problem;
        if (value == EVALUATION_FAILED) {
            // A failure isn't guaranteed, because domain translation may effectively reorder predicate conjuncts.
            // But a failing row cannot become retained one.
            if (!passes) {
                return;
            }
            problem = "the predicate fails for the row, but the translation passes it";
        }
        else if (TRUE.equals(value) == passes) {
            return;
        }
        else {
            problem = TRUE.equals(value) ?
                    "the predicate is true for the row, but the translation does not pass it" :
                    "the predicate is not true for the row, but the translation passes it";
        }

        fail("%s%n  predicate:  %s%n  domain:     %s%n  remaining:  %s%n  row:        %s",
                problem,
                predicate,
                result.getTupleDomain(),
                result.getRemainingExpression(),
                bindings);
    }

    private static boolean containsRow(TupleDomain<Symbol> tupleDomain, Map<String, Object> bindings)
    {
        if (tupleDomain.isNone()) {
            return false;
        }
        for (Map.Entry<Symbol, Domain> entry : tupleDomain.getDomains().orElseThrow().entrySet()) {
            String name = entry.getKey().name();
            checkState(bindings.containsKey(name), "extracted a domain for %s, which the predicate does not reference", name);
            if (!entry.getValue().includesNullableValue(bindings.get(name))) {
                return false;
            }
        }
        return true;
    }
}
