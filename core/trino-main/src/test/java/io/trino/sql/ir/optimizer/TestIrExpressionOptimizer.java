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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIrExpressionOptimizer
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReferenceBindings(boolean partialEvaluation)
    {
        IrExpressionOptimizer optimizer = IrExpressionOptimizer.newOptimizer(PLANNER_CONTEXT);
        if (partialEvaluation) {
            optimizer = IrExpressionOptimizer.newPartialEvaluator(PLANNER_CONTEXT);
        }
        Reference a = new Reference(BIGINT, "a");
        Reference b = new Reference(BIGINT, "b");
        Constant value = new Constant(BIGINT, 7L);
        assertThat(optimizer.process(a, testSession(), emptySymbolAllocator(), ImmutableMap.of())).isEmpty();
        assertThat(optimizer.process(a, testSession(), emptySymbolAllocator(), ImmutableMap.of(Symbol.from(b), value))).isEmpty();
        assertThat(optimizer.process(a, testSession(), emptySymbolAllocator(), ImmutableMap.of(Symbol.from(a), b, Symbol.from(b), value))).contains(value);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 2})
    public void testRewriteChildLists(int changedIndex)
    {
        List<Expression> original = ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"), new Reference(BIGINT, "c"));
        List<Expression> expected = new ArrayList<>(original);
        Map<Symbol, Expression> bindings = ImmutableMap.of();
        if (changedIndex >= 0) {
            Constant replacement = new Constant(BIGINT, 7L);
            expected.set(changedIndex, replacement);
            bindings = ImmutableMap.of(Symbol.from(original.get(changedIndex)), replacement);
        }

        assertRewritten(new Row(original), new Row(expected), bindings);

        ImmutableList.Builder<WhenClause> originalCaseClauses = ImmutableList.builder();
        ImmutableList.Builder<WhenClause> expectedCaseClauses = ImmutableList.builder();
        ImmutableList.Builder<MatchClause> originalMatchClauses = ImmutableList.builder();
        ImmutableList.Builder<MatchClause> expectedMatchClauses = ImmutableList.builder();
        for (int index = 0; index < original.size(); index++) {
            Reference condition = new Reference(BOOLEAN, "condition" + index);
            originalCaseClauses.add(new WhenClause(condition, original.get(index)));
            expectedCaseClauses.add(new WhenClause(condition, expected.get(index)));
            Lambda predicate = new Lambda(ImmutableList.of(new Symbol(BIGINT, "operand")), condition);
            originalMatchClauses.add(new MatchClause(predicate, original.get(index)));
            expectedMatchClauses.add(new MatchClause(predicate, expected.get(index)));
        }
        Reference defaultValue = new Reference(BIGINT, "defaultValue");
        Reference operand = new Reference(BIGINT, "input");
        assertRewritten(new Case(originalCaseClauses.build(), defaultValue), new Case(expectedCaseClauses.build(), defaultValue), bindings);
        assertRewritten(new Match(operand, originalMatchClauses.build(), defaultValue), new Match(operand, expectedMatchClauses.build(), defaultValue), bindings);
    }

    private static void assertRewritten(Expression original, Expression expected, Map<Symbol, Expression> bindings)
    {
        var result = IrExpressionOptimizer.newPartialEvaluator(PLANNER_CONTEXT).process(original, testSession(), emptySymbolAllocator(), bindings);
        if (bindings.isEmpty()) {
            assertThat(result).isEmpty();
        }
        else {
            assertThat(result).contains(expected);
        }
    }
}
