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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.Type;
import io.trino.sql.ExpressionTestUtils;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.testng.Assert.assertEquals;

public class TestSortExpressionExtractor
{
    private static final TypeProvider TYPE_PROVIDER = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("b1"), DOUBLE)
            .put(new Symbol("b2"), DOUBLE)
            .put(new Symbol("p1"), BIGINT)
            .put(new Symbol("p2"), DOUBLE)
            .buildOrThrow());
    private static final Set<Symbol> BUILD_SYMBOLS = ImmutableSet.of(new Symbol("b1"), new Symbol("b2"));

    @Test
    public void testGetSortExpression()
    {
        assertGetSortExpression("p1 > b1", "b1");

        assertGetSortExpression("b2 <= p1", "b2");

        assertGetSortExpression("b2 > p1", "b2");

        assertGetSortExpression("b2 > sin(p1)", "b2");

        assertNoSortExpression("b2 > random(p1)");

        assertGetSortExpression("b2 > random(p1) AND b2 > p1", "b2", "b2 > p1");

        assertGetSortExpression("b2 > random(p1) AND b1 > p1", "b1", "b1 > p1");

        assertNoSortExpression("b1 > p1 + b2");

        assertNoSortExpression("sin(b1) > p1");

        assertNoSortExpression("b1 <= p1 OR b2 <= p1");

        assertNoSortExpression("sin(b2) > p1 AND (b2 <= p1 OR b2 <= p1 + 10)");

        assertGetSortExpression("sin(b2) > p1 AND (b2 <= p1 AND b2 <= p1 + 10)", "b2", "b2 <= p1", "b2 <= p1 + 10");

        assertGetSortExpression("b1 > p1 AND b1 <= p1", "b1");

        assertGetSortExpression("b1 > p1 AND b1 <= p1 AND b2 > p1", "b1", "b1 > p1", "b1 <= p1");

        assertGetSortExpression("b1 > p1 AND b1 <= p1 AND b2 > p1 AND b2 < p1 + 10 AND b2 > p2", "b2", "b2 > p1", "b2 < p1 + 10", "b2 > p2");

        assertGetSortExpression("p1 BETWEEN b1 AND b2", "b1", "p1 >= b1");

        assertGetSortExpression("p1 BETWEEN p2 AND b1", "b1", "p1 <= b1");

        assertGetSortExpression("b1 BETWEEN p1 AND p2", "b1", "b1 >= p1");

        assertGetSortExpression("b1 > p1 AND p1 BETWEEN b1 AND b2", "b1", "b1 > p1", "p1 >= b1");
    }

    private Expression expression(String sql)
    {
        return ExpressionTestUtils.planExpression(PLANNER_CONTEXT, TEST_SESSION, TYPE_PROVIDER, PlanBuilder.expression(sql));
    }

    private void assertNoSortExpression(String expression)
    {
        assertNoSortExpression(expression(expression));
    }

    private void assertNoSortExpression(Expression expression)
    {
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(PLANNER_CONTEXT.getMetadata(), BUILD_SYMBOLS, expression);
        assertEquals(actual, Optional.empty());
    }

    private void assertGetSortExpression(String expression, String expectedSymbol)
    {
        assertGetSortExpression(expression(expression), expectedSymbol);
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol)
    {
        // for now we expect that search expressions contain all the conjuncts from filterExpression as more complex cases are not supported yet.
        assertGetSortExpression(expression, expectedSymbol, extractConjuncts(expression));
    }

    private void assertGetSortExpression(String expression, String expectedSymbol, String... searchExpressions)
    {
        assertGetSortExpression(expression(expression), expectedSymbol, searchExpressions);
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol, String... searchExpressions)
    {
        List<Expression> searchExpressionList = Arrays.stream(searchExpressions)
                .map(this::expression)
                .collect(toImmutableList());
        assertGetSortExpression(expression, expectedSymbol, searchExpressionList);
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol, List<Expression> searchExpressions)
    {
        Optional<SortExpressionContext> expected = Optional.of(new SortExpressionContext(new SymbolReference(expectedSymbol), searchExpressions));
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(PLANNER_CONTEXT.getMetadata(), BUILD_SYMBOLS, expression);
        assertEquals(actual, expected);
    }
}
