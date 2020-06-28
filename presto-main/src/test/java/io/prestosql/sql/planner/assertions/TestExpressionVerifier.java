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
package io.prestosql.sql.planner.assertions;

import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.ExpressionVerifier.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExpressionVerifier
{
    @Test
    public void test()
    {
        String actual = "NOT(orderkey = 3 AND custkey = 3 AND orderkey < 10)";

        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        assertTrue(verify(actual, "NOT(X = 3 AND Y = 3 AND X < 10)", symbolAliases));
        assertThrows(() -> verify(actual, "NOT(X = 3 AND Y = 3 AND Z < 10)", symbolAliases));
        assertFalse(verify(actual, "NOT(X = 3 AND X = 3 AND X < 10)", symbolAliases));
    }

    @Test
    public void testCast()
    {
        SymbolAliases aliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .build();

        assertTrue(verify("CAST('2' AS varchar)", "CAST('2' AS varchar)", aliases));
        assertFalse(verify("CAST('2' AS varchar)", "CAST('2' AS bigint)", aliases));
        assertTrue(verify("CAST(orderkey AS varchar)", "CAST(X AS varchar)", aliases));
    }

    @Test
    public void testBetween()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        // Complete match
        assertTrue(verify("orderkey BETWEEN 1 AND 2", "X BETWEEN 1 AND 2", symbolAliases));
        // Different value
        assertFalse(verify("orderkey BETWEEN 1 AND 2", "Y BETWEEN 1 AND 2", symbolAliases));
        assertFalse(verify("custkey BETWEEN 1 AND 2", "X BETWEEN 1 AND 2", symbolAliases));
        // Different min or max
        assertFalse(verify("orderkey BETWEEN 2 AND 4", "X BETWEEN 1 AND 2", symbolAliases));
        assertFalse(verify("orderkey BETWEEN 1 AND 2", "X BETWEEN '1' AND '2'", symbolAliases));
        assertFalse(verify("orderkey BETWEEN 1 AND 2", "X BETWEEN 4 AND 7", symbolAliases));
    }

    @Test
    public void testSymmetry()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("a", new SymbolReference("x"))
                .put("b", new SymbolReference("y"))
                .build();

        assertTrue(verify("x > y", "a > b", symbolAliases));
        assertTrue(verify("x > y", "b < a", symbolAliases));
        assertTrue(verify("y < x", "a > b", symbolAliases));
        assertTrue(verify("y < x", "b < a", symbolAliases));

        assertFalse(verify("x < y", "a > b", symbolAliases));
        assertFalse(verify("x < y", "b < a", symbolAliases));
        assertFalse(verify("y > x", "a > b", symbolAliases));
        assertFalse(verify("y > x", "b < a", symbolAliases));

        assertTrue(verify("x >= y", "a >= b", symbolAliases));
        assertTrue(verify("x >= y", "b <= a", symbolAliases));
        assertTrue(verify("y <= x", "a >= b", symbolAliases));
        assertTrue(verify("y <= x", "b <= a", symbolAliases));

        assertFalse(verify("x <= y", "a >= b", symbolAliases));
        assertFalse(verify("x <= y", "b <= a", symbolAliases));
        assertFalse(verify("y >= x", "a >= b", symbolAliases));
        assertFalse(verify("y >= x", "b <= a", symbolAliases));

        assertTrue(verify("x = y", "a = b", symbolAliases));
        assertTrue(verify("x = y", "b = a", symbolAliases));
        assertTrue(verify("y = x", "a = b", symbolAliases));
        assertTrue(verify("y = x", "b = a", symbolAliases));
        assertTrue(verify("x <> y", "a <> b", symbolAliases));
        assertTrue(verify("x <> y", "b <> a", symbolAliases));
        assertTrue(verify("y <> x", "a <> b", symbolAliases));
        assertTrue(verify("y <> x", "b <> a", symbolAliases));

        assertTrue(verify("x IS DISTINCT FROM y", "a IS DISTINCT FROM b", symbolAliases));
        assertTrue(verify("x IS DISTINCT FROM y", "b IS DISTINCT FROM a", symbolAliases));
        assertTrue(verify("y IS DISTINCT FROM x", "a IS DISTINCT FROM b", symbolAliases));
        assertTrue(verify("y IS DISTINCT FROM x", "b IS DISTINCT FROM a", symbolAliases));
    }

    @Test
    public void testLambdaExpressions()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("a", new SymbolReference("x"))
                .put("b", new SymbolReference("y"))
                .build();

        assertTrue(verify(
                "transform(x, p -> p.f0.f0 + y)",
                "transform(a, q -> q.f0.f0 + b)",
                symbolAliases));

        // The same identifier used in multiple lambda expressions
        assertTrue(verify(
                "transform(x, p -> p.f0.f0) || transform(y, p -> p + 2)",
                "transform(a, q -> q.f0.f0) || transform(b, q -> q + 2)",
                symbolAliases));

        assertTrue(verify(
                "transform(x, p -> p.f0.f0) || transform(y, p -> p + 2)",
                "transform(a, q -> q.f0.f0) || transform(b, r -> r + 2)",
                symbolAliases));

        // Nested lambda expressions
        assertTrue(verify(
                "transform(x, p -> transform(array[p], q -> q + 1 + f(p)))",
                "transform(a, q -> transform(array[q], s -> s + 1 + f(q)))",
                symbolAliases));
    }

    private static void assertThrows(Runnable runnable)
    {
        try {
            runnable.run();
            throw new AssertionError("Method didn't throw exception as expected");
        }
        catch (Exception expected) {
        }
    }
}
