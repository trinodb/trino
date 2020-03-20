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
package io.prestosql.sql.planner;

import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.SymbolAliases;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static io.prestosql.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.prestosql.sql.planner.OrExpressionNormalizer.normalizeOrExpressions;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestOrExpressionNormalizer
        extends BasePlanTest
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testSimpleOrExpressions()
    {
        assertExpression(
                "x = 0 OR x = 1 OR x = 2 OR x = 3",
                "x IN (0, 1, 2, 3)");
        assertExpression(
                "(x = 0 OR x = 1) OR (x = 2 OR x = 3)",
                "x IN (0, 1, 2, 3)");
        assertExpression(
                "x = 0 OR (x = 1 OR (x = 2 OR x = 3))",
                "x IN (0, 1, 2, 3)");
        assertExpression(
                "x = 0 OR (x = 1 OR (x = 2 OR x = a))",
                "x IN (0, a, 1, 2)");
        assertExpression(
                "x = 0 OR x in (1, 2, 3)",
                "x IN (0, 1, 2, 3)");
        assertExpression(
                "x > 10 OR x IN (1, 2, 3)",
                "x > 10 OR x IN (1, 2, 3)");
    }

    @Test
    public void testOnMultiColumns()
    {
        assertExpression(
                "a = 0 OR a = 1 OR x = 2 OR x = 3",
                "a IN (0, 1) OR x IN (2, 3)");
        assertExpression(
                "(a = 0 OR x = 1) OR (a = 2 OR x = 3)",
                "a IN (0, 2) OR x IN (1, 3)");
        assertExpression(
                "a = 0 OR (a = 1 OR (x = 2 OR x = 3))",
                "a IN (0, 1) OR x IN (2, 3)");
        assertExpression(
                "(a = 0 OR a = 1) AND (x = 2 OR x = 3)",
                "a IN (0, 1) and x IN (2, 3)");
        assertExpression(
                "a = 0 OR a = 1 AND x = 2 OR x = 3",
                "(a = 1 AND x = 2) OR a = 0 OR x = 3");
        assertExpression(
                "a IN (1, 2) OR x IN (1, 2)",
                "a IN (1, 2) OR x IN (1, 2)");
    }

    @Test
    public void testWithComplexCondition()
    {
        assertExpression(
                "(a = 0 OR x = 1) AND (a IN (6, 7) OR x = 2)",
                "(a = 0 OR x = 1) AND (a IN (6, 7) OR x = 2)");
        assertExpression(
                "a = 0 OR a = 1 OR (CASE WHEN (a = 10 OR a = 20) THEN x = 40 OR x = 50 END)",
                "((CASE WHEN (a IN (20, 10)) THEN (x IN (40, 50)) END) OR (a IN (0, 1)))");
    }

    @Test
    public void testNotIn()
    {
        assertExpression(
                "a NOT IN (0, 1) AND a <> 5",
                "a NOT IN (0, 1) AND a <> 5");
    }

    private void assertExpression(String from, String to)
    {
        assertExpressionEquals(
                normalizeOrExpressions(METADATA, expression(from)),
                expression(to),
                SymbolAliases.builder()
                        .put("a", new SymbolReference("a"))
                        .put("x", new SymbolReference("x"))
                        .build());
    }
}
