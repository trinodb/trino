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
import static io.prestosql.sql.planner.OrExpressionNormalizer.rewrite;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestOrExpressionNormalizer
        extends BasePlanTest
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testSimpleOrExpressions()
    {
        assertExpression("x = 0 or x = 1 or x = 2 or x = 3", "x in (0, 1, 2, 3)");
        assertExpression("(x = 0 or x = 1) or (x = 2 or x = 3)", "x in (0, 1, 2, 3)");
        assertExpression("x = 0 or (x = 1 or (x = 2 or x = 3))", "x in (0, 1, 2, 3)");
        assertExpression("x = 0 or (x = 1 or (x = 2 or x = a))", "x in (0, a, 1, 2)");
        assertExpression("x = 0 or x in (1, 2, 3)", "x in (0, 1, 2, 3)");
        assertExpression("x > 10 or x in (1, 2, 3)", "x > 10 or x in (1, 2, 3)");
    }

    @Test
    public void testOnMultiColumns()
    {
        assertExpression("a = 0 or a = 1 or x = 2 or x = 3", "a in (0, 1) or x in (2, 3)");
        assertExpression("(a = 0 or x = 1) or (a = 2 or x = 3)", "a in (0, 2) or x in (1, 3)");
        assertExpression("a = 0 or (a = 1 or (x = 2 or x = 3))", "a in (0, 1) or x in (2, 3)");
        assertExpression("(a = 0 or a = 1) and (x = 2 or x = 3)", "a in (0, 1) and x in (2, 3)");
    }

    @Test
    public void testWithComplexCondition()
    {
        assertExpression("(a = 0 or x = 1) and (a in (6, 7) or x = 2)", "(a = 0 or x = 1) and (a in (6, 7) or x = 2)");
        assertExpression("a = 0 or a = 1 or (CASE WHEN (a = 10 or a = 20) THEN x = 40 or x = 50 END)", "((CASE WHEN (a IN (20, 10)) THEN (x IN (40, 50)) END) OR (a IN (0, 1)))");
    }

    @Test
    public void testNotIn()
    {
        assertExpression("a not in (0, 1) and a <> 5", "a not in (0, 1) and a <> 5");
    }

    private void assertExpression(String from, String to)
    {
        assertExpressionEquals(
                rewrite(METADATA, expression(from)),
                expression(to),
                SymbolAliases.builder()
                        .put("a", new SymbolReference("a"))
                        .put("x", new SymbolReference("x"))
                        .build());
    }
}
