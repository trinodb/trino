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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Metadata;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LogicalExpression;
import org.testng.annotations.Test;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static org.testng.Assert.assertEquals;

public class TestExpressionUtils
{
    private final Metadata metadata = createTestMetadataManager();

    @Test
    public void testAnd()
    {
        Expression a = name("a");
        Expression b = name("b");
        Expression c = name("c");
        Expression d = name("d");
        Expression e = name("e");

        assertEquals(
                ExpressionUtils.and(a, b, c, d, e),
                new LogicalExpression(AND, ImmutableList.of(a, b, c, d, e)));

        assertEquals(
                ExpressionUtils.combineConjuncts(metadata, a, b, a, c, d, c, e),
                new LogicalExpression(AND, ImmutableList.of(a, b, c, d, e)));
    }

    private static Identifier name(String name)
    {
        return new Identifier(name);
    }
}
