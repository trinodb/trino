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

package io.trino.plugin.deltalake.expression;

import io.trino.plugin.deltalake.expression.ArithmeticBinaryExpression.Operator;
import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.expression.SparkExpressionParser.createExpression;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSparkExpressionParser
{
    @Test
    public void testStringLiteral()
    {
        assertStringLiteral("''", "");
        assertStringLiteral("'abc'", "abc");
        assertStringLiteral("'NULL'", "NULL");

        assertStringLiteral("'あ'", "あ");
        assertStringLiteral("'\\u3042'", "あ");
        assertStringLiteral("'👍'", "👍");
        assertStringLiteral("'\\U0001F44D'", "👍");

        assertStringLiteral("'a''quote'", "a'quote");
        assertStringLiteral("\"double-quote\"", "double-quote");
        assertStringLiteral("\"a\"\"double-quote\"", "a\"double-quote");
    }

    @Test
    public void testUnsupportedStringLiteral()
    {
        // r prefix is unsupported
        assertParseFailure("r'raw literal'", "extraneous input ''raw literal'' expecting <EOF>");
        assertParseFailure("r\"'\\n' represents newline character.\"", "extraneous input '\"'\\n' represents newline character.\"' expecting <EOF>");

        // Spark allows spaces after 'r' for raw literals
        assertParseFailure("r 'a space after prefix'", "extraneous input ''a space after prefix'' expecting <EOF>");
        assertParseFailure("r  'two spaces after prefix'", "extraneous input ''two spaces after prefix'' expecting <EOF>");
    }

    @Test
    public void testArithmeticBinary()
    {
        assertEquals(createExpression("a + b * c"), new ArithmeticBinaryExpression(
                Operator.ADD,
                new Identifier("a"),
                new ArithmeticBinaryExpression(Operator.MULTIPLY, new Identifier("b"), new Identifier("c"))));

        assertEquals(createExpression("a * b + c"), new ArithmeticBinaryExpression(
                Operator.ADD,
                new ArithmeticBinaryExpression(Operator.MULTIPLY, new Identifier("a"), new Identifier("b")),
                new Identifier("c")));
    }

    private static void assertStringLiteral(String sparkExpression, String expected)
    {
        SparkExpression expression = createExpression(sparkExpression);
        assertEquals(expression, new StringLiteral(expected));
    }

    private static void assertParseFailure(String sparkExpression, String reason)
    {
        assertThatThrownBy(() -> createExpression(sparkExpression))
                .hasMessage("Cannot parse Spark expression [%s]: %s", sparkExpression, reason);
    }
}
