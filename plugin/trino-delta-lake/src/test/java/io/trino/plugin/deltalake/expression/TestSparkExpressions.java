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

import io.trino.spi.TrinoException;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSparkExpressions
{
    @Test
    public void testBoolean()
    {
        assertExpressionTranslates("a = true", "(\"a\" = true)");
        assertExpressionTranslates("true", "true");
    }

    @Test
    public void testString()
    {
        assertExpressionTranslates("a = 'test'", "(\"a\" = 'test')");

        // Spark uses 16-bit or 32-bit unicode escape of the form \\u**** or \\U********
        assertExpressionTranslates("a = '\\u3042'", "(\"a\" = 'あ')");
        assertExpressionTranslates("a = '\\U0001F44D'", "(\"a\" = '👍')");

        // Spark supports both ' and " for string literals
        assertExpressionTranslates("a = 'quote'", "(\"a\" = 'quote')");
        assertExpressionTranslates("a = 'a''quote'", "(\"a\" = 'a''quote')");
        assertExpressionTranslates("a = \"double-quote\"", "(\"a\" = 'double-quote')");
        assertExpressionTranslates("a = \"a\"\"double-quote\"", "(\"a\" = 'a\"double-quote')");

        // TODO: Support non-hexadecimal special characters. \n should insert a new empty line
        assertParseFailure("a = 'a\\nwrap'");
    }

    @Test
    public void testNumber()
    {
        assertExpressionTranslates("a = -1", "(\"a\" = -1)");
    }

    @Test
    public void testEquals()
    {
        assertExpressionTranslates("a = 1", "(\"a\" = 1)");
        assertExpressionTranslates("a = 'test'", "(\"a\" = 'test')");
    }

    @Test
    public void testNotEquals()
    {
        assertExpressionTranslates("a <> 1", "(\"a\" <> 1)");
        assertExpressionTranslates("a != 1", "(\"a\" <> 1)");
    }

    @Test
    public void testLessThan()
    {
        assertExpressionTranslates("a < 1", "(\"a\" < 1)");
    }

    @Test
    public void testLessThanOrEquals()
    {
        assertExpressionTranslates("a <= 1", "(\"a\" <= 1)");
    }

    @Test
    public void testGraterThan()
    {
        assertExpressionTranslates("a > 1", "(\"a\" > 1)");
    }

    @Test
    public void testGraterThanOrEquals()
    {
        assertExpressionTranslates("a >= 1", "(\"a\" >= 1)");
    }

    @Test
    public void testAnd()
    {
        assertExpressionTranslates("a > 1 AND a < 10", "((\"a\" > 1) AND (\"a\" < 10))");
    }

    @Test
    public void testOr()
    {
        assertExpressionTranslates("a > 1 OR a < 10", "((\"a\" > 1) OR (\"a\" < 10))");
    }

    @Test
    public void testIdentifier()
    {
        // Spark uses ` for identifiers
        assertExpressionTranslates("`a` = 1", "(\"a\" = 1)");
        assertExpressionTranslates("`UPPERCASE` = 1", "(\"UPPERCASE\" = 1)");
        assertExpressionTranslates("`123` = 1", "(\"123\" = 1)");

        // Special characters
        assertExpressionTranslates("`a.dot` = 1", "(\"a.dot\" = 1)");
        assertExpressionTranslates("`a``backtick` = 1", "(\"a`backtick\" = 1)");
        assertExpressionTranslates("`a,comma` = 1", "(\"a,comma\" = 1)");
        assertExpressionTranslates("`a;semicolon` = 1", "(\"a;semicolon\" = 1)");
        assertExpressionTranslates("`a{}braces` = 1", "(\"a{}braces\" = 1)");
        assertExpressionTranslates("`a()parenthesis` = 1", "(\"a()parenthesis\" = 1)");
        assertExpressionTranslates("`a\nnewline` = 1", "(\"a\nnewline\" = 1)");
        assertExpressionTranslates("`a\ttab` = 1", "(\"a\ttab\" = 1)");
        assertExpressionTranslates("`a.dot` = 1", "(\"a.dot\" = 1)");
        assertExpressionTranslates("`a=equal` = 1", "(\"a=equal\" = 1)");

        assertParseFailure("`a`b` = 1");
    }

    @Test
    public void testInvalidNotBoolean()
    {
        assertParseFailure("a + a");
    }

    // TODO: Support following expressions

    @Test
    public void testUnsupportedRawStringLiteral()
    {
        assertParseFailure("a = r'Some\nText'");
    }

    @Test
    public void testUnsupportedNot()
    {
        assertParseFailure("NOT a = 1");
    }

    @Test
    public void testUnsupportedOperator()
    {
        assertParseFailure("a <=> 1");
        assertParseFailure("a == 1");
        assertParseFailure("a = b % 1");
        assertParseFailure("a = b & 1");
        assertParseFailure("a = b * 1");
        assertParseFailure("a = b + 1");
        assertParseFailure("a = b - 1");
        assertParseFailure("a = b / 1");
        assertParseFailure("a = b ^ 1");
        assertParseFailure("a = b::INTEGER");
        assertParseFailure("a = json_column:root");
        assertParseFailure("a BETWEEN 1 AND 10");
        assertParseFailure("a IS NULL");
        assertParseFailure("a IS DISTINCT FROM b");
        assertParseFailure("a IS true");
        assertParseFailure("a = 'Spark' || 'SQL'");
        assertParseFailure("a = 3 | 5");
        assertParseFailure("a = ~ 0");
        assertParseFailure("a = cast(TIMESTAMP'1970-01-01 00:00:01' AS LONG)");
    }

    @Test
    public void testUnsupportedTypes()
    {
        assertParseFailure("a = 123.456");
        assertParseFailure("a = x'123456'");
        assertParseFailure("a = date '2021-01-01'");
        assertParseFailure("a = timestamp '2021-01-01 00:00:00'");
        assertParseFailure("a = array(1, 2, 3)");
        assertParseFailure("a = map(1.0, '2', 3.0, '4')");
        assertParseFailure("a = struct(1, 2, 3)");

        assertParseFailure("a[0] = 1");
    }

    @Test
    public void testUnsupportedCallFunction()
    {
        assertParseFailure("a = abs(-1)");
    }

    private static void assertExpressionTranslates(@Language("SQL") String sparkExpression, @Language("SQL") String trinoExpression)
    {
        assertEquals(toTrinoExpression(sparkExpression), trinoExpression);
    }

    private static String toTrinoExpression(@Language("SQL") String sparkExpression)
    {
        return SparkExpressionParser.toTrinoExpression(sparkExpression);
    }

    private static void assertParseFailure(@Language("SQL") String sparkExpression)
    {
        assertThatThrownBy(() -> toTrinoExpression(sparkExpression))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unsupported Spark expression: " + sparkExpression);
    }
}
