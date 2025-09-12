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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.parser.ParserAssert.expression;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionFormatter
{
    @Test
    public void testIdentifier()
    {
        assertFormattedExpression(
                new Identifier("abc"),
                "abc");
        assertFormattedExpression(
                new Identifier("with a space"),
                "\"with a space\"");
        assertFormattedExpression(
                new Identifier("with \" quote, $ dollar and ' apostrophe"),
                "\"with \"\" quote, $ dollar and ' apostrophe\"");
    }

    @Test
    public void testStringLiteral()
    {
        assertFormattedExpression(
                new StringLiteral("test"),
                "'test'");
        assertFormattedExpression(
                new StringLiteral("攻殻機動隊"),
                "'攻殻機動隊'");
        assertFormattedExpression(
                new StringLiteral("😂"),
                "'😂'");
    }

    @Test
    public void testCharLiteral()
    {
        assertFormattedExpression(
                new GenericLiteral("CHAR", "test"),
                "CHAR 'test'");
        assertFormattedExpression(
                new GenericLiteral("CHAR", "攻殻機動隊"),
                "CHAR '攻殻機動隊'");
        assertFormattedExpression(
                new GenericLiteral("CHAR", "😂"),
                "CHAR '😂'");
    }

    @Test
    public void testGenericLiteral()
    {
        assertFormattedExpression(
                new GenericLiteral("VARCHAR", "test"),
                "VARCHAR 'test'");
        assertFormattedExpression(
                new GenericLiteral("VARCHAR", "攻殻機動隊"),
                "VARCHAR '攻殻機動隊'");
        assertFormattedExpression(
                new GenericLiteral("VARCHAR", "😂"),
                "VARCHAR '😂'");
    }

    @Test
    public void testIntervalLiteral()
    {
        // positive
        assertFormattedExpression(
                new IntervalLiteral("2", POSITIVE, HOUR),
                "INTERVAL '2' HOUR");
        // negative
        assertFormattedExpression(
                new IntervalLiteral("2", NEGATIVE, HOUR),
                "INTERVAL -'2' HOUR");

        // from .. to
        assertFormattedExpression(
                new IntervalLiteral("2", POSITIVE, HOUR, Optional.of(SECOND)),
                "INTERVAL '2' HOUR TO SECOND");

        // negative from .. to
        assertFormattedExpression(
                new IntervalLiteral("2", NEGATIVE, HOUR, Optional.of(SECOND)),
                "INTERVAL -'2' HOUR TO SECOND");
    }

    @Test
    public void testRowLiteral()
    {
        assertFormattedExpression(
                createRow("a"),
                "ROW('v0' AS a)");
        assertFormattedExpression(
                createRow((String) null),
                "ROW('v0')");
        assertFormattedExpression(
                createRow("a", null, "b"),
                "ROW('v0' AS a, 'v1', 'v2' AS b)");
        assertFormattedExpression(
                new Row(ImmutableList.of(createRowField("x", new Row(ImmutableList.of(createRowField("y", createRow("a", null, "b"))))))),
                "ROW(ROW(ROW('v0' AS a, 'v1', 'v2' AS b) AS y) AS x)");
        assertFormattedExpression(
                new Row(ImmutableList.of(createRowField(null, new Row(ImmutableList.of(createRowField(null, createRow("a", null, "b"))))))),
                "ROW(ROW(ROW('v0' AS a, 'v1', 'v2' AS b)))");
    }

    private static Row createRow(String... fieldNames)
    {
        ImmutableList.Builder<Row.Field> fields = ImmutableList.builder();
        for (int i = 0; i < fieldNames.length; i++) {
            fields.add(createRowField(fieldNames[i], new StringLiteral("v" + i)));
        }
        return new Row(fields.build());
    }

    private static Row.Field createRowField(String fieldName, Expression expression)
    {
        return new Row.Field(new NodeLocation(1, 1), Optional.ofNullable(fieldName).map(Identifier::new), expression);
    }

    private void assertFormattedExpression(Expression expression, String expected)
    {
        assertThat(ExpressionFormatter.formatExpression(expression)).as("formatted expression")
                .isEqualTo(expected);

        // validate that the expected parses back
        assertThat(expression(expected))
                .ignoringLocation()
                .isEqualTo(expression);
    }
}
