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
package io.trino.sql.parser;

import io.trino.sql.tree.CollateExpression;
import io.trino.sql.tree.CollatedDataType;
import io.trino.sql.tree.CollationFor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.PadAttribute;
import io.trino.sql.tree.ShowCollations;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParserAssert.expression;
import static org.assertj.core.api.Assertions.assertThat;

class TestCollateExpression
{
    private final SqlParser parser = new SqlParser();

    @Test
    void testPrecedence()
    {
        // COLLATE binds to its operand, tighter than concatenation and arithmetic
        assertSameAst("a || b COLLATE unicode", "a || (b COLLATE unicode)");
        assertSameAst("a COLLATE unicode || b", "(a COLLATE unicode) || b");
        assertSameAst("a = b COLLATE unicode", "a = (b COLLATE unicode)");
        assertSameAst("x COLLATE unicode COLLATE unicode_ci", "(x COLLATE unicode) COLLATE unicode_ci");
        assertSameAst("-a COLLATE unicode", "-(a COLLATE unicode)");
        // a parenthesized operand collates the whole expression
        Expression grouped = parser.createExpression("(a || b) COLLATE unicode");
        assertThat(grouped).isInstanceOf(CollateExpression.class);
        assertThat(((CollateExpression) grouped).getCollation()).isEqualTo(new Identifier("unicode"));
    }

    @Test
    void testCollationFor()
    {
        Expression expression = parser.createExpression("COLLATION FOR (x || y)");
        assertThat(expression).isInstanceOf(CollationFor.class);
        assertSameAst("COLLATION FOR (x COLLATE unicode)", "COLLATION FOR ((x COLLATE unicode))");
    }

    @Test
    void testKeywordsRemainIdentifiers()
    {
        assertThat(parser.createExpression("collate")).isEqualTo(new Identifier("collate"));
        assertThat(parser.createExpression("collation")).isEqualTo(new Identifier("collation"));
        assertThat(parser.createStatement("SELECT collation, collate FROM t")).isNotNull();
        // a function named collation is still a function call
        assertThat(parser.createExpression("collation(x)")).isNotInstanceOf(CollationFor.class);
    }

    @Test
    void testTypeCollation()
    {
        // a COLLATE clause in a type is part of the type, not an expression collation
        CollatedDataType type = (CollatedDataType) parser.createType("varchar(10) COLLATE unicode_ci");
        assertThat(type.getCollation()).isEqualTo(new Identifier("unicode_ci"));
        assertSameAst("CAST(x AS varchar COLLATE unicode)", "CAST(x AS varchar COLLATE unicode)");
    }

    @Test
    void testPadAttribute()
    {
        CollatedDataType padSpace = (CollatedDataType) parser.createType("varchar(10) COLLATE unicode_ci PAD SPACE");
        assertThat(padSpace.getCollation()).isEqualTo(new Identifier("unicode_ci"));
        assertThat(padSpace.getPadAttribute()).contains(PadAttribute.PAD_SPACE);
        assertThat(((CollatedDataType) parser.createType("varchar COLLATE unicode NO PAD")).getPadAttribute()).contains(PadAttribute.NO_PAD);
        assertThat(((CollatedDataType) parser.createType("varchar COLLATE unicode")).getPadAttribute()).isEmpty();

        CollateExpression expression = (CollateExpression) parser.createExpression("x COLLATE unicode PAD SPACE");
        assertThat(expression.getCollation()).isEqualTo(new Identifier("unicode"));
        assertThat(expression.getPadAttribute()).contains(PadAttribute.PAD_SPACE);
        assertThat(((CollateExpression) parser.createExpression("x COLLATE unicode NO PAD")).getPadAttribute()).contains(PadAttribute.NO_PAD);
        // the pad attribute binds to COLLATE, before any following operator
        assertSameAst("x COLLATE unicode PAD SPACE = y", "(x COLLATE unicode PAD SPACE) = y");
        // an explicit NO PAD is distinct syntax from an omitted pad attribute
        assertThat(parser.createExpression("x COLLATE unicode NO PAD")).isNotEqualTo(parser.createExpression("x COLLATE unicode"));

        // pad, space, and no remain identifiers
        assertThat(parser.createExpression("pad")).isEqualTo(new Identifier("pad"));
        assertThat(parser.createExpression("space")).isEqualTo(new Identifier("space"));
        assertThat(parser.createStatement("SELECT pad, space, no FROM t")).isNotNull();
        // a collation may be named pad
        assertThat(((CollateExpression) parser.createExpression("x COLLATE pad")).getCollation()).isEqualTo(new Identifier("pad"));
        assertThat(((CollateExpression) parser.createExpression("x COLLATE pad PAD SPACE")).getPadAttribute()).contains(PadAttribute.PAD_SPACE);
    }

    @Test
    void testShowCollations()
    {
        assertThat(parser.createStatement("SHOW COLLATIONS")).isEqualTo(new ShowCollations(new NodeLocation(1, 1), Optional.empty(), Optional.empty()));
        assertThat(parser.createStatement("SHOW COLLATIONS LIKE 'de%' ESCAPE '$'"))
                .isEqualTo(new ShowCollations(new NodeLocation(1, 1), Optional.of("de%"), Optional.of("$")));
        assertThat(formatSql(parser.createStatement("SHOW COLLATIONS LIKE 'de%' ESCAPE '$'"))).isEqualTo("SHOW COLLATIONS LIKE 'de%' ESCAPE '$'");
        // collations remains an identifier
        assertThat(parser.createExpression("collations")).isEqualTo(new Identifier("collations"));
    }

    @Test
    void testFormatting()
    {
        for (String sql : new String[] {
                "a || b COLLATE unicode",
                "(a || b) COLLATE unicode",
                "x COLLATE unicode COLLATE unicode_ci",
                "COLLATION FOR (x COLLATE unicode)",
                "CAST(x AS varchar(10) COLLATE unicode_ci)",
                "CAST(x AS varchar(10) COLLATE unicode_ci PAD SPACE)",
                "CAST(x AS varchar COLLATE unicode NO PAD)",
                "x COLLATE unicode PAD SPACE",
                "x COLLATE unicode NO PAD",
        }) {
            Expression parsed = parser.createExpression(sql);
            assertThat(expression(formatExpression(parsed)))
                    .as(sql)
                    .ignoringLocation()
                    .isEqualTo(parsed);
        }
        assertThat(formatExpression(parser.createExpression("a || b COLLATE unicode"))).isEqualTo("concat(a, (b COLLATE unicode))");
        assertThat(formatSql(parser.createStatement("SELECT x COLLATE unicode FROM t ORDER BY y COLLATE unicode_ci")))
                .contains("(x COLLATE unicode)")
                .contains("(y COLLATE unicode_ci)");
    }

    private void assertSameAst(String sql, String expected)
    {
        assertThat(expression(sql))
                .ignoringLocation()
                .isEqualTo(parser.createExpression(expected));
    }
}
