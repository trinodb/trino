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
