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

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Identifier;
import org.junit.jupiter.api.Test;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParserAssert.expression;
import static io.trino.sql.parser.ParserAssert.statement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIntervalValueExpression
{
    private final SqlParser parser = new SqlParser();

    @Test
    void testImplicitAliases()
    {
        for (String field : new String[] {"day", "hour", "minute", "second", "year", "month"}) {
            assertThat(statement("SELECT (a - b) " + field + " FROM t"))
                    .ignoringLocation()
                    .isEqualTo(parser.createStatement("SELECT a - b AS " + field + " FROM t"));
            assertThat(statement("SELECT (a - b - c) " + field + " FROM t"))
                    .ignoringLocation()
                    .isEqualTo(parser.createStatement("SELECT a - b - c AS " + field + " FROM t"));
            assertThat(statement("SELECT ((a - b) " + field + ") AS result FROM t"))
                    .ignoringLocation()
                    .isEqualTo(parser.createStatement("SELECT (a - b) " + field + " AS result FROM t"));
            assertThat(expression("(a - b) " + field))
                    .ignoringLocation()
                    .isEqualTo(parser.createExpression("((a - b) " + field + ")"));
        }
        assertThat(statement("SELECT (a - b) DAY(3) FROM t"))
                .ignoringLocation()
                .isEqualTo(parser.createStatement("SELECT ((a - b) DAY(3)) FROM t"));
        assertThat(statement("SELECT (a - b) DAY TO SECOND FROM t"))
                .ignoringLocation()
                .isEqualTo(parser.createStatement("SELECT ((a - b) DAY TO SECOND) FROM t"));
    }

    @Test
    void testAliasExpressionGrouping()
    {
        for (String field : new String[] {"day", "hour", "minute", "second", "year", "month"}) {
            for (String value : new String[] {"a - b + c", "a - b - c", "a - (b + c)", "a + b - c", "a - b || c", "a = b"}) {
                for (String template : new String[] {
                        "SELECT (%s) %s FROM t",
                        "SELECT ROW((%s) %s) FROM t",
                        "SELECT * FROM t PIVOT ((%s) %s FOR k IN (1))",
                        "SELECT * FROM t PIVOT (sum(x) FOR k IN ((%s) %s))",
                }) {
                    String sql = template.formatted(value, field);
                    String explicitAlias = template.formatted(value, "AS " + field);
                    assertThat(statement(sql))
                            .ignoringLocation()
                            .isEqualTo(parser.createStatement(explicitAlias));
                    assertThat(statement(formatSql(parser.createStatement(sql))))
                            .ignoringLocation()
                            .isEqualTo(parser.createStatement(explicitAlias));
                }
            }
        }
    }

    @Test
    void testQualifiedExpressionGrouping()
    {
        for (String[] example : new String[][] {
                {"a - b - c", "(a - b) - c"},
                {"a + b - c", "(a + b) - c"},
                {"a - (b + c)", "a - (b + c)"},
                {"a - (b - c)", "a - (b - c)"},
                {"a * b - c / d", "(a * b) - (c / d)"},
        }) {
            assertThat(expression("(" + example[0] + ") DAY(3)"))
                    .ignoringLocation()
                    .isEqualTo(parser.createExpression("(" + example[1] + ") DAY(3)"));
        }
        for (String value : new String[] {"a - b + c", "a - b || c", "a + b", "a"}) {
            assertThatThrownBy(() -> parser.createExpression("(" + value + ") DAY(3)"))
                    .isInstanceOf(ParsingException.class)
                    .hasMessageContaining("Qualified datetime difference must be a subtraction");
        }
    }

    @Test
    void testQualifiedValuesInAliasedContexts()
    {
        for (String template : new String[] {
                "SELECT %s FROM t",
                "SELECT ROW(%s) FROM t",
                "SELECT * FROM t PIVOT (%s FOR k IN (1))",
                "SELECT * FROM t PIVOT (sum(x) FOR k IN (%s))",
        }) {
            for (String value : new String[] {"(a - b) DAY(3)", "(a - b) DAY TO SECOND", "((a - b) DAY)"}) {
                String sql = template.formatted(value + " AS result");
                assertThat(statement(formatSql(parser.createStatement(sql))))
                        .ignoringLocation()
                        .isEqualTo(parser.createStatement(sql));
            }
        }
    }

    @Test
    void testIntervalLiteralsInSelectItems()
    {
        for (String qualifier : new String[] {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DAY(3)", "SECOND(3, 9)", "YEAR TO MONTH", "DAY TO SECOND", "HOUR TO MINUTE", "MINUTE TO SECOND"}) {
            String literal = "INTERVAL '1' " + qualifier;
            for (String expression : new String[] {literal, literal + " / 2", literal + " = " + literal}) {
                for (String alias : new String[] {"", " result", " AS result"}) {
                    String sql = "SELECT " + expression + alias + " FROM t";
                    assertThat(statement(sql))
                            .ignoringLocation()
                            .isEqualTo(parser.createStatement("SELECT (" + expression + ")" + alias + " FROM t"));
                    assertThat(statement(formatSql(parser.createStatement(sql))))
                            .ignoringLocation()
                            .isEqualTo(parser.createStatement(sql));
                }
            }
        }
    }

    @Test
    void testRewritingOperands()
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new Identifier(node.getLocation().orElseThrow(), node.getValue() + "1", false);
            }
        }, parser.createExpression("(a - b) SECOND(3, 9)"));
        assertThat(expression("(a1 - b1) SECOND(3, 9)"))
                .ignoringLocation()
                .isEqualTo(rewritten);
    }
}
