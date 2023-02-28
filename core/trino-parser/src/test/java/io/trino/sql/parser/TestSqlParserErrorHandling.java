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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSqlParserErrorHandling
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions();

    private static Stream<Arguments> expressions()
    {
        return Stream.of(
                Arguments.of("", "line 1:1: mismatched input '<EOF>'. Expecting: <expression>"),
                Arguments.of("1 + 1 x", "line 1:7: mismatched input 'x'. Expecting: '%', '*', '+', '-', '.', '/', 'AND', 'AT', 'OR', '[', '||', <EOF>, <predicate>"));
    }

    private static Stream<Arguments> statements()
    {
        return Stream.of(
                Arguments.of("",
                        "line 1:1: mismatched input '<EOF>'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMENT', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DENY', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', " +
                                "'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'TRUNCATE', 'UPDATE', 'USE', <query>"),
                Arguments.of("@select",
                        "line 1:1: mismatched input '@'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMENT', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DENY', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', " +
                                "'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'TRUNCATE', 'UPDATE', 'USE', <query>"),
                Arguments.of("select * from foo where @what",
                        "line 1:25: mismatched input '@'. Expecting: <expression>"),
                Arguments.of("select * from 'oops",
                        "line 1:15: mismatched input '''. Expecting: '(', 'LATERAL', 'TABLE', 'UNNEST', <identifier>"),
                Arguments.of("select *\nfrom x\nfrom",
                        "line 3:1: mismatched input 'from'. Expecting: ',', '.', 'AS', 'CROSS', 'EXCEPT', 'FETCH', 'FOR', 'FULL', 'GROUP', 'HAVING', 'INNER', 'INTERSECT', 'JOIN', 'LEFT', " +
                                "'LIMIT', 'MATCH_RECOGNIZE', 'NATURAL', 'OFFSET', 'ORDER', 'RIGHT', 'TABLESAMPLE', 'UNION', 'WHERE', 'WINDOW', <EOF>, <identifier>"),
                Arguments.of("select *\nfrom x\nwhere from",
                        "line 3:7: mismatched input 'from'. Expecting: <expression>"),
                Arguments.of("select ",
                        "line 1:8: mismatched input '<EOF>'. Expecting: '*', 'ALL', 'DISTINCT', <expression>"),
                Arguments.of("select * from",
                        "line 1:14: mismatched input '<EOF>'. Expecting: '(', 'LATERAL', 'TABLE', 'UNNEST', <identifier>"),
                Arguments.of("select * from  ",
                        "line 1:16: mismatched input '<EOF>'. Expecting: '(', 'LATERAL', 'TABLE', 'UNNEST', <identifier>"),
                Arguments.of("select * from `foo`",
                        "line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers"),
                Arguments.of("select * from foo `bar`",
                        "line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers"),
                Arguments.of("select 1x from dual",
                        "line 1:8: identifiers must not start with a digit; surround the identifier with double quotes"),
                Arguments.of("select fuu from dual order by fuu order by fuu",
                        "line 1:35: mismatched input 'order'. Expecting: '%', '*', '+', ',', '-', '.', '/', 'AND', 'ASC', 'AT', 'DESC', 'FETCH', 'LIMIT', 'NULLS', 'OFFSET', 'OR', '[', '||', <EOF>, <predicate>"),
                Arguments.of("select fuu from dual limit 10 order by fuu",
                        "line 1:31: mismatched input 'order'. Expecting: <EOF>"),
                Arguments.of("select CAST(12223222232535343423232435343 AS BIGINT)",
                        "line 1:1: Invalid numeric literal: 12223222232535343423232435343"),
                Arguments.of("select CAST(-12223222232535343423232435343 AS BIGINT)",
                        "line 1:1: Invalid numeric literal: -12223222232535343423232435343"),
                Arguments.of("select foo.!",
                        "line 1:12: mismatched input '!'. Expecting: '*', <identifier>"),
                Arguments.of("select foo(,1)",
                        "line 1:12: mismatched input ','. Expecting: ')', '*', 'ALL', 'DISTINCT', 'ORDER', <expression>"),
                Arguments.of("select foo ( ,1)",
                        "line 1:14: mismatched input ','. Expecting: ')', '*', 'ALL', 'DISTINCT', 'ORDER', <expression>"),
                Arguments.of("select foo(DISTINCT)",
                        "line 1:20: mismatched input ')'. Expecting: <expression>"),
                Arguments.of("select foo(DISTINCT ,1)",
                        "line 1:21: mismatched input ','. Expecting: <expression>"),
                Arguments.of("CREATE )",
                        "line 1:8: mismatched input ')'. Expecting: 'CATALOG', 'MATERIALIZED', 'OR', 'ROLE', 'SCHEMA', 'TABLE', 'VIEW'"),
                Arguments.of("CREATE TABLE ) AS (VALUES 1)",
                        "line 1:14: mismatched input ')'. Expecting: 'IF', <identifier>"),
                Arguments.of("CREATE TABLE foo ",
                        "line 1:18: mismatched input '<EOF>'. Expecting: '(', '.', 'AS', 'COMMENT', 'WITH'"),
                Arguments.of("CREATE TABLE foo () AS (VALUES 1)",
                        "line 1:19: mismatched input ')'. Expecting: 'LIKE', <identifier>"),
                Arguments.of("CREATE TABLE foo (*) AS (VALUES 1)",
                        "line 1:19: mismatched input '*'. Expecting: 'LIKE', <identifier>"),
                Arguments.of("SELECT grouping(a+2) FROM (VALUES (1)) AS t (a) GROUP BY a+2",
                        "line 1:18: mismatched input '+'. Expecting: ')', ',', '.'"),
                Arguments.of("SELECT x() over (ROWS select) FROM t",
                        "line 1:23: mismatched input 'select'. Expecting: ')', 'BETWEEN', 'CURRENT', 'GROUPS', 'MEASURES', 'ORDER', 'PARTITION', 'RANGE', 'ROWS', 'UNBOUNDED', <expression>"),
                Arguments.of("SELECT X() OVER (ROWS UNBOUNDED) FROM T",
                        "line 1:32: mismatched input ')'. Expecting: '%', '(', '*', '+', '-', '->', '.', '/', 'AND', 'AT', 'FOLLOWING', 'OR', 'OVER', 'PRECEDING', '[', '||', <predicate>, <string>"),
                Arguments.of("SELECT a FROM x ORDER BY (SELECT b FROM t WHERE ",
                        "line 1:49: mismatched input '<EOF>'. Expecting: <expression>"),
                Arguments.of("SELECT a FROM a AS x TABLESAMPLE x ",
                        "line 1:34: mismatched input 'x'. Expecting: 'BERNOULLI', 'SYSTEM'"),
                Arguments.of("SELECT a AS z FROM t GROUP BY CUBE (a), ",
                        "line 1:41: mismatched input '<EOF>'. Expecting: '(', 'CUBE', 'GROUPING', 'ROLLUP', <expression>"),
                Arguments.of("SELECT a AS z FROM t WHERE x = 1 + ",
                        "line 1:36: mismatched input '<EOF>'. Expecting: <expression>"),
                Arguments.of("SELECT a AS z FROM t WHERE a. ",
                        "line 1:29: mismatched input '.'. Expecting: '%', '*', '+', '-', '/', 'AND', 'AT', 'EXCEPT', 'FETCH', 'GROUP', 'HAVING', 'INTERSECT', 'LIMIT', 'OFFSET', 'OR', 'ORDER', 'UNION', 'WINDOW', '||', <EOF>, <predicate>"),
                Arguments.of("CREATE TABLE t (x bigint) COMMENT ",
                        "line 1:35: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SELECT * FROM ( ",
                        "line 1:17: mismatched input '<EOF>'. Expecting: '(', 'LATERAL', 'TABLE', 'UNNEST', <identifier>, <query>"),
                Arguments.of("SELECT CAST(a AS )",
                        "line 1:18: mismatched input ')'. Expecting: <type>"),
                Arguments.of("SELECT CAST(a AS decimal()",
                        "line 1:26: mismatched input ')'. Expecting: <integer>, <type>"),
                Arguments.of("SELECT foo(*) filter (",
                        "line 1:23: mismatched input '<EOF>'. Expecting: 'WHERE'"),
                Arguments.of("SELECT * FROM t t x",
                        "line 1:19: mismatched input 'x'. Expecting: '(', ',', 'CROSS', 'EXCEPT', 'FETCH', 'FULL', 'GROUP', 'HAVING', 'INNER', 'INTERSECT', 'JOIN', 'LEFT', 'LIMIT', " +
                                "'MATCH_RECOGNIZE', 'NATURAL', 'OFFSET', 'ORDER', 'RIGHT', 'TABLESAMPLE', 'UNION', 'WHERE', 'WINDOW', <EOF>"),
                Arguments.of("SELECT * FROM t WHERE EXISTS (",
                        "line 1:31: mismatched input '<EOF>'. Expecting: <query>"),
                Arguments.of("SELECT \"\" FROM t",
                        "line 1:8: Zero-length delimited identifier not allowed"),
                Arguments.of("SELECT a FROM \"\"",
                        "line 1:15: Zero-length delimited identifier not allowed"),
                Arguments.of("SELECT a FROM \"\".t",
                        "line 1:15: Zero-length delimited identifier not allowed"),
                Arguments.of("SELECT a FROM \"\".s.t",
                        "line 1:15: Zero-length delimited identifier not allowed"),
                Arguments.of("WITH t AS (SELECT 1 SELECT t.* FROM t",
                        "line 1:21: mismatched input 'SELECT'. Expecting: '%', ')', '*', '+', ',', '-', '.', '/', 'AND', 'AS', 'AT', 'EXCEPT', 'FETCH', 'FROM', " +
                                "'GROUP', 'HAVING', 'INTERSECT', 'LIMIT', 'OFFSET', 'OR', 'ORDER', 'UNION', 'WHERE', 'WINDOW', '[', '||', " +
                                "<identifier>, <predicate>"),
                Arguments.of("SHOW CATALOGS LIKE '%$_%' ESCAPE",
                        "line 1:33: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW SCHEMAS IN foo LIKE '%$_%' ESCAPE",
                        "line 1:39: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW FUNCTIONS LIKE '%$_%' ESCAPE",
                        "line 1:34: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW SESSION LIKE '%$_%' ESCAPE",
                        "line 1:32: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW CATALOGS LIKE '%$_%' ESCAPE",
                        "line 1:33: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW SCHEMAS IN foo LIKE '%$_%' ESCAPE",
                        "line 1:39: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW FUNCTIONS LIKE '%$_%' ESCAPE",
                        "line 1:34: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SHOW SESSION LIKE '%$_%' ESCAPE",
                        "line 1:32: mismatched input '<EOF>'. Expecting: <string>"),
                Arguments.of("SELECT * FROM t FOR TIMESTAMP ",
                        "line 1:31: mismatched input '<EOF>'. Expecting: 'AS'"),
                Arguments.of("SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP WHERE",
                        "line 1:52: mismatched input '<EOF>'. Expecting: <expression>"),
                Arguments.of("SELECT * FROM t FOR VERSION AS OF TIMESTAMP WHERE",
                        "line 1:50: mismatched input '<EOF>'. Expecting: <expression>"),
                Arguments.of("SELECT ROW(DATE '2022-10-10', DOUBLE 12.0)",
                        "line 1:38: mismatched input '12.0'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', 'AND', 'AT', 'OR', 'ORDER', 'OVER', 'PRECISION', '[', '||', <predicate>, <string>"),
                Arguments.of("VALUES(DATE 2)",
                        "line 1:13: mismatched input '2'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', 'AND', 'AT', 'OR', 'OVER', '[', '||', <predicate>, <string>"),
                Arguments.of("SELECT count(DISTINCT *) FROM (VALUES 1)",
                        "line 1:23: mismatched input '*'. Expecting: <expression>"));
    }

    @Test
    @Timeout(value = 1, unit = SECONDS)
    public void testPossibleExponentialBacktracking()
    {
        testStatement("SELECT CASE WHEN " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * " +
                        "1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9",
                "line 1:375: mismatched input '<EOF>'. Expecting: '%', '*', '+', '-', '.', '/', 'AND', 'AT', 'OR', 'THEN', '[', '||', <predicate>");
    }

    @Test
    public void testPossibleExponentialBacktracking2()
    {
        testStatement("SELECT id FROM t WHERE\n" +
                        "(f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "OR (f()\n" +
                        "GROUP BY id",
                "line 24:1: mismatched input 'GROUP'. Expecting: '%', ')', '*', '+', ',', '-', '.', '/', 'AND', 'AT', 'FILTER', 'IGNORE', 'OR', 'OVER', 'RESPECT', '[', '||', <predicate>");
    }

    @ParameterizedTest
    @MethodSource("statements")
    public void testStatement(String sql, String error)
    {
        assertThatThrownBy(() -> SQL_PARSER.createStatement(sql, PARSING_OPTIONS))
                .isInstanceOf(ParsingException.class)
                .hasMessage(error);
    }

    @ParameterizedTest
    @MethodSource("expressions")
    public void testExpression(String sql, String error)
    {
        assertThatThrownBy(() -> SQL_PARSER.createExpression(sql, PARSING_OPTIONS))
                .isInstanceOf(ParsingException.class)
                .hasMessage(error);
    }

    @Test
    public void testParsingExceptionPositionInfo()
    {
        assertThatThrownBy(() -> SQL_PARSER.createStatement("select *\nfrom x\nwhere from", PARSING_OPTIONS))
                .isInstanceOfSatisfying(ParsingException.class, e -> {
                    assertTrue(e.getMessage().startsWith("line 3:7: mismatched input 'from'"));
                    assertTrue(e.getErrorMessage().startsWith("mismatched input 'from'"));
                    assertEquals(3, e.getLineNumber());
                    assertEquals(7, e.getColumnNumber());
                });
    }

    @Test
    public void testStackOverflowExpression()
    {
        assertThatThrownBy(
                () -> {
                    for (int size = 3000; size <= 100_000; size *= 2) {
                        String expression = "x = y";
                        for (int i = 1; i < size; i++) {
                            expression = "(" + expression + ") OR x = y";
                        }
                        SQL_PARSER.createExpression(expression, new ParsingOptions());
                    }
                })
                .hasMessageContaining("line 1:1: expression is too large (stack overflow while parsing)");
    }

    @Test
    public void testStackOverflowStatement()
    {
        assertThatThrownBy(
                () -> {
                    for (int size = 6000; size <= 100_000; size *= 2) {
                        String expression = "x = y";
                        for (int i = 1; i < size; i++) {
                            expression = "(" + expression + ") OR x = y";
                        }
                        SQL_PARSER.createStatement("SELECT " + expression, PARSING_OPTIONS);
                    }
                })
                .hasMessageContaining("line 1:1: statement is too large (stack overflow while parsing)");
    }
}
