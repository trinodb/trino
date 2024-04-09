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
package io.trino.sql.query;

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.PATH_EVALUATION_ERROR;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonTable
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimple()
    {
        assertThat(assertions.query("""
                 SELECT first, last
                 FROM (SELECT '{"a" : [1, 2, 3], "b" : [4, 5, 6]}') t(json_col), JSON_TABLE(
                     json_col,
                     'lax $.a'
                     COLUMNS(
                         first bigint PATH 'lax $[0]',
                         last bigint PATH 'lax $[last]'))
                """))
                .matches("VALUES (BIGINT '1', BIGINT '3')");

        assertThat(assertions.query("""
                 SELECT *
                 FROM
                     (SELECT '{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}') t(json_col),
                     JSON_TABLE(
                         json_col,
                         'lax $.a' AS "path_a"
                         COLUMNS(
                             NESTED PATH 'lax $.b[*]' AS "path_b"
                                     COLUMNS (c1 integer PATH 'lax $ * 10'),
                             NESTED PATH 'lax $.c' AS "path_c"
                                     COLUMNS (
                                         NESTED PATH 'lax $[0][*]' AS "path_d" COLUMNS (c2 integer PATH 'lax $ * 100'),
                                         NESTED PATH 'lax $[last][*]' AS "path_e" COLUMNS (c3 integer PATH 'lax $ * 1000')))
                         PLAN ("path_a" OUTER ("path_b" UNION ("path_c" INNER ("path_d" CROSS "path_e")))))
                """))
                .matches("""
                        VALUES
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', 10, CAST(null AS integer), CAST(null AS integer)),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', 20, null, null),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', 30, null, null),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 400, 7000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 400, 8000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 400, 9000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 500, 7000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 500, 8000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 500, 9000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 600, 7000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 600, 8000),
                            ('{"a" : {"b" : [1, 2, 3], "c" : [[4, 5, 6], [7, 8, 9]]}}', null, 600, 9000)
                        """);
    }

    @Test
    public void testSubqueries()
    {
        // test subqueries in: context item, value of path parameter "index", empty default, error default
        assertThat(assertions.query("""
                 SELECT empty_default, error_default
                 FROM (SELECT '[[1, 2, 3], [4, 5, 6]]') t(json_col), JSON_TABLE(
                     (SELECT json_col),
                     'lax $[$index]' PASSING (SELECT 0) AS "index"
                     COLUMNS(
                         empty_default bigint PATH 'lax $[-42]' DEFAULT (SELECT -42) ON EMPTY,
                         error_default bigint PATH 'strict $[42]' DEFAULT (SELECT 42) ON ERROR))
                """))
                .matches("VALUES (BIGINT '-42', BIGINT '42')");
    }

    @Test
    public void testCorrelation()
    {
        // test correlation in: context item, value of path parameter "index", empty default, error default
        assertThat(assertions.query("""
                 SELECT empty_default, error_default
                 FROM (SELECT '[[1, 2, 3], [4, 5, 6]]', 0, -42, 42) t(json_col, index_col, empty_default_col, error_default_col),
                 JSON_TABLE(
                     json_col,
                     'lax $[$index]' PASSING index_col AS "index"
                     COLUMNS(
                         empty_default bigint PATH 'lax $[-42]' DEFAULT empty_default_col ON EMPTY,
                         error_default bigint PATH 'strict $[42]' DEFAULT error_default_col ON ERROR))
                """))
                .matches("VALUES (BIGINT '-42', BIGINT '42')");
    }

    @Test
    public void testParameters()
    {
        // test parameters in: context item, value of path parameter "index", empty default, error default
        Session session = Session.builder(assertions.getDefaultSession())
                .addPreparedStatement(
                        "my_query",
                        """
                                SELECT empty_default, error_default
                                FROM JSON_TABLE(
                                    ?,
                                    'lax $[$index]' PASSING ? AS "index"
                                    COLUMNS(
                                        empty_default bigint PATH 'lax $[-42]' DEFAULT ? ON EMPTY,
                                        error_default bigint PATH 'strict $[42]' DEFAULT ? ON ERROR))
                                """)
                .build();
        assertThat(assertions.query(session, "EXECUTE my_query USING '[[1, 2, 3], [4, 5, 6]]', 0, -42, 42"))
                .matches("VALUES (BIGINT '-42', BIGINT '42')");
    }

    @Test
    public void testOutputLayout()
    {
        // first the columns from the left side of the join (json_col, index_col, empty_default_col, error_default_col), next the json_table columns (empty_default, error_default)
        assertThat(assertions.query("""
                 SELECT *
                 FROM (SELECT '[[1, 2, 3], [4, 5, 6]]', 0, -42, 42) t(json_col, index_col, empty_default_col, error_default_col),
                 JSON_TABLE(
                     json_col,
                     'lax $[$index]' PASSING index_col AS "index"
                     COLUMNS(
                         empty_default bigint PATH 'lax $[-42]' DEFAULT empty_default_col * 2 ON EMPTY,
                         error_default bigint PATH 'strict $[42]' DEFAULT error_default_col * 2 ON ERROR))
                """))
                .matches("VALUES ('[[1, 2, 3], [4, 5, 6]]', 0, -42, 42, BIGINT '-84', BIGINT '84')");

        // json_table columns in order of declaration
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[]',
                        'lax $' AS "p"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $' AS "p1"
                                    COLUMNS (
                                        b varchar(1) PATH 'lax "B"',
                                        NESTED PATH 'lax $' AS "p2 "COLUMNS (
                                                                c varchar(1) PATH 'lax "C"',
                                                                d varchar(1) PATH 'lax "D"'),
                                        e varchar(1) PATH 'lax "E"'),
                            f varchar(1) PATH 'lax "F"',
                            NESTED PATH 'lax $' AS "p3"
                                     COLUMNS (g varchar(1) PATH 'lax "G"'),
                            h varchar(1) PATH 'lax "H"')
                        PLAN DEFAULT (CROSS))
                """))
                .matches("VALUES ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H')");
    }

    @Test
    public void testJoinTypes()
    {
        // implicit CROSS join
        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES ('[1, 2, 3]'), ('[4, 5, 6, 7, 8]')) t(json_col),
                 JSON_TABLE(
                     json_col,
                     'lax $[4]'
                     COLUMNS(a integer PATH 'lax $'))
                """))
                .matches("VALUES ('[4, 5, 6, 7, 8]', 8)");

        // INNER join
        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES ('[1, 2, 3]'), ('[4, 5, 6, 7, 8]')) t(json_col)
                 INNER JOIN
                 JSON_TABLE(
                     json_col,
                     'lax $[4]'
                     COLUMNS(a integer PATH 'lax $'))
                ON TRUE
                """))
                .matches("VALUES ('[4, 5, 6, 7, 8]', 8)");

        // LEFT join
        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES ('[1, 2, 3]'), ('[4, 5, 6, 7, 8]')) t(json_col)
                 LEFT JOIN
                 JSON_TABLE(
                     json_col,
                     'lax $[4]'
                     COLUMNS(a integer PATH 'lax $'))
                ON TRUE
                """))
                .matches("""
                        VALUES
                            ('[1, 2, 3]', CAST(null AS integer)),
                            ('[4, 5, 6, 7, 8]', 8)
                        """);

        // RIGHT join is effectively INNER. Correlation is not allowed in RIGHT join
        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES 1) t(x)
                 RIGHT JOIN
                 JSON_TABLE(
                     '[1, 2, 3]',
                     'lax $[4]'
                     COLUMNS(a integer PATH 'lax $'))
                ON TRUE
                """))
                .returnsEmptyResult();

        // FULL join. Correlation is not allowed in FULL join
        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES 1) t(x)
                 FULL JOIN
                 JSON_TABLE(
                     '[1, 2, 3]',
                     'lax $[4]'
                     COLUMNS(a integer PATH 'lax $'))
                ON TRUE
                """))
                .matches("VALUES (1, CAST(null AS integer))");
    }

    @Test
    public void testParentChildRelationship()
    {
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path"
                                    COLUMNS (b varchar(1) PATH 'lax "B"'))
                        PLAN ("root_path" OUTER "nested_path"))
                """))
                .matches("VALUES ('A', CAST(null AS varchar(1)))");

        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path"
                                    COLUMNS (b varchar(1) PATH 'lax "B"'))
                        PLAN ("root_path" INNER "nested_path"))
                """))
                .returnsEmptyResult();

        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[[], [1]]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path_1"
                                    COLUMNS (
                                        b varchar(1) PATH 'lax "B"',
                                        NESTED PATH 'lax $[*]' AS "nested_path_2"
                                                COLUMNS(
                                                    c varchar(1) PATH 'lax "C"')))
                        PLAN ("root_path" OUTER ("nested_path_1" OUTER "nested_path_2")))
                """))
                .matches("""
                        VALUES
                            ('A', 'B', CAST(null AS varchar(1))),
                            ('A', 'B', 'C')
                        """);

        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[[], [1]]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path_1"
                                    COLUMNS (
                                        b varchar(1) PATH 'lax "B"',
                                        NESTED PATH 'lax $[*]' AS "nested_path_2"
                                                COLUMNS(
                                                    c varchar(1) PATH 'lax "C"')))
                        PLAN ("root_path" OUTER ("nested_path_1" INNER "nested_path_2")))
                """))
                .matches("VALUES ('A', 'B', 'C')");

        // intermediately nested path returns empty sequence
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path_1"
                                    COLUMNS (
                                        b varchar(1) PATH 'lax "B"',
                                        NESTED PATH 'lax $' AS "nested_path_2"
                                                COLUMNS(
                                                    c varchar(1) PATH 'lax "C"')))
                        PLAN ("root_path" OUTER ("nested_path_1" INNER "nested_path_2")))
                """))
                .matches("VALUES ('A', CAST(null AS varchar(1)), CAST(null AS varchar(1)))");
    }

    @Test
    public void testSiblingsRelationship()
    {
        // each sibling produces 1 row
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $' AS "nested_path_b"
                                    COLUMNS (b varchar(1) PATH 'lax "B"'),
                            NESTED PATH 'lax $' AS "nested_path_c"
                                    COLUMNS (c varchar(1) PATH 'lax "C"'),
                            NESTED PATH 'lax $' AS "nested_path_d"
                                    COLUMNS (d varchar(1) PATH 'lax "D"'))
                        PLAN ("root_path" INNER ("nested_path_c" UNION ("nested_path_d" CROSS "nested_path_b"))))
                """))
                .matches("""
                        VALUES
                            ('A', CAST(null AS varchar(1)), 'C', CAST(null AS varchar(1))),
                            ('A', 'B', CAST(null AS varchar(1)), 'D')
                        """);

        // each sibling produces 2 rows
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[10, 1000]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path_1"
                                    COLUMNS (b integer PATH 'lax $ * 1'),
                            NESTED PATH 'lax $[*]' AS "nested_path_2"
                                    COLUMNS (c integer PATH 'lax $ * 2'),
                            NESTED PATH 'lax $[*]' AS "nested_path_3"
                                    COLUMNS (d integer PATH 'lax $ * 3'))
                        PLAN ("root_path" INNER ("nested_path_2" UNION ("nested_path_3" CROSS "nested_path_1"))))
                """))
                .matches("""
                        VALUES
                            ('A', CAST(null AS integer),    20,     CAST(null AS integer)),
                            ('A', null,                     2000,   null),
                            ('A', 10,                       null,   30),
                            ('A', 10,                       null,   3000),
                            ('A', 1000,                     null,   30),
                            ('A', 1000,                     null,   3000)
                        """);

        // one sibling produces empty result -- CROSS result is empty
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[10, 1000]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path_1"
                                    COLUMNS (b integer PATH 'lax $ * 1'),
                            NESTED PATH 'lax $[42]' AS "nested_path_2"
                                    COLUMNS (c integer PATH 'lax $ * 2'))
                        PLAN ("root_path" INNER ("nested_path_1" CROSS "nested_path_2")))
                """))
                .returnsEmptyResult();

        // one sibling produces empty result -- UNION result contains the other sibling's result
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[10, 1000]',
                        'lax $' AS "root_path"
                        COLUMNS(
                            a varchar(1) PATH 'lax "A"',
                            NESTED PATH 'lax $[*]' AS "nested_path_1"
                                    COLUMNS (b integer PATH 'lax $ * 1'),
                            NESTED PATH 'lax $[42]' AS "nested_path_2"
                                    COLUMNS (c integer PATH 'lax $ * 2'))
                        PLAN ("root_path" INNER ("nested_path_1" UNION "nested_path_2")))
                """))
                .matches("""
                        VALUES
                            ('A', 10,   CAST(null AS integer)),
                            ('A', 1000, null)
                        """);
    }

    @Test
    public void testImplicitColumnPath()
    {
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '{"A" : 42, "b" : true}',
                     'lax $'
                     COLUMNS(
                        a integer,
                        "b" boolean))
                """))
                .matches("VALUES (42, true)");

        // the implicit column path is 'lax $.C'. It produces empty sequence, so the ON EMPTY clause determines the result
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '{"A" : 42, "b" : true}',
                     'lax $'
                     COLUMNS(c varchar (5) DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR))
                """))
                .matches("VALUES 'empty'");
    }

    @Test
    public void testRootPathErrorHandling()
    {
        // error during root path evaluation handled according to top level EMPTY ON ERROR clause
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'strict $[42]'
                     COLUMNS(a integer PATH 'lax 1')
                     EMPTY ON ERROR)
                """))
                .returnsEmptyResult();

        // error during root path evaluation handled according to top level ON ERROR clause which defaults to EMPTY ON ERROR
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'strict $[42]'
                     COLUMNS(a integer PATH 'lax 1'))
                """))
                .returnsEmptyResult();

        // error during root path evaluation handled according to top level ERROR ON ERROR clause
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'strict $[42]'
                     COLUMNS(a integer PATH 'lax 1')
                     ERROR ON ERROR)
                """))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: structural error: invalid array subscript for empty array");
    }

    @Test
    public void testNestedPathErrorHandling()
    {
        // error during nested path evaluation handled according to top level EMPTY ON ERROR clause
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'lax $' AS "root_path"
                     COLUMNS(
                        a integer PATH 'lax 1',
                        NESTED PATH 'strict $[42]' AS "nested_path"
                            COLUMNS(b integer PATH 'lax 2'))
                     PLAN DEFAULT(INNER)
                     EMPTY ON ERROR)
                """))
                .returnsEmptyResult();

        // error during nested path evaluation handled according to top level ON ERROR clause which defaults to EMPTY ON ERROR
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'lax $' AS "root_path"
                     COLUMNS(
                        a integer PATH 'lax 1',
                        NESTED PATH 'strict $[42]' AS "nested_path"
                            COLUMNS(b integer PATH 'lax 2'))
                     PLAN DEFAULT(INNER))
                """))
                .returnsEmptyResult();

        // error during nested path evaluation handled according to top level ERROR ON ERROR clause
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'lax $' AS "root_path"
                     COLUMNS(
                        a integer PATH 'lax 1',
                        NESTED PATH 'strict $[42]' AS "nested_path"
                            COLUMNS(b integer PATH 'lax 2'))
                     PLAN DEFAULT(INNER)
                     ERROR ON ERROR)
                """))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: structural error: invalid array subscript for empty array");
    }

    @Test
    public void testColumnPathErrorHandling()
    {
        // error during column path evaluation handled according to column's ERROR ON ERROR clause
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'lax $'
                     COLUMNS(a integer PATH 'strict $[42]' ERROR ON ERROR)
                     EMPTY ON ERROR)
                """))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: structural error: invalid array subscript for empty array");

        // error during column path evaluation handled according to column's ON ERROR clause which defaults to NULL ON ERROR
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'lax $'
                     COLUMNS(a integer PATH 'strict $[42]')
                     EMPTY ON ERROR)
                """))
                .matches("VALUES CAST(null as integer)");

        // error during column path evaluation handled according to column's ON ERROR clause which defaults to ERROR ON ERROR because the top level error behavior is ERROR ON ERROR
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     '[]',
                     'lax $'
                     COLUMNS(a integer PATH 'strict $[42]')
                     ERROR ON ERROR)
                """))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: structural error: invalid array subscript for empty array");
    }

    @Test
    public void testEmptyInput()
    {
        assertThat(assertions.query("""
                 SELECT *
                 FROM (SELECT '[]' WHERE rand() > 1) t(json_col),
                 JSON_TABLE(
                     json_col,
                     'lax $'
                     COLUMNS(a integer PATH 'lax 1'))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testNullInput()
    {
        // if input is null, json_table returns empty result
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     CAST (null AS varchar),
                     'lax $'
                     COLUMNS(a integer PATH 'lax 1'))
                """))
                .returnsEmptyResult();

        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES (CAST(null AS varchar)), (CAST(null AS varchar)), (CAST(null AS varchar))) t(json_col),
                 JSON_TABLE(
                     json_col,
                     'lax $'
                     COLUMNS(a integer PATH 'lax 1'))
                """))
                .returnsEmptyResult();

        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES (CAST(null AS varchar)), (CAST(null AS varchar)), (CAST(null AS varchar))) t(json_col),
                 JSON_TABLE(
                     json_col,
                     'lax $'
                     COLUMNS(
                        NESTED PATH 'lax $'
                            COLUMNS(a integer PATH 'lax 1')))
                """))
                .returnsEmptyResult();

        // null as formatted input evaluates to empty sequence. json_table returns empty result
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                     CAST (null AS varchar) FORMAT JSON,
                     'lax $'
                     COLUMNS(a varchar FORMAT JSON PATH 'lax $'))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testNullPathParameter()
    {
        // null as SQL-value parameter "index" is evaluated to a JSON null, and causes type mismatch
        assertThat(assertions.query("""
                 SELECT *
                 FROM (SELECT '[1, 2, 3]', CAST(null AS integer)) t(json_col, index_col),
                 JSON_TABLE(
                     json_col,
                     'lax $[$index]' PASSING index_col AS "index"
                     COLUMNS(a integer PATH 'lax 1')
                     ERROR ON ERROR)
                """))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: invalid item type. Expected: NUMBER, actual: NULL");

        // null as JSON (formatted) parameter "index" evaluates to empty sequence, and causes type mismatch
        assertThat(assertions.query("""
                 SELECT *
                 FROM (SELECT '[1, 2, 3]', CAST(null AS varchar)) t(json_col, index_col),
                 JSON_TABLE(
                     json_col,
                     'lax $[$index]' PASSING index_col FORMAT JSON AS "index"
                     COLUMNS(a integer PATH 'lax 1')
                     ERROR ON ERROR)
                """))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: array subscript 'from' value must be singleton numeric");
    }

    @Test
    public void testNullDefaultValue()
    {
        assertThat(assertions.query("""
                 SELECT a
                 FROM (SELECT null) t(empty_default),
                 JSON_TABLE(
                     '[1, 2, 3]',
                     'lax $'
                     COLUMNS(a integer PATH 'lax $[42]' DEFAULT empty_default ON EMPTY DEFAULT -1 ON ERROR))
                """))
                .matches("VALUES CAST(null AS integer)");

        assertThat(assertions.query("""
                 SELECT a
                 FROM (SELECT null) t(error_default),
                 JSON_TABLE(
                     '[1, 2, 3]',
                     'lax $'
                     COLUMNS(a integer PATH 'strict $[42]' DEFAULT -1 ON EMPTY DEFAULT error_default ON ERROR))
                """))
                .matches("VALUES CAST(null AS integer)");
    }

    @Test
    public void testValueColumnCoercion()
    {
        // returned value cast to declared type
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[1, 2, 3]',
                        'lax $'
                        COLUMNS(a real PATH 'lax $[last]'))
                """))
                .matches("VALUES REAL '3'");

        // default value cast to declared type
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[1, 2, 3]',
                        'lax $'
                        COLUMNS(a real PATH 'lax $[42]' DEFAULT 42 ON EMPTY))
                """))
                .matches("VALUES REAL '42'");

        // default ON EMPTY value is null. It is cast to declared type
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[1, 2, 3]',
                        'lax $'
                        COLUMNS(a real PATH 'lax $[42]'))
                """))
                .matches("VALUES CAST(null AS REAL)");

        // default value cast to declared type
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[1, 2, 3]',
                        'lax $'
                        COLUMNS(a real PATH 'strict $[42]' DEFAULT 42 ON ERROR))
                """))
                .matches("VALUES REAL '42'");

        // default ON ERROR value is null. It is cast to declared type
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[1, 2, 3]',
                        'lax $'
                        COLUMNS(a real PATH 'strict $[42]'))
                """))
                .matches("VALUES CAST(null AS REAL)");
    }

    @Test
    public void testQueryColumnFormat()
    {
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[{"a" : true}]',
                        'lax $'
                        COLUMNS(a varchar(50) FORMAT JSON PATH 'lax $[0]'))
                """))
                .matches("VALUES CAST('{\"a\":true}' AS VARCHAR(50))");

        String varbinaryLiteral = "X'" + base16().encode("{\"a\":true}".getBytes(UTF_16LE)) + "'";
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[{"a" : true}]',
                        'lax $'
                        COLUMNS(a varbinary FORMAT JSON ENCODING UTF16 PATH 'lax $[0]'))
                """))
                .matches("VALUES " + varbinaryLiteral);

        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[{"a" : true}]',
                        'lax $'
                        COLUMNS(a char(50) FORMAT JSON PATH 'lax $[42]' EMPTY OBJECT ON EMPTY))
                """))
                .matches("VALUES CAST('{}' AS CHAR(50))");

        varbinaryLiteral = "X'" + base16().encode("[]".getBytes(UTF_16LE)) + "'";
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[{"a" : true}]',
                        'lax $'
                        COLUMNS(a varbinary FORMAT JSON ENCODING UTF16 PATH 'strict $[42]' EMPTY ARRAY ON ERROR))
                """))
                .matches("VALUES " + varbinaryLiteral);

        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '[{"a" : true}]',
                        'lax $'
                        COLUMNS(a varbinary FORMAT JSON ENCODING UTF16 PATH 'lax $[42]' NULL ON EMPTY))
                """))
                .matches("VALUES CAST(null AS VARBINARY)");
    }

    @Test
    public void testOrdinalityColumn()
    {
        assertThat(assertions.query("""
                 SELECT *
                 FROM JSON_TABLE(
                        '["a", "b", "c", "d", "e", "f", "g", "h"]',
                        'lax $[*]' AS "root_path"
                        COLUMNS(
                            o FOR ORDINALITY,
                            x varchar(1) PATH 'lax $'))
                """))
                .matches("""
                        VALUES
                            (BIGINT '1', 'a'),
                                    (2,  'b'),
                                    (3,  'c'),
                                    (4,  'd'),
                                    (5,  'e'),
                                    (6,  'f'),
                                    (7,  'g'),
                                    (8,  'h')
                        """);

        assertThat(assertions.query("""
                 SELECT *
                 FROM (VALUES
                        ('[["a", "b"], ["c", "d"], ["e", "f"]]'),
                        ('[["g", "h"], ["i", "j"], ["k", "l"]]')) t(json_col),
                 JSON_TABLE(
                        json_col,
                        'lax $' AS "root_path"
                        COLUMNS(
                            o FOR ORDINALITY,
                            NESTED PATH 'lax $[0][*]' AS "nested_path_1"
                                    COLUMNS (
                                        x1 varchar PATH 'lax $',
                                        o1 FOR ORDINALITY),
                            NESTED PATH 'lax $[1][*]' AS "nested_path_2"
                                    COLUMNS (
                                        x2 varchar PATH 'lax $',
                                        o2 FOR ORDINALITY),
                            NESTED PATH 'lax $[2][*]' AS "nested_path_3"
                                    COLUMNS (
                                        x3 varchar PATH 'lax $',
                                        o3 FOR ORDINALITY))
                        PLAN ("root_path" INNER ("nested_path_2" UNION ("nested_path_3" CROSS "nested_path_1"))))
                """))
                .matches("""
                        VALUES
                            ('[["a", "b"], ["c", "d"], ["e", "f"]]', BIGINT '1', VARCHAR 'a', BIGINT '1', CAST(null AS varchar), CAST(null AS bigint), VARCHAR 'e', BIGINT '1'),
                            ('[["a", "b"], ["c", "d"], ["e", "f"]]',         1,          'a',         1,                   null,                 null,         'f',         2),
                            ('[["a", "b"], ["c", "d"], ["e", "f"]]',         1,          'b',         2,                   null,                 null,         'e',         1),
                            ('[["a", "b"], ["c", "d"], ["e", "f"]]',         1,          'b',         2,                   null,                 null,         'f',         2),
                            ('[["a", "b"], ["c", "d"], ["e", "f"]]',         1,         null,      null,                    'c',                    1,        null,      null),
                            ('[["a", "b"], ["c", "d"], ["e", "f"]]',         1,         null,      null,                    'd',                    2,        null,      null),

                            ('[["g", "h"], ["i", "j"], ["k", "l"]]',         1,  VARCHAR 'g', BIGINT '1', CAST(null AS varchar), CAST(null AS bigint), VARCHAR 'k', BIGINT '1'),
                            ('[["g", "h"], ["i", "j"], ["k", "l"]]',         1,          'g',         1,                   null,                 null,         'l',         2),
                            ('[["g", "h"], ["i", "j"], ["k", "l"]]',         1,          'h',         2,                   null,                 null,         'k',         1),
                            ('[["g", "h"], ["i", "j"], ["k", "l"]]',         1,          'h',         2,                   null,                 null,         'l',         2),
                            ('[["g", "h"], ["i", "j"], ["k", "l"]]',         1,         null,      null,                    'i',                    1,        null,      null),
                            ('[["g", "h"], ["i", "j"], ["k", "l"]]',         1,         null,      null,                    'j',                    2,        null,      null)
                        """);
    }
}
