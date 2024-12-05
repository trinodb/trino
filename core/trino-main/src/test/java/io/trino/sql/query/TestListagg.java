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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestListagg
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testListaggQueryWithOneValue()
    {
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a') t(value)"))
                .matches("VALUES (VARCHAR 'a')");
    }

    @Test
    public void testListaggQueryWithOneValueGrouping()
    {
        assertThat(assertions.query(
                "SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES (1, 'a')) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .matches("VALUES (1, VARCHAR 'a')");
    }

    @Test
    public void testListaggQueryWithMultipleValues()
    {
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) "))
                .matches("VALUES (VARCHAR 'a,bb,ccc,dddd')");
    }

    @Test
    public void testListaggQueryWithImplicitSeparator()
    {
        assertThat(assertions.query(
                "SELECT listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'b', 'c') t(value) "))
                .matches("VALUES (VARCHAR 'abc')");
    }

    @Test
    public void testListaggQueryWithImplicitSeparatorGrouping()
    {
        assertThat(assertions.query(
                "SELECT id, listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "         (1, 'c'), " +
                        "         (2, 'b'), " +
                        "         (1, 'a')," +
                        "         (2, 'd')" +
                        "     )  t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .matches("VALUES " +
                        "        (1, VARCHAR 'ac')," +
                        "        (2, VARCHAR 'bd')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesOrderedDescending()
    {
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value DESC) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) "))
                .matches("VALUES (VARCHAR 'dddd,ccc,bb,a')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesMultipleSortItems()
    {
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY sortitem1, sortitem2) " +
                        "FROM (VALUES (2, 'C', 'ccc'), (2, 'B', 'bb'), (3, 'D', 'dddd'), (1, 'A', 'a')) t(sortitem1, sortitem2, value) "))
                .matches("VALUES (VARCHAR 'a,bb,ccc,dddd')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesMultipleSortItemsGrouping()
    {
        assertThat(assertions.query(
                "SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY weight, label) " +
                        "FROM (VALUES (1, 200, 'C', 'ccc'), (1, 200, 'B', 'bb'), (2, 300, 'D', 'dddd'), (1, 100, 'A', 'a')) t(id, weight, label, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .matches("VALUES (1, VARCHAR 'a,bb,ccc')," +
                        "        (2, VARCHAR 'dddd')");
    }

    @Test
    public void testListaggQueryWithFunctionExpression()
    {
        assertThat(assertions.query(
                "SELECT listagg(upper(value), ' ') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'Trino', 'SQL', 'everything') t(value) "))
                .matches("VALUES (VARCHAR 'SQL TRINO EVERYTHING')");
    }

    @Test
    public void testListaggQueryWithNullValues()
    {
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', NULL, 'bb', NULL, 'ccc', NULL, 'dddd', NULL) t(value) "))
                .matches("VALUES (VARCHAR 'a,bb,ccc,dddd')");
    }

    @Test
    public void testListaggQueryWithNullValuesGrouping()
    {
        assertThat(assertions.query(
                "SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "             (1, 'a'), " +
                        "             (2, NULL), " +
                        "             (3, 'bb'), " +
                        "             (1, NULL), " +
                        "             (1, 'ccc'), " +
                        "             (2, NULL), " +
                        "             (3, 'dddd'), " +
                        "             (2, NULL)" +
                        "     ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .matches("VALUES (1, VARCHAR 'a,ccc')," +
                        "        (2, NULL)," +
                        "        (3, VARCHAR 'bb,dddd')");
    }

    @Test
    public void testListaggQueryIncorrectSyntax()
    {
        // missing WITHIN GROUP (ORDER BY ...)
        assertThat(assertions.query(
                "SELECT listagg(v, ',') " +
                        "FROM (VALUES 'a') t(v)"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:24: mismatched input 'FROM'. Expecting: 'WITHIN'");

        // missing WITHIN GROUP (ORDER BY ...)
        assertThat(assertions.query(
                "SELECT listagg(v) " +
                        "FROM (VALUES 'a') t(v)"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:19: mismatched input 'FROM'. Expecting: 'WITHIN'");

        // too many arguments
        assertThat(assertions.query(
                "SELECT listagg(v, ',', '...') WITHIN GROUP (ORDER BY v)" +
                        "FROM (VALUES 'a') t(v)"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:22: mismatched input ','. Expecting: ')', 'ON'");

        // invalid argument for ON OVERFLOW clause
        assertThat(assertions.query(
                "SELECT listagg(v, ',' ON OVERFLOW COLLAPSE) WITHIN GROUP (ORDER BY v)" +
                        "FROM (VALUES 'a') t(v)"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:35: mismatched input 'COLLAPSE'. Expecting: 'ERROR', 'TRUNCATE'");

        // invalid separator type (integer instead of varchar)
        assertThat(assertions.query(
                "SELECT LISTAGG(v, 123) WITHIN GROUP (ORDER BY v) " +
                        "FROM (VALUES 'Trino', 'SQL', 'everything') t(v) "))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:19: mismatched input '123'. Expecting: <string>");

        // invalid truncation filler type (integer instead of varchar)
        assertThat(assertions.query(
                "SELECT LISTAGG(v, ',' ON OVERFLOW TRUNCATE 1234567890 WITHOUT COUNT) WITHIN GROUP (ORDER BY v) " +
                        "FROM (VALUES 'Trino', 'SQL', 'everything') t(v) "))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:44: mismatched input '1234567890'. Expecting: 'WITH', 'WITHOUT', <string>");
    }

    @Test
    public void testListaggQueryIncorrectExpression()
    {
        // integer values
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 1, NULL, 2, 3, 4) t(value)"))
                .failure().hasMessage("line 1:8: Expected expression of varchar, but 'value' has integer type");

        // boolean values
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES TRUE, NULL, FALSE, FALSE, TRUE) t(value)"))
                .failure().hasMessage("line 1:8: Expected expression of varchar, but 'value' has boolean type");

        // array values
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES array['abc', 'def'], array['sql']) t(value)"))
                .failure().hasMessage("line 1:8: Expected expression of varchar, but 'value' has array(varchar(3)) type");
    }

    @Test
    public void testListaaggQueryIncorrectOrderByExpression()
    {
        assertThat(assertions.query(
                "SELECT listagg(label, ',') WITHIN GROUP (ORDER BY rgb) " +
                        "FROM (VALUES ('red', rgb(255, 0, 0)), ('green', rgb(0, 128, 0)), ('blue', rgb(0, 0, 255))) color(label, rgb) "))
                .failure().hasMessage("line 1:8: ORDER BY can only be applied to orderable types (actual: color)");
    }

    @Test
    public void testListaggQueryWithExplicitlyCastedNumericValues()
    {
        assertThat(assertions.query(
                "SELECT listagg(try_cast(value as varchar), ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 1, NULL, 2, 3, 4) t(value)"))
                .matches("VALUES (VARCHAR '1,2,3,4')");
    }

    @Test
    public void testListaggQueryWithDistinct()
    {
        assertThat(assertions.query(
                "SELECT listagg( DISTINCT value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES  'a', 'b', 'a', 'b', 'c', 'd', 'd', 'a', 'd', 'b', 'd') t(value)"))
                .matches("VALUES (VARCHAR 'a,b,c,d')");
    }

    @Test
    public void testListaggQueryWithDistinctGrouping()
    {
        assertThat(assertions.query(
                "SELECT id, listagg( DISTINCT value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES  " +
                        "          (1, 'a'), " +
                        "          (1, 'b'), " +
                        "          (1, 'a'), " +
                        "          (2, 'b'), " +
                        "          (1, 'c'), " +
                        "          (1, 'd'), " +
                        "          (2, 'd'), " +
                        "          (1, 'a'), " +
                        "          (2, 'd'), " +
                        "          (2, 'b'), " +
                        "          (1, 'd')" +
                        "    ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .matches("VALUES (1, VARCHAR 'a,b,c,d')," +
                        "        (2, VARCHAR 'b,d')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesWithDefaultSeparator()
    {
        assertThat(assertions.query(
                "SELECT listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) "))
                .matches("VALUES (VARCHAR 'abbcccdddd')");
    }

    @Test
    public void testListaggQueryWithOrderingAndGrouping()
    {
        assertThat(assertions.query("SELECT id, LISTAGG(value, ',') WITHIN GROUP (ORDER BY value) " +
                "          FROM (VALUES " +
                "                   (1, 'a'), " +
                "                   (1, 'b'), " +
                "                   (2, 'd'), " +
                "                   (2, 'c') " +
                "               ) t(id, value)" +
                "          GROUP BY id" +
                "          ORDER BY id"))
                .matches("VALUES     " +
                        "     (1, VARCHAR 'a,b')," +
                        "     (2, VARCHAR 'c,d')");
    }

    @Test
    public void testListaggQueryOverflowError()
    {
        assertThat(assertions.query(
                "SELECT LISTAGG(value, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES lpad('a', " + DEFAULT_MAX_PAGE_SIZE_IN_BYTES + ", 'a'),'Trino') t(value) "))
                .failure()
                .hasMessage("Concatenated string has the length in bytes larger than the maximum output length 1048576")
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT);
    }

    @Test
    public void testListaggQueryOverflowErrorGrouping()
    {
        assertThat(assertions.query(
                "SELECT id, LISTAGG(value, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "           (1, lpad('a', " + DEFAULT_MAX_PAGE_SIZE_IN_BYTES + ", 'a'))," +
                        "           (1, 'Trino')," +
                        "           (2, 'SQL')" +
                        "     ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .failure()
                .hasMessage("Concatenated string has the length in bytes larger than the maximum output length 1048576")
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT);
    }

    @Test
    public void testListaggQueryOverflowTruncateWithoutCountAndWithoutOverflowFiller()
    {
        int size = DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 10;
        assertThat(assertions.query(
                "SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES lpad('', " + size + ", 'a'), 'trino', 'rocks') t(value) "))
                .matches("VALUES (lpad('', " + size + ", 'a') || ',rocks,...')");
    }

    @Test
    public void testListaggQueryOverflowTruncateWithCountAndWithOverflowFiller()
    {
        int size = DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 12;
        assertThat(assertions.query(
                "SELECT substring(LISTAGG(value, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) WITHIN GROUP (ORDER BY value), -30) " +
                        "FROM (VALUES lpad('', " + size + ", 'a'), 'trino', 'sql', 'everything') t(value) "))
                .skippingTypesCheck()
                .matches("VALUES ('aaaaaaaaaa,everything,.....(2)')");
    }

    @Test
    public void testListaggQueryGroupingOverflowTruncateWithCountAndWithOverflowFiller()
    {
        int size = DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 12;
        assertThat(assertions.query(
                "SELECT id, substring(LISTAGG(value, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) WITHIN GROUP (ORDER BY value), if(id = 1, -30, 1)) " +
                        "FROM (VALUES " +
                        "             (1, lpad('', " + size + ", 'a')), " +
                        "             (1, 'trino'), " +
                        "             (1, 'sql'), " +
                        "             (1, 'everything'), " +
                        "             (2, 'listagg'), " +
                        "             (2, 'string joiner') " +
                        "     ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id "))
                .matches("VALUES " +
                        "   (1, VARCHAR 'aaaaaaaaaa,everything,.....(2)')," +
                        "   (2, VARCHAR 'listagg,string joiner')");
    }

    @Test
    void testFilter()
    {
        assertThat(assertions.query(
                """
                SELECT listagg(value, ',') WITHIN GROUP (ORDER BY id) FILTER (WHERE id % 2 = 0)
                FROM (
                     VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')
                ) t(id, value)
                """))
                .matches("VALUES VARCHAR 'b,d'");
    }

    @Test
    void testListaggWindow()
    {
        assertThat(assertions.query(
                """
                SELECT a, listagg(DISTINCT c) WITHIN GROUP (ORDER BY c DESC) OVER w
                FROM (
                    VALUES (1, 1, '1'), (1, 2, '2'), (1, 3, '1'), (1, 4, '4'), (1, 4, '2'), (1, 5, '5'),
                           (2, 1, '1'), (2, 2, '3'), (2, 3, '2'), (2, 4, '3')) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, CAST('1' AS VARCHAR)),
                            (1, '21'),
                            (1, '21'),
                            (1, '421'),
                            (1, '421'),
                            (1, '5421'),
                            (2, '1'),
                            (2, '31'),
                            (2, '321'),
                            (2, '321')
                        """);
    }
}
