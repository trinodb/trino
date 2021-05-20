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

import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestListagg
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
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
    public void testListaggQueryWithMultipleValues()
    {
        assertThat(assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) "))
                .matches("VALUES (VARCHAR 'a,bb,ccc,dddd')");
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
                        "FROM (VALUES (1, 'A', 'a') , (2, 'B', 'bb'), (2, 'C', 'ccc'), (3, 'D', 'dddd')) t(sortitem1, sortitem2, value) "))
                .matches("VALUES (VARCHAR 'a,bb,ccc,dddd')");
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
    public void testListaggQueryIncorrectSyntax()
    {
        // missing WITHIN GROUP (ORDER BY ...)
        assertThatThrownBy(() -> assertions.query(
                "SELECT listagg(value, ',') " +
                        "FROM (VALUES 'a') t(value)"))
                .isInstanceOf(ParsingException.class)
                .withFailMessage("line 1:8: incorrect syntax for 'listagg' function");

        // missing WITHIN GROUP (ORDER BY ...)
        assertThatThrownBy(() -> assertions.query(
                "SELECT listagg(value) " +
                        "FROM (VALUES 'a') t(value)"))
                .isInstanceOf(ParsingException.class)
                .withFailMessage("line 1:8: incorrect syntax for 'listagg' function");

        // too many arguments
        assertThatThrownBy(() -> assertions.query(
                "SELECT listagg(value, ',', '...') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 'a') t(value)"))
                .isInstanceOf(ParsingException.class)
                .withFailMessage("line 1:8: incorrect syntax for 'listagg' function");

        // window frames are not supported
        assertThatThrownBy(() -> assertions.query(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) OVER (PARTITION BY id)" +
                        "FROM (VALUES (1, 'a')) t(id, value)"))
                .isInstanceOf(ParsingException.class)
                .withFailMessage("line 1:8: incorrect syntax for 'listagg' function");

        // invalid argument for ON OVERFLOW clause
        assertThatThrownBy(() -> assertions.query(
                "SELECT listagg(value, ',' ON OVERFLOW COLLAPSE) WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 'a') t(value)"))
                .isInstanceOf(ParsingException.class)
                .withFailMessage("line 1:8: incorrect syntax for 'listagg' function");
    }

    @Test
    public void testListaggQueryWithNonVarcharValuesShouldFail()
    {
        assertThatThrownBy(() -> assertions.query(
                "SELECT listagg(value) WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES  1) t(value) "))
                .isInstanceOf(TrinoException.class)
                .matches(throwable -> ((TrinoException) throwable).getErrorCode() == FUNCTION_NOT_FOUND.toErrorCode());
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
    public void testListaggQueryWithMultipleValuesWithDefaultSeparator()
    {
        assertThat(assertions.query(
                "SELECT listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) "))
                .matches("VALUES (VARCHAR 'abbcccdddd')");
    }

    @Test
    public void testListaggQueryWithOrdering()
    {
        assertThat(assertions.query(
                "SELECT LISTAGG(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'ccc', 'dddd', 'bb') t(value) "))
                .matches("VALUES (VARCHAR 'a,bb,ccc,dddd')");
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
        String tooLargeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        assertThatThrownBy(() -> assertions.query(
                "SELECT LISTAGG(value, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES '" + tooLargeValue + "','Trino') t(value) "))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Concatenated string is too large")
                .matches(throwable -> ((TrinoException) throwable).getErrorCode() == QUERY_REJECTED.toErrorCode());
    }

    @Test
    public void testListaggQueryOverflowTruncateWithoutCountAndWithoutTruncationFiller()
    {
        String largeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 6);
        assertThat(assertions.query(
                "SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES '" + largeValue + "', 'trino', 'rocks') t(value) "))
                .matches("VALUES (VARCHAR '" + largeValue + ",ro...')");
    }

    @Test
    public void testListaggQueryOverflowTruncateWithCountAndWithTruncationFiller()
    {
        String largeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 12);
        assertThat(assertions.query(
                "SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES '" + largeValue + "', 'trino', 'sql', 'everything') t(value) "))
                .matches("VALUES (VARCHAR '" + largeValue + ",eve.....(2)')");
    }
}
