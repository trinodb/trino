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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDistinctAggregations
{
    protected QueryAssertions assertions;

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
    public void testGroupAllSingleDistinct()
    {
        assertThat(assertions.query(
                "SELECT count(DISTINCT x) FROM " +
                        "(VALUES 1, 1, 2, 3) t(x)"))
                .matches("VALUES BIGINT '3'");

        assertThat(assertions.query(
                "SELECT count(DISTINCT x), sum(DISTINCT x) FROM " +
                        "(VALUES 1, 1, 2, 3) t(x)"))
                .matches("VALUES (BIGINT '3', BIGINT '6')");
    }

    @Test
    public void testGroupBySingleDistinct()
    {
        assertThat(assertions.query(
                "SELECT k, count(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1), " +
                        "   (1, 1), " +
                        "   (1, 2)," +
                        "   (1, 3)," +
                        "   (2, 1), " +
                        "   (2, 10), " +
                        "   (2, 10)," +
                        "   (2, 20)," +
                        "   (2, 30)" +
                        ") t(k, x) " +
                        "GROUP BY k"))
                .matches("VALUES " +
                        "(1, BIGINT '3'), " +
                        "(2, BIGINT '4')");

        assertThat(assertions.query(
                "SELECT k, count(DISTINCT x), sum(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1), " +
                        "   (1, 1), " +
                        "   (1, 2)," +
                        "   (1, 3)," +
                        "   (2, 1), " +
                        "   (2, 10), " +
                        "   (2, 10)," +
                        "   (2, 20)," +
                        "   (2, 30)" +
                        ") t(k, x) " +
                        "GROUP BY k"))
                .matches("VALUES " +
                        "(1, BIGINT '3', BIGINT '6'), " +
                        "(2, BIGINT '4', BIGINT '61')");
    }

    @Test
    public void testGroupingSetsSingleDistinct()
    {
        assertThat(assertions.query(
                "SELECT k, count(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1), " +
                        "   (1, 1), " +
                        "   (1, 2)," +
                        "   (1, 3)," +
                        "   (2, 1), " +
                        "   (2, 10), " +
                        "   (2, 10)," +
                        "   (2, 20)," +
                        "   (2, 30)" +
                        ") t(k, x) " +
                        "GROUP BY GROUPING SETS ((), (k))"))
                .matches("VALUES " +
                        "(1, BIGINT '3'), " +
                        "(2, BIGINT '4'), " +
                        "(CAST(NULL AS INTEGER), BIGINT '6')");

        assertThat(assertions.query(
                "SELECT k, count(DISTINCT x), sum(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1), " +
                        "   (1, 1), " +
                        "   (1, 2)," +
                        "   (1, 3)," +
                        "   (2, 1), " +
                        "   (2, 10), " +
                        "   (2, 10)," +
                        "   (2, 20)," +
                        "   (2, 30)" +
                        ") t(k, x) " +
                        "GROUP BY GROUPING SETS ((), (k))"))
                .matches("VALUES " +
                        "(1, BIGINT '3', BIGINT '6'), " +
                        "(2, BIGINT '4', BIGINT '61'), " +
                        "(CAST(NULL AS INTEGER), BIGINT '6', BIGINT '66')");
    }

    @Test
    public void testGroupAllMixedDistinct()
    {
        assertThat(assertions.query(
                "SELECT count(DISTINCT x), count(*) FROM " +
                        "(VALUES 1, 1, 2, 3) t(x)"))
                .matches("VALUES (BIGINT '3', BIGINT '4')");

        assertThat(assertions.query(
                "SELECT count(DISTINCT x), count(DISTINCT y) FROM " +
                        "(VALUES " +
                        "   (1, 10), " +
                        "   (1, 20)," +
                        "   (1, 30)," +
                        "   (2, 30)) t(x, y)"))
                .matches("VALUES (BIGINT '2', BIGINT '3')");

        assertThat(assertions.query(
                "SELECT k, count(DISTINCT x), count(DISTINCT y) FROM " +
                        "(VALUES " +
                        "   (1, 1, 100), " +
                        "   (1, 1, 100), " +
                        "   (1, 2, 100)," +
                        "   (1, 3, 200)," +
                        "   (2, 1, 100), " +
                        "   (2, 10, 200), " +
                        "   (2, 10, 300)," +
                        "   (2, 20, 400)," +
                        "   (2, 30, 400)" +
                        ") t(k, x, y) " +
                        "GROUP BY GROUPING SETS ((), (k))"))
                .matches("VALUES " +
                        "(1, BIGINT '3', BIGINT '2'), " +
                        "(2, BIGINT '4', BIGINT '4'), " +
                        "(CAST(NULL AS INTEGER), BIGINT '6', BIGINT '4')");
    }

    @Test
    public void testMultipleInputs()
    {
        assertThat(assertions.query(
                "SELECT corr(DISTINCT x, y) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)"))
                .matches("VALUES (REAL '1.0')");

        assertThat(assertions.query(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)"))
                .matches("VALUES (REAL '1.0', REAL '1.0')");

        assertThat(assertions.query(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)"))
                .matches("VALUES (REAL '1.0', REAL '1.0', BIGINT '4')");

        assertThat(assertions.query(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)"))
                .matches("VALUES (REAL '1.0', REAL '1.0', BIGINT '3')");
    }

    @Test
    public void testMixedDistinctAndNonDistinct()
    {
        assertThat(assertions.query(
                "SELECT sum(DISTINCT x), sum(DISTINCT y), sum(z) FROM " +
                        "(VALUES " +
                        "   (1, 10, 100), " +
                        "   (1, 20, 200)," +
                        "   (2, 20, 300)," +
                        "   (3, 30, 300)) t(x, y, z)"))
                .matches("VALUES (BIGINT '6', BIGINT '60', BIGINT '900')");
    }

    @Test
    public void testMixedDistinctWithFilter()
    {
        assertThat(assertions.query(
                "SELECT " +
                        "     count(DISTINCT x) FILTER (WHERE x > 0), " +
                        "     sum(x) " +
                        "FROM (VALUES 0, 1, 1, 2) t(x)"))
                .matches("VALUES (BIGINT '2', BIGINT '4')");

        assertThat(assertions.query(
                "SELECT count(DISTINCT x) FILTER (where y = 1)" +
                        "FROM (VALUES (2, 1), (1, 2), (1,1)) t(x, y)"))
                .matches("VALUES (BIGINT '2')");

        assertThat(assertions.query(
                "SELECT " +
                        "     count(DISTINCT x), " +
                        "     sum(x) FILTER (WHERE x > 0) " +
                        "FROM (VALUES 0, 1, 1, 2) t(x)"))
                .matches("VALUES (BIGINT '3', BIGINT '4')");

        assertThat(assertions.query(
                "SELECT" +
                        "     sum(DISTINCT x) FILTER (WHERE y > 3)," +
                        "     sum(DISTINCT y) FILTER (WHERE x > 1)" +
                        "FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)"))
                .matches("VALUES (BIGINT '6', BIGINT '9')");

        assertThat(assertions.query(
                "SELECT" +
                        "     sum(x) FILTER (WHERE x > 1) AS x," +
                        "     sum(DISTINCT x)" +
                        "FROM (VALUES (1), (2), (2), (4)) t (x)"))
                .matches("VALUES (BIGINT '8', BIGINT '7')");

        // filter out all rows
        assertThat(assertions.query(
                "SELECT sum(DISTINCT x) FILTER (WHERE y > 5)" +
                        "FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)"))
                .matches("VALUES (CAST(NULL AS BIGINT))");
        assertThat(assertions.query(
                "SELECT" +
                        "     count(DISTINCT y) FILTER (WHERE x > 4)," +
                        "     sum(DISTINCT x) FILTER (WHERE y > 5)" +
                        "FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)"))
                .matches("VALUES (BIGINT '0', CAST(NULL AS BIGINT))");
    }

    @Test
    public void testUuidDistinct()
    {
        assertThat(assertions.query(
                "SELECT DISTINCT uuid_col " +
                        "FROM (VALUES (UUID'be0b0518-35a1-4d10-b7f1-1b61355fa741')," +
                        "             (UUID'be0b0518-35a1-4d10-b7f1-1b61355fa741')) AS t (uuid_col)"))
                .matches("VALUES UUID'be0b0518-35a1-4d10-b7f1-1b61355fa741'");
    }

    @Test
    public void testIpAddressDistinct()
    {
        assertThat(assertions.query(
                "SELECT DISTINCT ipaddress_col " +
                        "FROM (VALUES (IPADDRESS'2001:db8:0:0:1::1')," +
                        "             (IPADDRESS'2001:db8:0:0:1::1')) AS t (ipaddress_col)"))
                .matches("VALUES IPADDRESS'2001:db8:0:0:1::1'");
    }

    @Test
    public void testCompletelyFilteredGroup()
    {
        // This query filters out all values to a most groups, which results in an accumulator with no pages to sort.
        // This can cause a failure if the ordering code does not inform the accumulator of the max row group.
        assertThat(assertions.query("" +
                "SELECT count(id) > 15000, sum(cardinality(v)) " +
                "FROM ( " +
                "    SELECT " +
                "        id, " +
                "        array_agg(DISTINCT v) filter (WHERE v IS NOT NULL) AS v " +
                "    from ( " +
                "        ( " +
                "            SELECT 'filtered' AS id, cast('value' AS varchar) AS v " +
                "            FROM (VALUES 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) " +
                "        ) " +
                "        UNION ALL " +
                "        ( " +
                "            SELECT cast(uuid() AS varchar) AS id, cast(null AS varchar) AS v " +
                "            FROM UNNEST(combinations(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 5)) " +
                "        ) " +
                "    ) " +
                "    GROUP BY id " +
                ")"))
                .matches("VALUES (TRUE, BIGINT '1')");
    }
}
