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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOrderedAggregation
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
    public void testAggregationWithOrderBy()
    {
        assertThat(assertions.query(
                "SELECT sum(x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)"))
                .matches("VALUES (BIGINT '8')");
        assertThat(assertions.query(
                "SELECT array_agg(x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)"))
                .matches("VALUES ARRAY[4, 1, 3]");

        assertThat(assertions.query(
                "SELECT array_agg(x ORDER BY y DESC) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)"))
                .matches("VALUES ARRAY[3, 1, 4]");

        assertThat(assertions.query(
                "SELECT array_agg(x ORDER BY x DESC) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)"))
                .matches("VALUES ARRAY[4, 3, 1]");

        assertThat(assertions.query(
                "SELECT array_agg(x ORDER BY x) FROM (VALUES ('a', 2), ('bcd', 5), ('abcd', 1)) t(x, y)"))
                .matches("VALUES ARRAY['a', 'abcd', 'bcd']");

        assertThat(assertions.query(
                "SELECT array_agg(y ORDER BY x) FROM (VALUES ('a', 2), ('bcd', 5), ('abcd', 1)) t(x, y)"))
                .matches("VALUES ARRAY[2, 1, 5]");

        assertThat(assertions.query(
                "SELECT array_agg(y ORDER BY x) FROM (VALUES ((1, 2), 2), ((3, 4), 5), ((1, 1), 1)) t(x, y)"))
                .matches("VALUES ARRAY[1, 2, 5]");

        assertThat(assertions.query(
                "SELECT array_agg(z ORDER BY x, y DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (1, 1, 1)) t(x, y, z)"))
                .matches("VALUES ARRAY[2, 1, 5, 3, 4]");

        assertThat(assertions.query(
                "SELECT x, array_agg(z ORDER BY y + z DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (3, 2, 1), (1, 1, 1)) t(x, y, z) GROUP BY x"))
                .matches("VALUES (1, ARRAY[2, 1]), (2, ARRAY[5, 3]), (3, ARRAY[4, 1])");

        assertThat(assertions.query(
                "SELECT array_agg(y ORDER BY x.a DESC) FROM (VALUES (CAST(ROW(1) AS ROW(a BIGINT)), 1), (CAST(ROW(2) AS ROW(a BIGINT)), 2)) t(x, y)"))
                .matches("VALUES ARRAY[2, 1]");

        assertThat(assertions.query(
                "SELECT x, y, array_agg(z ORDER BY z DESC NULLS FIRST) FROM (VALUES (1, 2, NULL), (1, 2, 1), (1, 2, 2), (2, 1, 3), (2, 1, 4), (2, 1, NULL)) t(x, y, z) GROUP BY x, y"))
                .matches("VALUES (1, 2, ARRAY[NULL, 2, 1]), (2, 1, ARRAY[NULL, 4, 3])");

        assertThat(assertions.query(
                "SELECT x, y, array_agg(z ORDER BY z DESC NULLS LAST) FROM (VALUES (1, 2, 3), (1, 2, 1), (1, 2, 2), (2, 1, 3), (2, 1, 4), (2, 1, NULL)) t(x, y, z) GROUP BY GROUPING SETS ((x), (x, y))"))
                .matches("VALUES (1, 2, ARRAY[3, 2, 1]), (1, NULL, ARRAY[3, 2, 1]), (2, 1, ARRAY[4, 3, NULL]), (2, NULL, ARRAY[4, 3, NULL])");

        assertThat(assertions.query(
                "SELECT x, y, array_agg(z ORDER BY z DESC NULLS LAST) FROM (VALUES (1, 2, 3), (1, 2, 1), (1, 2, 2), (2, 1, 3), (2, 1, 4), (2, 1, NULL)) t(x, y, z) GROUP BY GROUPING SETS ((x), (x, y))"))
                .matches("VALUES (1, 2, ARRAY[3, 2, 1]), (1, NULL, ARRAY[3, 2, 1]), (2, 1, ARRAY[4, 3, NULL]), (2, NULL, ARRAY[4, 3, NULL])");

        assertThat(assertions.query(
                "SELECT x, array_agg(DISTINCT z + y ORDER BY z + y DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (3, 2, 1), (1, 1, 1)) t(x, y, z) GROUP BY x"))
                .matches("VALUES (1, ARRAY[4, 2]), (2, ARRAY[9, 5]), (3, ARRAY[8, 3])");

        assertThat(assertions.query(
                "SELECT x, sum(cast(x AS double))\n" +
                        "FROM (VALUES '1.0') t(x)\n" +
                        "GROUP BY x\n" +
                        "ORDER BY sum(cast(t.x AS double) ORDER BY t.x)"))
                .matches("VALUES ('1.0', 1e0)");

        assertThat(assertions.query(
                "SELECT x, y, array_agg(z ORDER BY z) FROM (VALUES (1, 2, 3), (1, 2, 1), (2, 1, 3), (2, 1, 4)) t(x, y, z) GROUP BY GROUPING SETS ((x), (x, y))"))
                .matches("VALUES (1, NULL, ARRAY[1, 3]), (2, NULL, ARRAY[3, 4]), (1, 2, ARRAY[1, 3]), (2, 1, ARRAY[3, 4])");

        assertThatThrownBy(() -> assertions.query(
                "SELECT array_agg(z ORDER BY z) OVER (PARTITION BY x) FROM (VALUES (1, 2, 3), (1, 2, 1), (2, 1, 3), (2, 1, 4)) t(x, y, z) GROUP BY x, z"))
                .hasMessageMatching(".* Window function with ORDER BY is not supported");

        assertThatThrownBy(() -> assertions.query(
                "SELECT array_agg(DISTINCT x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)"))
                .hasMessageMatching(".* For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");

        assertThatThrownBy(() -> assertions.query(
                "SELECT array_agg(DISTINCT x+y ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)"))
                .hasMessageMatching(".* For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");

        assertThatThrownBy(() -> assertions.query(
                "SELECT x, array_agg(DISTINCT y ORDER BY z + y DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (3, 2, 1), (1, 1, 1)) t(x, y, z) GROUP BY x"))
                .hasMessageMatching(".* For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");

        assertThat(assertions.query(
                "SELECT multimap_agg(x, y ORDER BY z) FROM (VALUES (1, 2, 2), (1, 5, 5), (2, 1, 5), (3, 4, 4), (2, 5, 1), (1, 1, 1)) t(x, y, z)"))
                .matches("VALUES map_from_entries(ARRAY[row(1, ARRAY[1, 2, 5]), row(2, ARRAY[5, 1]), row(3, ARRAY[4])])");
    }

    @Test
    public void testGroupingSets()
    {
        assertThat(assertions.query(
                "SELECT x, array_agg(y ORDER BY y), array_agg(y ORDER BY y) FILTER (WHERE y > 1), count(*) FROM (" +
                        "VALUES " +
                        "   (1, 3), " +
                        "   (1, 1), " +
                        "   (2, 3), " +
                        "   (2, 4)) t(x, y) " +
                        "GROUP BY GROUPING SETS ((), (x))"))
                .matches("VALUES " +
                        "   (1, ARRAY[1, 3], ARRAY[3], BIGINT '2'), " +
                        "   (2, ARRAY[3, 4], ARRAY[3, 4], BIGINT '2'), " +
                        "   (NULL, ARRAY[1, 3, 3, 4], ARRAY[3, 3, 4], BIGINT '4')");

        assertThat(assertions.query(
                "SELECT x, array_agg(DISTINCT y ORDER BY y), count(*) FROM (" +
                        "VALUES " +
                        "   (1, 3), " +
                        "   (1, 1), " +
                        "   (1, 3), " +
                        "   (2, 3), " +
                        "   (2, 4)) t(x, y) " +
                        "GROUP BY GROUPING SETS ((), (x))"))
                .matches("VALUES " +
                        "   (1, ARRAY[1, 3], BIGINT '3'), " +
                        "   (2, ARRAY[3, 4], BIGINT '2'), " +
                        "   (NULL, ARRAY[1, 3, 4], BIGINT '5')");
    }

    @Test
    public void testRepeatedSortItems()
    {
        assertThat(assertions.query("SELECT count(x ORDER BY y, y) FROM (VALUES ('a', 2)) t(x, y)"))
                .matches("VALUES BIGINT '1'");
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
                "        array_agg( v ORDER BY t DESC) filter (WHERE v IS NOT NULL) AS v " +
                "    FROM ( " +
                "        ( " +
                "            SELECT 'filtered' AS id, cast('value' AS varchar) AS v, 'sort' AS t " +
                "            FROM (VALUES 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) " +
                "        ) " +
                "        UNION ALL " +
                "        ( " +
                "            SELECT cast(uuid() AS varchar) AS id, cast(null AS varchar) AS v, 'sort' AS t " +
                "            FROM UNNEST(combinations(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 5)) " +
                "        ) " +
                "    ) " +
                "    GROUP BY id " +
                ")"))
                .matches("VALUES (TRUE, BIGINT '10')");
    }
}
