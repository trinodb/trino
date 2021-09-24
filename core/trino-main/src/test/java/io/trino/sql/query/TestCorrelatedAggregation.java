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
public class TestCorrelatedAggregation
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
    public void testGlobalDistinctAggregation()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(DISTINCT value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '2'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT avg(DISTINCT value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, 10e0), (2, 15e0), (3, 15e0)");

        // with FILTER
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(DISTINCT value) FILTER (WHERE value > 15) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '0'), (3, BIGINT '1')");

        // with ORDER BY
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT array_agg(DISTINCT value ORDER BY value) FROM (VALUES (1, 10), (2, 10), (3, 20)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, ARRAY[10]), (3, ARRAY[10])");

        // with projection
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(DISTINCT value) + 100 FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '101'), (2, BIGINT '102'), (3, BIGINT '102')");

        // null in subquery
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(DISTINCT value) FROM (VALUES (1, null), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");

        // empty lateral relation
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(DISTINCT value) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '0'), (3, BIGINT '0')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT avg(DISTINCT value) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, CAST(null AS double)), (2, null), (3, null)");

        // empty lateral relation for some input row
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(DISTINCT value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT avg(DISTINCT value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, 10e0), (3, 15e0)");

        // INNER join
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(DISTINCT value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");
    }

    @Test
    public void testGlobalAggregation()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '2'), (3, BIGINT '3')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(*) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '2'), (3, BIGINT '3')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT avg(value) FROM (VALUES (1, 10), (2, 20), (3, 30)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, 10e0), (2, 15e0), (3, 20e0)");

        // with FILTER
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT avg(value) FILTER (WHERE value > 15) FROM (VALUES (1, 10), (2, 20), (3, 30)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, 20e0), (3, 25e0)");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value) FILTER (WHERE value > 15) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '0'), (3, BIGINT '0')");

        // with ORDER BY
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT array_agg(value ORDER BY value) FROM (VALUES (1, 10), (2, 30), (3, 20)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, ARRAY[10]), (2, ARRAY[10, 30]), (3, ARRAY[10, 20, 30])");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT array_agg(value ORDER BY value) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, CAST(null AS array(integer))), (2, null), (3, null)");

        // multiple aggregations
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value), avg(value) FROM (VALUES (1, 10), (2, 10), (3, 40)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1', 10e0), (2, BIGINT '2', 10e0), (3, BIGINT '3', 20e0)");

        // with projection
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value) + 100, avg(value) * 100e0 FROM (VALUES (1, 10), (2, 10), (3, 40)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '101', 1000e0), (2, BIGINT '102', 1000e0), (3, BIGINT '103', 2000e0)");

        // null in subquery
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value) FROM (VALUES (1, null), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(*) FROM (VALUES (1, null), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '2'), (3, BIGINT '3')");

        // empty lateral relation
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '0'), (3, BIGINT '0')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(*) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '0'), (3, BIGINT '0')");

        // empty lateral relation for some input row
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT count(*) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");

        // INNER join
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(value) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '0'), (2, BIGINT '1'), (3, BIGINT '2')");
    }

    @Test
    public void testLeftCorrelatedJoinWithDistinctAggregation()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT value FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, 10), (2, 10), (2, 20), (3, 10), (3, 20)");

        // with projection
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT value + 100 FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, 110), (2, 110), (2, 120), (3, 110), (3, 120)");

        // null in subquery
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT value FROM (VALUES (1, null), (2, null), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, null), (3, 10)");

        // empty lateral relation
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT value FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, CAST(null AS integer)), (2, null), (3, null)");

        // empty lateral relation for some input row
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT value FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key < t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, 10), (3, 10), (3, 20)");
    }

    @Test
    public void testInnerCorrelatedJoinWithGroupedDistinctAggregation()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(DISTINCT key) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '1'), (2, BIGINT '1'), (3, BIGINT '1'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT avg(DISTINCT key) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, 1e0), (2, 1e0), (2, 2e0), (3, 2e0), (3, 2e0)");

        // with FILTER
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT avg(DISTINCT key) FILTER (WHERE key > 1) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (2, 2e0), (3, 2e0), (3, 3e0)");

        // with ORDER BY
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT array_agg(DISTINCT key ORDER BY key) FROM (VALUES (1, 10), (2, 20), (2, 10), (2, 10), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, ARRAY[1]), (2, ARRAY[1, 2]), (2, ARRAY[2]), (3, ARRAY[1, 2, 3]), (3, ARRAY[2])");

        // with projection
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(DISTINCT key) + 100 FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '101'), (2, BIGINT '101'), (2, BIGINT '101'), (3, BIGINT '101'), (3, BIGINT '102')");

        // null in subquery
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(DISTINCT key) FROM (VALUES (1, null), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '1'), (2, BIGINT '1'), (3, BIGINT '1'), (3, BIGINT '1'), (3, BIGINT '1')");

        // empty lateral relation
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(DISTINCT key) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .returnsEmptyResult();

        // empty lateral relation for some input row
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(DISTINCT key) FROM (VALUES (1, 10), (2, 10), (3, 10)) t2(key, value) WHERE t2.key < t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (2, BIGINT '1'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT avg(DISTINCT key) FROM (VALUES (1, 10), (2, 10), (3, 10)) t2(key, value) WHERE t2.key < t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (2, 1e0), (3, 1.5e0)");
    }

    @Test
    public void testInnerCorrelatedJoinWithGroupedAggregation()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(key) FROM (VALUES (1, 10), (2, 10), (2, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '3'), (3, BIGINT '3')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(*) FROM (VALUES (1, 10), (2, 10), (2, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '3'), (3, BIGINT '3')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT avg(key) FROM (VALUES (1, 10), (2, 10), (2, 20)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, 1e0), (2, 1.5e0), (2, 2e0), (3, 1.5e0), (3, 2e0)");

        // with FILTER
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT avg(key) FILTER (WHERE key > 1) FROM (VALUES (1, 10), (2, 20), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (2, 2e0), (3, 2e0), (3, 3e0)");

        // with ORDER BY
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT array_agg(key ORDER BY key) FROM (VALUES (1, 10), (2, 20), (2, 10), (2, 10), (3, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, ARRAY[1]), (2, ARRAY[1, 2, 2]), (2, ARRAY[2]), (3, ARRAY[1, 2, 2, 3]), (3, ARRAY[2])");

        // with projection
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(key) + 100 FROM (VALUES (1, 10), (2, 10), (2, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '101'), (2, BIGINT '103'), (3, BIGINT '103')");

        // null in subquery
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(key) FROM (VALUES (1, null), (2, 10), (2, 10)) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '1'), (2, BIGINT '2'), (3, BIGINT '1'), (3, BIGINT '2')");

        // empty lateral relation
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(key) FROM (SELECT 0, 0 WHERE false) t2(key, value) WHERE t2.key <= t.key GROUP BY value) " +
                "ON TRUE"))
                .returnsEmptyResult();

        // empty lateral relation for some input row
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT count(key) FROM (VALUES (1, 10), (2, 10), (3, 10)) t2(key, value) WHERE t2.key < t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (2, BIGINT '1'), (3, BIGINT '2')");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2, 3) t(key) " +
                "INNER JOIN " +
                "LATERAL (SELECT avg(key) FROM (VALUES (1, 10), (2, 10), (3, 10)) t2(key, value) WHERE t2.key < t.key GROUP BY value) " +
                "ON TRUE"))
                .matches("VALUES (2, 1e0), (3, 1.5e0)");
    }

    @Test
    public void testArrayAgg()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT array_agg(value) FROM (SELECT 1, 1 WHERE false) t2(key, value) WHERE t2.key = t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, CAST(null AS array(integer)))");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT array_agg(value) FILTER (WHERE value > 1) FROM (VALUES (1, 1), (2, 2)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, ARRAY[2])");
    }

    @Test
    public void testChecksum()
    {
        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT checksum(value) FROM (SELECT 1, 1 WHERE false) t2(key, value) WHERE t2.key = t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, CAST(null AS varbinary))");

        assertThat(assertions.query("SELECT * FROM " +
                "(VALUES 1, 2) t(key) " +
                "LEFT JOIN " +
                "LATERAL (SELECT checksum(value) FILTER (WHERE value > 1) FROM (VALUES (1, 1), (2, 2)) t2(key, value) WHERE t2.key <= t.key) " +
                "ON TRUE"))
                .matches("VALUES (1, null), (2, x'd0f70cebd131ec61')");
    }
}
