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
package io.trino.tests;

import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregations
        extends AbstractTestAggregations
{
    private final Session memorySession = testSessionBuilder()
            .setCatalog("memory")
            .setSchema("default")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = TpchQueryRunner.builder().build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");

        // create test table for pre-aggregate-case-aggregations optimizer and populate it with data
        queryRunner.execute(memorySession, "CREATE TABLE test_table (key VARCHAR, sequence BIGINT, value DECIMAL(2, 0))");
        queryRunner.execute(memorySession, "" +
                "INSERT INTO test_table VALUES " +
                "('a', 0, 0)," +
                "('a', 0, 1)," +
                "('a', 1, 2)," +
                "('a', 1, 3)," +
                "('b', 0, 10)," +
                "('b', 0, 11)," +
                "('b', 1, 13)," +
                "('b', 1, 14)");

        return queryRunner;
    }

    @Test
    public void testPreAggregate()
    {
        assertQuery(
                memorySession,
                "SELECT " +
                        "key, " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 2 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN cast(value * 2 as real) ELSE cast(0 as real) END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 1, null, 2, 1, 10), ('b', 21, null, 13, 11, 54)",
                plan -> assertAggregationNodeCount(plan, 4));

        assertQuery(
                memorySession,
                "SELECT " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 2 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN value * 2 ELSE 0 END) " +
                        "FROM test_table",
                "VALUES (22, null, 2, 11, 64)",
                plan -> assertAggregationNodeCount(plan, 4));

        assertQuery(
                memorySession,
                "SELECT " +
                        "key, " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 2 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN value * 2 ELSE 1 END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 1, null, 2, 1, 12), ('b', 21, null, 13, 11, 56)",
                plan -> assertAggregationNodeCount(plan, 2));

        // non null default value on max aggregation
        assertQuery(
                memorySession,
                "SELECT " +
                        "key, " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 2 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "max(CASE WHEN sequence = 1 THEN value * 2 ELSE 100 END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 1, null, 2, 1, 100), ('b', 21, null, 13, 11, 100)",
                plan -> assertAggregationNodeCount(plan, 2));

        // no rows matching sequence number
        assertQuery(
                memorySession,
                "SELECT " +
                        "key, " +
                        "sum(CASE WHEN sequence = 42 THEN value ELSE 0 END), " +
                        "sum(CASE WHEN sequence = 42 THEN value END), " +
                        "sum(CASE WHEN sequence = 24 THEN cast(value * 2 as real) ELSE cast(0 as real) END), " +
                        "sum(CASE WHEN sequence = 24 THEN cast(value * 2 as real) END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 0, null, 0, null), ('b', 0, null, 0, null)",
                plan -> assertAggregationNodeCount(plan, 4));

        assertQuery(
                memorySession,
                "SELECT " +
                        "sum(CASE WHEN sequence = 42 THEN value ELSE 0 END), " +
                        "sum(CASE WHEN sequence = 42 THEN value END), " +
                        "sum(CASE WHEN sequence = 24 THEN cast(value * 2 as real) ELSE cast(0 as real) END), " +
                        "sum(CASE WHEN sequence = 24 THEN cast(value * 2 as real) END) " +
                        "FROM test_table",
                "VALUES (0, null, 0, null)",
                plan -> assertAggregationNodeCount(plan, 4));
    }

    @Test
    public void testPreAggregateWithFilter()
    {
        assertQuery(
                memorySession,
                "SELECT " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 2 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN value * 2 ELSE 0 END) " +
                        "FROM test_table " +
                        "WHERE sequence = 42",
                "VALUES (null, null, null, null, null)",
                plan -> assertAggregationNodeCount(plan, 4));
    }

    @Test
    public void testStreamMinMax()
    {
        assertQuery("SELECT max(a) FROM (VALUES ARRAY[1], ARRAY[2], ARRAY[3]) t(a)", "VALUES ARRAY[3]");
        assertQuery("SELECT max(stream(a)) FROM (VALUES ARRAY[1], ARRAY[2, 4], ARRAY[3, 5, 0]) t(a)", "VALUES ARRAY[5]");
        assertQuery("SELECT max(stream(a)) FROM (VALUES ARRAY['a'], ARRAY['b'], ARRAY['c']) t(a)", "VALUES ARRAY['c']");

        assertQuery("SELECT min(stream(a)) FROM (VALUES ARRAY[1], ARRAY[2], ARRAY[3]) t(a)", "VALUES ARRAY[1]");
        assertQuery("SELECT min(stream(a)) FROM (VALUES ARRAY[1], ARRAY[2, 4], ARRAY[3, 5, 0]) t(a)", "VALUES ARRAY[0]");

        assertQuery("SELECT max(stream(repeat(nationkey, 3))) FROM nation", "SELECT max(nationkey) FROM nation");
    }

    @Test
    public void testStreamApproxDistinct()
    {
        assertQuery("SELECT approx_distinct(a) FROM (VALUES ARRAY[1, 2, 3], ARRAY[4, 5], ARRAY[6]) t(a)", "VALUES 3");
        assertQuery("SELECT approx_distinct(stream(a)) FROM (VALUES ARRAY[1, 2, 3], ARRAY[4, 5], ARRAY[6]) t(a)", "VALUES 6");
    }

    @Test
    public void testSumStream()
    {
        assertQuery("SELECT SUM(a) OVER (PARTITION BY b) FROM (VALUES (1, 1), (2, 1), (3, 2), (4, 2)) t(a, b)", "VALUES (3), (3), (7), (7)");
        assertQuery("SELECT SUM(stream(a)) FROM (VALUES ARRAY[1, 2, 3], ARRAY[4, 5], ARRAY[6]) t(a)", "VALUES 21");
    }

    @Test
    public void testStreamSumWindow()
    {
        assertQuery("""
                SELECT SUM(stream(a)) OVER (PARTITION BY b)
                FROM (VALUES (ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)], 1), (ARRAY[CAST(4 AS BIGINT), CAST(5 AS BIGINT)], 1), (ARRAY[CAST(6 AS BIGINT)], 2)) t(a, b)""",
                "VALUES (15), (15), (6)");

        assertQuery("""
                SELECT SUM(stream(transform(a, x -> x + 1))) OVER (PARTITION BY b)
                FROM (VALUES (ARRAY[1, 2, 3], 1), (ARRAY[4, 5], 1), (ARRAY[6], 2)) t(a, b)""",
                "VALUES (20), (20), (7)");

        assertQuery("""
                SELECT SUM(stream(filter(a, x -> x >= 5))) OVER (PARTITION BY b)
                FROM (VALUES (ARRAY[1, 2, 3], 1), (ARRAY[4, 5], 1), (ARRAY[6], 2)) t(a, b)""",
                "VALUES (5), (5), (6)");
    }

    @Test
    public void testStreamMapKey()
    {
        assertQuery("""
            SELECT
                approx_distinct(stream(map_keys(a)))
                , SUM(stream(map_values(a)))
            FROM (VALUES
                MAP_FROM_ENTRIES(ARRAY[ROW('a', 1)]),
                MAP_FROM_ENTRIES(ARRAY[ROW('b', 2), ROW('x', 21)]),
                MAP_FROM_ENTRIES(ARRAY[ROW('c', 3), ROW('d', 4), ROW('0', -9)])
            ) t(a)""",
                "VALUES (6, 22)");

        assertQuery("""
            SELECT
                CAST(approx_most_frequent(3, stream(map_keys(a)), 10) AS JSON)
            FROM (VALUES
                MAP_FROM_ENTRIES(ARRAY[ROW(CAST('a' AS VARCHAR), 1)]),
                MAP_FROM_ENTRIES(ARRAY[ROW(CAST('b' AS VARCHAR), 2), ROW(CAST('x' AS VARCHAR), 21)]),
                MAP_FROM_ENTRIES(ARRAY[ROW(CAST('c' AS VARCHAR), 3), ROW(CAST('d' AS VARCHAR), 4), ROW(CAST('0' AS VARCHAR), -9)])
            ) t(a)""",
                "VALUES '{\"a\":1,\"b\":1,\"x\":1}'");
    }

    @Test
    public void testNestedArrayStream()
    {
        assertQuery("SELECT CAST(max(stream(a)) AS JSON) FROM (VALUES ARRAY[ARRAY[1, 2]], ARRAY[ARRAY[2, 3]], ARRAY[ARRAY[3, 4]]) t(a)", "VALUES '[[3, 4]]'");
        assertQuery("SELECT CAST(max(stream(ARRAY[ARRAY[nationkey, nationkey + 1]])) AS JSON) FROM nation", "VALUES '[[24, 25]]'");

        assertQuery("SELECT CAST(max(stream(stream(a))) AS JSON) FROM (VALUES ARRAY[ARRAY[1]], ARRAY[ARRAY[2]], ARRAY[ARRAY[3]]) t(a)", "VALUES '[[3]]'");
    }

    @Test
    public void testApproxMostFrequent()
    {
        assertQuery("SELECT CAST(approx_most_frequent(2, a, 2) AS JSON) FROM (VALUES (1), (2), (3), (4), (5), (6), (6)) t(a)", "VALUES '{\"5\":3,\"6\":4}'");
        assertQuery("SELECT CAST(approx_most_frequent(2, stream(a), 2) AS JSON) FROM (VALUES ARRAY[1, 2, 3], ARRAY[4, 5, 6], ARRAY[6]) t(a)", "VALUES '{\"5\":3,\"6\":4}'");
        assertQuery("SELECT CAST(approx_most_frequent(2, stream(a), 2) AS JSON) FROM (VALUES ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)], ARRAY[CAST(4 AS BIGINT), CAST(5 AS BIGINT), CAST(6 AS BIGINT)], ARRAY[CAST(6 AS BIGINT)]) t(a)", "VALUES '{\"5\":3,\"6\":4}'");
    }

    private void assertAggregationNodeCount(Plan plan, int count)
    {
        assertThat(countOfMatchingNodes(plan, AggregationNode.class::isInstance)).isEqualTo(count);
    }

    private static int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        return searchFrom(plan.getRoot()).where(predicate).count();
    }
}
