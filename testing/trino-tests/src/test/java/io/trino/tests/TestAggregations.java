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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

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
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build();

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
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN cast(value * 2 as real) ELSE cast(0 as real) END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 1, 2, 1, 10), ('b', 21, 13, 11, 54)",
                plan -> assertAggregationNodeCount(plan, 4));

        assertQuery(
                memorySession,
                "SELECT " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN value * 2 ELSE 0 END) " +
                        "FROM test_table",
                "VALUES (22, 2, 11, 64)",
                plan -> assertAggregationNodeCount(plan, 4));

        assertQuery(
                memorySession,
                "SELECT " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN value * 2 ELSE 0 END) " +
                        "FROM test_table " +
                        "WHERE sequence = 42",
                "VALUES (null, null, null, null)",
                plan -> assertAggregationNodeCount(plan, 4));

        assertQuery(
                memorySession,
                "SELECT " +
                        "key, " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "sum(CASE WHEN sequence = 1 THEN value * 2 ELSE 1 END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 1, 2, 1, 12), ('b', 21, 13, 11, 56)",
                plan -> assertAggregationNodeCount(plan, 2));

        // non null default value on max aggregation
        assertQuery(
                memorySession,
                "SELECT " +
                        "key, " +
                        "sum(CASE WHEN sequence = 0 THEN value END), " +
                        "min(CASE WHEN sequence = 1 THEN value ELSE null END), " +
                        "max(CASE WHEN sequence = 0 THEN value END), " +
                        "max(CASE WHEN sequence = 1 THEN value * 2 ELSE 100 END) " +
                        "FROM test_table " +
                        "GROUP BY key",
                "VALUES ('a', 1, 2, 1, 100), ('b', 21, 13, 11, 100)",
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

    private void assertAggregationNodeCount(Plan plan, int count)
    {
        assertThat(countOfMatchingNodes(plan, AggregationNode.class::isInstance)).isEqualTo(count);
    }

    private static int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        return searchFrom(plan.getRoot()).where(predicate).count();
    }
}
