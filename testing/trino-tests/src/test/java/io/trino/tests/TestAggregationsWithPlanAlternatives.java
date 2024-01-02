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
import io.trino.connector.alternatives.MockPlanAlternativePlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAggregationsWithPlanAlternatives
        extends TestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().withPlanAlternatives().build();

        queryRunner.installPlugin(new MockPlanAlternativePlugin(new MemoryPlugin()));
        queryRunner.createCatalog("memory", "plan_alternatives_memory");

        // create test table for pre-aggregate-case-aggregations optimizer and populate it with data
        Session memorySession = testSessionBuilder().setCatalog("memory").setSchema("default").build();
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
    @Override
    public void testPreAggregateWithFilter()
    {
        Session memorySession = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();

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
                plan -> assertAggregationNodeCount(plan, 5)); // 5 instead of 4 because the bottom partial aggregation has 2 alternatives
    }
}
