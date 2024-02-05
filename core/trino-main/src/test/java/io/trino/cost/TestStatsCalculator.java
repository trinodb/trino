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
package io.trino.cost;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStatsCalculator
{
    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build());
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                "tpch",
                ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testStatsCalculatorUsesLayout()
    {
        assertPlan("SELECT orderstatus FROM orders WHERE orderstatus = 'P'",
                anyTree(node(TableScanNode.class).withOutputRowCount(363.0)));

        assertPlan("SELECT orderstatus FROM orders WHERE orderkey = 42",
                anyTree(node(TableScanNode.class).withOutputRowCount(15000.0)));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql);
            PlanAssert.assertPlan(
                    transactionSession,
                    queryRunner.getPlannerContext().getMetadata(),
                    queryRunner.getPlannerContext().getFunctionManager(),
                    queryRunner.getStatsCalculator(),
                    actualPlan,
                    pattern);
            return null;
        });
    }
}
