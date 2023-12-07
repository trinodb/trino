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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.connector.alternatives.MockPlanAlternativePlugin;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Set;

import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_PARTITIONING_ENABLED;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Make sure plan alternatives work with dynamic filters as they interact with each other
@Execution(SAME_THREAD)
public class TestCoordinatorDynamicFilteringWithPlanAlternatives
        extends TestCoordinatorDynamicFiltering
{
    @BeforeAll
    @Override
    public void setup()
    {
        QueryRunner queryRunner = getQueryRunner();
        // create lineitem table in test connector
        queryRunner.installPlugin(new MockPlanAlternativePlugin(
                new TestingPlugin(getRetryPolicy() == RetryPolicy.TASK),
                new TpchPlugin(),
                new TpcdsPlugin(),
                new MemoryPlugin()));
        queryRunner.createCatalog("test", "plan_alternatives_test", ImmutableMap.of());
        queryRunner.createCatalog(
                "tpch",
                "plan_alternatives_tpch",
                ImmutableMap.of(TPCH_PARTITIONING_ENABLED, "false", TPCH_SPLITS_PER_NODE, "16"));
        queryRunner.createCatalog("tpcds", "plan_alternatives_tpcds", ImmutableMap.of());
        queryRunner.createCatalog("memory", "plan_alternatives_memory", ImmutableMap.of("memory.splits-per-node", "16"));
        computeActual("CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
        computeActual("CREATE TABLE customer AS SELECT * FROM tpch.tiny.customer");
        computeActual("CREATE TABLE store_sales AS SELECT * FROM tpcds.tiny.store_sales");
    }

    @Test
    @Timeout(30)
    public void testJoinWithAlternativesOnBothSides()
    {
        testJoinWithAlternativesOnBothSides(BROADCAST, true);
        testJoinWithAlternativesOnBothSides(PARTITIONED, false);
        testJoinWithAlternativesOnBothSides(PARTITIONED, true);
    }

    private void testJoinWithAlternativesOnBothSides(JoinDistributionType joinDistributionType, boolean coordinatorDynamicFiltersDistribution)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType, coordinatorDynamicFiltersDistribution),
                // filter on lineitem and supplier force alternatives when going through PlanAlternativePlugin
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001' AND lineitem.shipmode = 'MAIL' ",
                Set.of(SUPP_KEY_HANDLE),
                collectedDomain -> {
                    TupleDomain<ColumnHandle> expectedRange = TupleDomain.withColumnDomains(ImmutableMap.of(
                            SUPP_KEY_HANDLE,
                            singleValue(BIGINT, 1L)));
                    assertThat(collectedDomain).isEqualTo(expectedRange);
                });
    }
}
