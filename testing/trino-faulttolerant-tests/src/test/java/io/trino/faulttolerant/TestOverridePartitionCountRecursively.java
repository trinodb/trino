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
package io.trino.faulttolerant;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.connector.InMemoryCatalogStore;
import io.trino.connector.LazyCatalogFactory;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmentIdAllocator;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.consumesHashPartitionedInput;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.getMaxPlanFragmentId;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.getMaxPlanId;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.overridePartitionCountRecursively;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TopologicalOrderSubPlanVisitor.sortPlanInTopologicalOrder;
import static io.trino.tpch.TpchTable.getTables;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOverridePartitionCountRecursively
        extends AbstractTestQueryFramework
{
    private static final int PARTITION_COUNT_OVERRIDE = 40;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> extraPropertiesWithRuntimeAdaptivePartitioning = ImmutableMap.builder();
        extraPropertiesWithRuntimeAdaptivePartitioning.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        extraPropertiesWithRuntimeAdaptivePartitioning.putAll(FaultTolerantExecutionConnectorTestHelper.enforceRuntimeAdaptivePartitioningProperties());

        return HiveQueryRunner.builder()
                .setExtraProperties(extraPropertiesWithRuntimeAdaptivePartitioning.buildOrThrow())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of("exchange.base-directories",
                                    System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                })
                .setInitialTables(getTables())
                .build();
    }

    @Test
    public void testCreateTableAs()
    {
        // already started: 3, 5, 6
        // added fragments: 7, 8, 9
        //   0                 0
        //   |                 |
        //   1                 1
        //   |                 |
        //   2                 2
        //  / \              /  \
        // 3*  4     =>    [7]   4
        //    / \           |   / \
        //   5* 6*         3* [8] [9]
        //                     |   |
        //                    5*   6*
        assertOverridePartitionCountRecursively(
                noJoinReordering(),
                "CREATE TABLE tmp AS " +
                "SELECT n1.* FROM nation n1 " +
                        "RIGHT JOIN " +
                        "(SELECT n.nationkey FROM (SELECT * FROM lineitem WHERE suppkey BETWEEN 20 and 30) l LEFT JOIN nation n on l.suppkey = n.nationkey) n2" +
                        " ON n1.nationkey = n2.nationkey + 1",
                ImmutableMap.<Integer, FragmentPartitioningInfo>builder()
                        .put(0, new FragmentPartitioningInfo(COORDINATOR_DISTRIBUTION, Optional.empty(), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(1, new FragmentPartitioningInfo(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, Optional.empty(), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(2, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.empty(), SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, Optional.empty()))
                        .put(3, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .put(4, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .put(5, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .put(6, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .buildOrThrow(),
                ImmutableMap.<Integer, FragmentPartitioningInfo>builder()
                        .put(0, new FragmentPartitioningInfo(COORDINATOR_DISTRIBUTION, Optional.empty(), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(1, new FragmentPartitioningInfo(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, Optional.empty(), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(2, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(40), SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, Optional.empty()))
                        .put(3, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .put(4, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(40), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(5, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .put(6, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.empty()))
                        .put(7, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(5), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(8, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(5), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(9, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(5), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .buildOrThrow(),
                ImmutableSet.of(3, 5, 6));
    }

    @Test
    public void testSkipBroadcastSubtree()
    {
        // result of fragment 7 will be broadcast,
        // so no runtime adaptive partitioning will be applied to its subtree
        // already started: 4, 10, 11, 12
        // added fragments: 13
        //        0                  0
        //        |                  |
        //        1                  1
        //       / \              /     \
        //      2   7     =>     2       7
        //     / \  |           / \      |
        //    3  6  8          3  6      8
        //   / \   / \        / \       / \
        //  4* 5  9  12*    [13] 5     9  12*
        //       / \          |       / \
        //     10* 11*       4*     10* 11*
        assertOverridePartitionCountRecursively(
                noJoinReordering(),
                "SELECT\n" +
                        "  ps.partkey,\n" +
                        "  sum(ps.supplycost * ps.availqty) AS value\n" +
                        "FROM\n" +
                        "  partsupp ps,\n" +
                        "  supplier s,\n" +
                        "  nation n\n" +
                        "WHERE\n" +
                        "  ps.suppkey = s.suppkey\n" +
                        "  AND s.nationkey = n.nationkey\n" +
                        "  AND n.name = 'GERMANY'\n" +
                        "GROUP BY\n" +
                        "  ps.partkey\n" +
                        "HAVING\n" +
                        "  sum(ps.supplycost * ps.availqty) > (\n" +
                        "    SELECT sum(ps.supplycost * ps.availqty) * 0.0001\n" +
                        "    FROM\n" +
                        "      partsupp ps,\n" +
                        "      supplier s,\n" +
                        "      nation n\n" +
                        "    WHERE\n" +
                        "      ps.suppkey = s.suppkey\n" +
                        "      AND s.nationkey = n.nationkey\n" +
                        "      AND n.name = 'GERMANY'\n" +
                        "  )\n" +
                        "ORDER BY\n" +
                        "  value DESC",
                ImmutableMap.<Integer, FragmentPartitioningInfo>builder()
                        .put(0, new FragmentPartitioningInfo(SINGLE_DISTRIBUTION, Optional.empty(), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(1, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(2, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(3, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(4, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(5, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(6, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(7, new FragmentPartitioningInfo(SINGLE_DISTRIBUTION, Optional.empty(), FIXED_BROADCAST_DISTRIBUTION, Optional.empty()))
                        .put(8, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(9, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(10, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(11, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(12, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .buildOrThrow(),
                ImmutableMap.<Integer, FragmentPartitioningInfo>builder()
                        .put(0, new FragmentPartitioningInfo(SINGLE_DISTRIBUTION, Optional.empty(), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(1, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(40), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(2, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(40), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(3, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(40), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(4, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(5, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(6, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .put(7, new FragmentPartitioningInfo(SINGLE_DISTRIBUTION, Optional.empty(), FIXED_BROADCAST_DISTRIBUTION, Optional.empty()))
                        .put(8, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), SINGLE_DISTRIBUTION, Optional.empty()))
                        .put(9, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(10, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(11, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(12, new FragmentPartitioningInfo(SOURCE_DISTRIBUTION, Optional.empty(), FIXED_HASH_DISTRIBUTION, Optional.of(4)))
                        .put(13, new FragmentPartitioningInfo(FIXED_HASH_DISTRIBUTION, Optional.of(4), FIXED_HASH_DISTRIBUTION, Optional.of(40)))
                        .buildOrThrow(),
                ImmutableSet.of(4, 10, 11, 12));
    }

    private void assertOverridePartitionCountRecursively(
            Session session,
            @Language("SQL") String sql,
            Map<Integer, FragmentPartitioningInfo> fragmentPartitioningInfoBefore,
            Map<Integer, FragmentPartitioningInfo> fragmentPartitioningInfoAfter,
            Set<Integer> startedFragments)
    {
        SubPlan plan = getSubPlan(session, sql);
        List<SubPlan> planInTopologicalOrder = sortPlanInTopologicalOrder(plan);
        assertThat(planInTopologicalOrder).hasSize(fragmentPartitioningInfoBefore.size());
        for (SubPlan subPlan : planInTopologicalOrder) {
            PlanFragment fragment = subPlan.getFragment();
            int fragmentIdAsInt = Integer.parseInt(fragment.getId().toString());
            FragmentPartitioningInfo fragmentPartitioningInfo = fragmentPartitioningInfoBefore.get(fragmentIdAsInt);
            assertEquals(fragment.getPartitionCount(), fragmentPartitioningInfo.inputPartitionCount());
            assertEquals(fragment.getPartitioning(), fragmentPartitioningInfo.inputPartitioning());
            assertEquals(fragment.getOutputPartitioningScheme().getPartitionCount(), fragmentPartitioningInfo.outputPartitionCount());
            assertEquals(fragment.getOutputPartitioningScheme().getPartitioning().getHandle(), fragmentPartitioningInfo.outputPartitioning());
        }

        PlanFragmentIdAllocator planFragmentIdAllocator = new PlanFragmentIdAllocator(getMaxPlanFragmentId(planInTopologicalOrder) + 1);
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator(getMaxPlanId(planInTopologicalOrder) + 1);
        int oldPartitionCount = planInTopologicalOrder.stream()
                .mapToInt(subPlan -> {
                    PlanFragment fragment = subPlan.getFragment();
                    if (consumesHashPartitionedInput(fragment)) {
                        return fragment.getPartitionCount().orElse(getFaultTolerantExecutionMaxPartitionCount(session));
                    }
                    else {
                        return 0;
                    }
                })
                .max()
                .orElseThrow();
        assertTrue(oldPartitionCount > 0);

        SubPlan newPlan = overridePartitionCountRecursively(
                plan,
                oldPartitionCount,
                PARTITION_COUNT_OVERRIDE,
                planFragmentIdAllocator,
                planNodeIdAllocator,
                startedFragments.stream().map(fragmentIdAsInt -> new PlanFragmentId(String.valueOf(fragmentIdAsInt))).collect(toImmutableSet()));
        planInTopologicalOrder = sortPlanInTopologicalOrder(newPlan);
        assertThat(planInTopologicalOrder).hasSize(fragmentPartitioningInfoAfter.size());
        for (SubPlan subPlan : planInTopologicalOrder) {
            PlanFragment fragment = subPlan.getFragment();
            int fragmentIdAsInt = Integer.parseInt(fragment.getId().toString());
            FragmentPartitioningInfo fragmentPartitioningInfo = fragmentPartitioningInfoAfter.get(fragmentIdAsInt);
            assertEquals(fragment.getPartitionCount(), fragmentPartitioningInfo.inputPartitionCount());
            assertEquals(fragment.getPartitioning(), fragmentPartitioningInfo.inputPartitioning());
            assertEquals(fragment.getOutputPartitioningScheme().getPartitionCount(), fragmentPartitioningInfo.outputPartitionCount());
            assertEquals(fragment.getOutputPartitioningScheme().getPartitioning().getHandle(), fragmentPartitioningInfo.outputPartitioning());
        }
    }

    private SubPlan getSubPlan(Session session, @Language("SQL") String sql)
    {
        QueryRunner queryRunner = getDistributedQueryRunner();
        Plan plan = queryRunner.createPlan(session, sql, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
        return transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    transactionSession.getCatalog().ifPresent(catalog -> queryRunner.getMetadata().getCatalogHandle(transactionSession, catalog));
                    return new PlanFragmenter(
                            queryRunner.getMetadata(),
                            queryRunner.getFunctionManager(),
                            queryRunner.getTransactionManager(),
                            new CoordinatorDynamicCatalogManager(new InMemoryCatalogStore(), new LazyCatalogFactory(), directExecutor()),
                            new QueryManagerConfig()).createSubPlans(transactionSession, plan, false, WarningCollector.NOOP);
                });
    }

    private record FragmentPartitioningInfo(
            PartitioningHandle inputPartitioning,
            Optional<Integer> inputPartitionCount,
            PartitioningHandle outputPartitioning,
            Optional<Integer> outputPartitionCount)
    {
        FragmentPartitioningInfo {
            requireNonNull(inputPartitioning, "inputPartitioning is null");
            requireNonNull(inputPartitionCount, "inputPartitionCount is null");
            requireNonNull(outputPartitioning, "outputPartitioning is null");
            requireNonNull(outputPartitionCount, "outputPartitionCount is null");
        }
    }
}
