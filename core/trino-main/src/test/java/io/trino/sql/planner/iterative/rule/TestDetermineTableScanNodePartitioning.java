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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.cost.TaskCountEstimator;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.sql.planner.TestTableScanNodePartitioning.BUCKET_COUNT;
import static io.trino.sql.planner.TestTableScanNodePartitioning.COLUMN_A;
import static io.trino.sql.planner.TestTableScanNodePartitioning.COLUMN_B;
import static io.trino.sql.planner.TestTableScanNodePartitioning.COLUMN_HANDLE_A;
import static io.trino.sql.planner.TestTableScanNodePartitioning.COLUMN_HANDLE_B;
import static io.trino.sql.planner.TestTableScanNodePartitioning.DISABLE_PLAN_WITH_TABLE_NODE_PARTITIONING;
import static io.trino.sql.planner.TestTableScanNodePartitioning.ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING;
import static io.trino.sql.planner.TestTableScanNodePartitioning.FIXED_PARTITIONED_TABLE;
import static io.trino.sql.planner.TestTableScanNodePartitioning.PARTITIONED_TABLE;
import static io.trino.sql.planner.TestTableScanNodePartitioning.SINGLE_BUCKET_TABLE;
import static io.trino.sql.planner.TestTableScanNodePartitioning.TEST_SCHEMA;
import static io.trino.sql.planner.TestTableScanNodePartitioning.UNPARTITIONED_TABLE;
import static io.trino.sql.planner.TestTableScanNodePartitioning.createMockFactory;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDetermineTableScanNodePartitioning
{
    private RuleTester tester;

    @BeforeAll
    public void setUp()
    {
        tester = RuleTester.builder()
                .withDefaultCatalogConnectorFactory(createMockFactory())
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testEnablePlanWithTableNodePartitioning()
    {
        testPlanWithTableNodePartitioning(
                ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                PARTITIONED_TABLE,
                BUCKET_COUNT,
                true);
    }

    @Test
    public void testDisablePlanWithTableNodePartitioning()
    {
        testPlanWithTableNodePartitioning(
                DISABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                PARTITIONED_TABLE,
                BUCKET_COUNT,
                false);
    }

    @Test
    public void testTableScanWithoutConnectorPartitioning()
    {
        testPlanWithTableNodePartitioning(
                ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                UNPARTITIONED_TABLE,
                BUCKET_COUNT,
                false);
    }

    @Test
    public void testTableScanWithFixedConnectorPartitioning()
    {
        testPlanWithTableNodePartitioning(
                DISABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                FIXED_PARTITIONED_TABLE,
                BUCKET_COUNT,
                true);
    }

    @Test
    public void testTableScanWithInsufficientBucketToTaskRatio()
    {
        testPlanWithTableNodePartitioning(
                ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                PARTITIONED_TABLE,
                BUCKET_COUNT * 2,
                true);

        testPlanWithTableNodePartitioning(
                ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                PARTITIONED_TABLE,
                BUCKET_COUNT * 2 + 1,
                false);

        testPlanWithTableNodePartitioning(
                ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING,
                SINGLE_BUCKET_TABLE,
                3,
                false);
    }

    private void testPlanWithTableNodePartitioning(
            Session session,
            String tableName,
            int numberOfTasks,
            boolean expectedEnabled)
    {
        TableHandle tableHandle = tester.getCurrentCatalogTableHandle(TEST_SCHEMA, tableName);
        tester.assertThat(new DetermineTableScanNodePartitioning(tester.getMetadata(), tester.getQueryRunner().getNodePartitioningManager(), new TaskCountEstimator(() -> numberOfTasks)))
                .on(p -> {
                    Symbol a = p.symbol(COLUMN_A);
                    Symbol b = p.symbol(COLUMN_B);
                    return p.tableScan(
                            tableHandle,
                            ImmutableList.of(a, b),
                            ImmutableMap.of(a, COLUMN_HANDLE_A, b, COLUMN_HANDLE_B));
                })
                .withSession(session)
                .matches(
                        tableScan(
                                tableHandle.getConnectorHandle()::equals,
                                TupleDomain.all(),
                                ImmutableMap.of(
                                        "A", COLUMN_HANDLE_A::equals,
                                        "B", COLUMN_HANDLE_B::equals))
                                .with(planWithTableNodePartitioning(expectedEnabled)));
    }

    private Matcher planWithTableNodePartitioning(boolean enabled)
    {
        return new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof TableScanNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                TableScanNode tableScanNode = (TableScanNode) node;
                if (tableScanNode.getUseConnectorNodePartitioning().isEmpty()) {
                    return NO_MATCH;
                }

                if (tableScanNode.isUseConnectorNodePartitioning() != enabled) {
                    return NO_MATCH;
                }

                return MatchResult.match();
            }
        };
    }
}
