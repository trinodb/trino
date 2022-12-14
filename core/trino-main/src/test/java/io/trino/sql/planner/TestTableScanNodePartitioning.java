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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableScanNodePartitioning
        extends BasePlanTest
{
    public static final String TEST_SCHEMA = "test_schema";

    public static final Session ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TEST_SCHEMA)
            .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "true")
            .setSystemProperty(TASK_CONCURRENCY, "2") // force parallel plan even on test nodes with single CPU
            .build();
    public static final Session DISABLE_PLAN_WITH_TABLE_NODE_PARTITIONING = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TEST_SCHEMA)
            .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "false")
            .setSystemProperty(TASK_CONCURRENCY, "2") // force parallel plan even on test nodes with single CPU
            .build();

    public static final int BUCKET_COUNT = 10;

    public static final String PARTITIONED_TABLE = "partitioned_table";
    public static final String SINGLE_BUCKET_TABLE = "single_bucket_table";
    public static final String FIXED_PARTITIONED_TABLE = "fixed_partitioned_table";
    public static final String UNPARTITIONED_TABLE = "unpartitioned_table";

    public static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};
    public static final ConnectorPartitioningHandle SINGLE_BUCKET_HANDLE = new ConnectorPartitioningHandle() {};
    public static final ConnectorPartitioningHandle FIXED_PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};

    public static final String COLUMN_A = "column_a";
    public static final String COLUMN_B = "column_b";

    public static final ColumnHandle COLUMN_HANDLE_A = new MockConnectorColumnHandle(COLUMN_A, BIGINT);
    public static final ColumnHandle COLUMN_HANDLE_B = new MockConnectorColumnHandle(COLUMN_B, VARCHAR);

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TEST_SCHEMA)
                .setSystemProperty(TASK_CONCURRENCY, "2"); // force parallel plan even on test nodes with single CPU

        LocalQueryRunner queryRunner = LocalQueryRunner.builder(sessionBuilder.build())
                .withNodeCountForStats(10)
                .build();
        queryRunner.createCatalog(TEST_CATALOG_NAME, createMockFactory(), ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testEnablePlanWithTableNodePartitioning()
    {
        assertTableScanPlannedWithPartitioning(ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING, PARTITIONED_TABLE, PARTITIONING_HANDLE);
    }

    @Test
    public void testDisablePlanWithTableNodePartitioning()
    {
        assertTableScanPlannedWithoutPartitioning(DISABLE_PLAN_WITH_TABLE_NODE_PARTITIONING, PARTITIONED_TABLE);
    }

    @Test
    public void testTableScanWithoutConnectorPartitioning()
    {
        assertTableScanPlannedWithoutPartitioning(ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING, UNPARTITIONED_TABLE);
    }

    @Test
    public void testTableScanWithFixedConnectorPartitioning()
    {
        assertTableScanPlannedWithPartitioning(DISABLE_PLAN_WITH_TABLE_NODE_PARTITIONING, FIXED_PARTITIONED_TABLE, FIXED_PARTITIONING_HANDLE);
    }

    @Test
    public void testTableScanWithInsufficientBucketToTaskRatio()
    {
        assertTableScanPlannedWithoutPartitioning(ENABLE_PLAN_WITH_TABLE_NODE_PARTITIONING, SINGLE_BUCKET_TABLE);
    }

    void assertTableScanPlannedWithPartitioning(Session session, String table, ConnectorPartitioningHandle expectedPartitioning)
    {
        String query = "SELECT count(column_b) FROM " + table + " GROUP BY column_a";
        assertDistributedPlan(query, session,
                anyTree(
                        aggregation(ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("COUNT_PART"))), FINAL,
                                exchange(LOCAL, REPARTITION,
                                        project(
                                                aggregation(ImmutableMap.of("COUNT_PART", functionCall("count", ImmutableList.of("B"))), PARTIAL,
                                                        tableScan(table, ImmutableMap.of("A", "column_a", "B", "column_b"))))))));
        SubPlan subPlan = subplan(query, OPTIMIZED_AND_VALIDATED, false, session);
        assertThat(subPlan.getAllFragments()).hasSize(1);
        assertThat(subPlan.getAllFragments().get(0).getPartitioning().getConnectorHandle()).isEqualTo(expectedPartitioning);
    }

    void assertTableScanPlannedWithoutPartitioning(Session session, String table)
    {
        String query = "SELECT count(column_b) FROM " + table + " GROUP BY column_a";
        assertDistributedPlan("SELECT count(column_b) FROM " + table + " GROUP BY column_a", session,
                anyTree(
                        aggregation(ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("COUNT_PART"))), FINAL,
                                exchange(LOCAL, REPARTITION,
                                        exchange(REMOTE, REPARTITION,
                                                project(
                                                        aggregation(ImmutableMap.of("COUNT_PART", functionCall("count", ImmutableList.of("B"))), PARTIAL,
                                                                tableScan(table, ImmutableMap.of("A", "column_a", "B", "column_b")))))))));
        SubPlan subPlan = subplan(query, OPTIMIZED_AND_VALIDATED, false, session);
        assertThat(subPlan.getAllFragments()).hasSize(2);
        assertThat(subPlan.getAllFragments().get(1).getPartitioning().getConnectorHandle()).isEqualTo(SOURCE_DISTRIBUTION.getConnectorHandle());
    }

    public static MockConnectorFactory createMockFactory()
    {
        return MockConnectorFactory.builder()
                .withPartitionProvider(new TestPartitioningProvider(new InMemoryNodeManager()))
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata(COLUMN_A, BIGINT),
                        new ColumnMetadata(COLUMN_B, VARCHAR)))
                .withGetTableProperties((session, tableHandle) -> {
                    String tableName = ((MockConnectorTableHandle) tableHandle).getTableName().getTableName();
                    if (tableName.equals(PARTITIONED_TABLE)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(PARTITIONING_HANDLE, ImmutableList.of(COLUMN_HANDLE_A))),
                                Optional.empty(),
                                ImmutableList.of());
                    }
                    if (tableName.equals(SINGLE_BUCKET_TABLE)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(SINGLE_BUCKET_HANDLE, ImmutableList.of(COLUMN_HANDLE_A))),
                                Optional.empty(),
                                ImmutableList.of());
                    }
                    if (tableName.equals(FIXED_PARTITIONED_TABLE)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(FIXED_PARTITIONING_HANDLE, ImmutableList.of(COLUMN_HANDLE_A))),
                                Optional.empty(),
                                ImmutableList.of());
                    }
                    return new ConnectorTableProperties();
                })
                .build();
    }

    public static class TestPartitioningProvider
            implements ConnectorNodePartitioningProvider
    {
        private final InternalNodeManager nodeManager;

        public TestPartitioningProvider(InternalNodeManager nodeManager)
        {
            this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        }

        @Override
        public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
        {
            if (partitioningHandle.equals(PARTITIONING_HANDLE)) {
                return Optional.of(createBucketNodeMap(BUCKET_COUNT));
            }
            if (partitioningHandle.equals(SINGLE_BUCKET_HANDLE)) {
                return Optional.of(createBucketNodeMap(1));
            }
            if (partitioningHandle.equals(FIXED_PARTITIONING_HANDLE)) {
                return Optional.of(createBucketNodeMap(ImmutableList.of(nodeManager.getCurrentNode())));
            }
            throw new IllegalArgumentException();
        }

        @Override
        public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
        {
            throw new UnsupportedOperationException();
        }
    }
}
