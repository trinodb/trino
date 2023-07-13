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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.BucketFunction;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static io.trino.SystemSessionProperties.COLOCATED_JOIN;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestColocatedJoin
        extends BasePlanTest
{
    private static final String TABLE_NAME = "orders";
    private static final String CATALOG_NAME = "mock";
    private static final String SCHEMA_NAME = "default";
    private static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};
    private static final int BUCKET_COUNT = 10;
    private static final String COLUMN_A = "column_a";
    private static final String COLUMN_B = "column_b";

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withGetTableHandle(((session, tableName) -> {
                    if (tableName.getTableName().equals(TABLE_NAME)) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                }))
                .withPartitionProvider(new TestPartitioningProvider())
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata(COLUMN_A, BIGINT),
                        new ColumnMetadata(COLUMN_B, VARCHAR)))
                .withName(CATALOG_NAME)
                .withGetTableProperties((session, tableHandle) -> new ConnectorTableProperties(
                        TupleDomain.all(),
                        Optional.of(new ConnectorTablePartitioning(
                                PARTITIONING_HANDLE,
                                ImmutableList.of(new MockConnectorColumnHandle(COLUMN_A, BIGINT)))),
                        Optional.empty(),
                        ImmutableList.of()))
                .build();

        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA_NAME)
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog(CATALOG_NAME, connectorFactory, ImmutableMap.of());
        return queryRunner;
    }

    @DataProvider(name = "colocated_join_enabled")
    public Object[][] colocatedJoinEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "colocated_join_enabled")
    public void testColocatedJoinWhenNumberOfBucketsInTableScanIsNotSufficient(boolean colocatedJoinEnabled)
    {
        assertDistributedPlan(
                """
                    SELECT
                        orders.column_a,
                        orders.column_b
                    FROM (
                        SELECT
                            column_a,
                            ARBITRARY(column_b) AS column_b,
                            COUNT(*)
                        FROM orders
                        GROUP BY
                            column_a
                        ) t,
                        orders
                        WHERE
                            orders.column_a = t.column_a
                        AND orders.column_b = t.column_b
                """,
                prepareSession(20, colocatedJoinEnabled),
                anyTree(
                        project(
                                anyTree(
                                        tableScan("orders"))),
                        exchange(
                                LOCAL,
                                project(
                                        exchange(
                                                REMOTE,
                                                anyTree(
                                                        tableScan("orders")))))));
    }

    @Test
    public void testColocatedJoinWhenNumberOfBucketsInTableScanIsSufficient()
    {
        assertDistributedPlan(
                """
                    SELECT
                        orders.column_a,
                        orders.column_b
                    FROM (
                        SELECT
                            column_a,
                            ARBITRARY(column_b) AS column_b,
                            COUNT(*)
                        FROM orders
                        GROUP BY
                            column_a
                        ) t,
                        orders
                        WHERE
                            orders.column_a = t.column_a
                            AND orders.column_b = t.column_b
                """,
                prepareSession(0.01, true),
                anyTree(
                        project(
                                anyTree(
                                        tableScan("orders"))),
                        exchange(
                                LOCAL,
                                project(
                                        tableScan("orders")))));
    }

    private Session prepareSession(double tableScanNodePartitioningMinBucketToTaskRatio, boolean colocatedJoinEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(TASK_CONCURRENCY, "16")
                .setSystemProperty(TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO, Double.toString(tableScanNodePartitioningMinBucketToTaskRatio))
                .setSystemProperty(COLOCATED_JOIN, Boolean.toString(colocatedJoinEnabled))
                .build();
    }

    public static class TestPartitioningProvider
            implements ConnectorNodePartitioningProvider
    {
        @Override
        public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
        {
            if (partitioningHandle.equals(PARTITIONING_HANDLE)) {
                return Optional.of(createBucketNodeMap(BUCKET_COUNT));
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
