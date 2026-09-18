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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MIN_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStageScheduler
        extends AbstractTestQueryFramework
{
    private static final String MOCK_CATALOG = "mock";
    private static final String TABLE_NAME = "test_table";
    private static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("default", TABLE_NAME);
    private static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new TestPartitionHandle();
    private static final ColumnHandle KEY_COLUMN_HANDLE = new MockConnectorColumnHandle("key", BIGINT);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(MOCK_CATALOG)
                                .setSchema("default")
                                .build())
                .setWorkerCount(2)
                .build();

        queryRunner.installPlugin(
                new MockConnectorPlugin(
                        MockConnectorFactory.builder()
                                .withGetColumns(_ -> ImmutableList.of(
                                        new ColumnMetadata("key", BIGINT),
                                        new ColumnMetadata("value", BIGINT)))
                                .withGetTableProperties((_, tableHandle) -> {
                                    String tableName = ((MockConnectorTableHandle) tableHandle).getTableName().getTableName();
                                    if (tableName.equals(TABLE_NAME)) {
                                        return new ConnectorTableProperties(
                                                TupleDomain.all(),
                                                Optional.of(new ConnectorTablePartitioning(PARTITIONING_HANDLE, ImmutableList.of(KEY_COLUMN_HANDLE), true)),
                                                Optional.empty(),
                                                ImmutableList.of());
                                    }
                                    return new ConnectorTableProperties();
                                })
                                .withPartitionProvider(new TestPartitioningProvider())
                                .withData(schemaTableName -> {
                                    if (schemaTableName.equals(SCHEMA_TABLE_NAME)) {
                                        ImmutableList.Builder<List<?>> rows = ImmutableList.builder();
                                        for (int i = 0; i < 25; i++) {
                                            rows.add(ImmutableList.of((long) (i % 5), (long) i));
                                        }
                                        return rows.build();
                                    }
                                    return ImmutableList.of();
                                })
                                .build()));
        queryRunner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());
        return queryRunner;
    }

    @Test
    void testTaskCountMatchesPartitionCount()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(MAX_HASH_PARTITION_COUNT, "1")
                .setSystemProperty(MIN_HASH_PARTITION_COUNT, "1")
                .setSystemProperty(TABLE_SCAN_NODE_PARTITIONING_MIN_BUCKET_TO_TASK_RATIO, "0")
                .build();

        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, "SELECT count(*) FROM mock.default.test_table GROUP BY key");

        assertThat(result.result().getMaterializedRows())
                .hasSize(5)
                .allMatch(row -> (long) row.getField(0) == 5);

        QueryInfo queryInfo = queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId());
        List<StageInfo> stages = StagesInfo.getAllStages(queryInfo.getStages());
        StageInfo partitionedStage = stages.stream()
                .filter(stage -> stage.plan() != null && stage.plan().getPartitioning().getConnectorHandle() instanceof TestPartitionHandle)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No stage with connector partitioning found"));

        assertThat(partitionedStage.tasks()).hasSize(1);
    }

    public record TestPartitionHandle()
            implements ConnectorPartitioningHandle {}

    private static class TestPartitioningProvider
            implements ConnectorNodePartitioningProvider
    {
        @Override
        public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorPartitioningHandle partitioningHandle)
        {
            if (partitioningHandle.equals(PARTITIONING_HANDLE)) {
                return Optional.of(createBucketNodeMap(1));
            }
            throw new IllegalArgumentException("Unknown partitioning handle: " + partitioningHandle);
        }

        @Override
        public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorPartitioningHandle partitioningHandle,
                int bucketCount)
        {
            return _ -> 0;
        }

        @Override
        public BucketFunction getBucketFunction(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorPartitioningHandle partitioningHandle,
                List<Type> partitionChannelTypes,
                int bucketCount)
        {
            return (_, _) -> 0;
        }
    }
}
