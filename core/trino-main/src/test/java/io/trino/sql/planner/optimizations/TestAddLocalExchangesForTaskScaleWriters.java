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
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MockConnectorTransactionHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.spi.statistics.TableStatistics.empty;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAddLocalExchangesForTaskScaleWriters
        extends BasePlanTest
{
    private static final ConnectorPartitioningHandle CONNECTOR_PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(testSessionBuilder().build());
        queryRunner.createCatalog(
                "mock_dont_report_written_bytes",
                createConnectorFactory("mock_dont_report_written_bytes", false, true),
                ImmutableMap.of());
        queryRunner.createCatalog(
                "mock_report_written_bytes_without_multiple_writer_per_partition",
                createConnectorFactory("mock_report_written_bytes", true, false),
                ImmutableMap.of());
        queryRunner.createCatalog(
                "mock_report_written_bytes_with_multiple_writer_per_partition",
                createConnectorFactory("mock_report_written_bytes_with_multiple_writer_per_partition", true, true),
                ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory createConnectorFactory(
            String catalogHandle,
            boolean supportsWrittenBytes,
            boolean supportsMultipleWritersPerPartition)
    {
        return MockConnectorFactory.builder()
                .withSupportsReportingWrittenBytes(supportsWrittenBytes)
                .withGetTableHandle(((session, tableName) -> {
                    if (tableName.getTableName().equals("source_table")
                            || tableName.getTableName().equals("system_partitioned_table")
                            || tableName.getTableName().equals("connector_partitioned_table")
                            || tableName.getTableName().equals("unpartitioned_table")) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                }))
                .withGetTableStatistics(tableName -> {
                    if (tableName.getTableName().equals("source_table")) {
                        return new TableStatistics(
                                Estimate.of(100),
                                ImmutableMap.of(
                                        new MockConnectorColumnHandle("year", INTEGER),
                                        new ColumnStatistics(Estimate.of(0), Estimate.of(10), Estimate.of(100), Optional.empty())));
                    }
                    return empty();
                })
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata("customer", INTEGER),
                        new ColumnMetadata("year", INTEGER)))
                .withGetInsertLayout((session, tableName) -> {
                    if (tableName.getTableName().equals("system_partitioned_table")) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("year")));
                    }
                    if (tableName.getTableName().equals("connector_partitioned_table")) {
                        return Optional.of(new ConnectorTableLayout(
                                CONNECTOR_PARTITIONING_HANDLE,
                                ImmutableList.of("year"),
                                supportsMultipleWritersPerPartition));
                    }
                    return Optional.empty();
                })
                .withName(catalogHandle)
                .build();
    }

    @Test
    public void testLocalScaledUnpartitionedWriterDistributionWithSupportsReportingWrittenBytes()
    {
        assertDistributedPlan(
                "INSERT INTO unpartitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_report_written_bytes_without_multiple_writer_per_partition")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

        assertDistributedPlan(
                "INSERT INTO unpartitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_report_written_bytes_without_multiple_writer_per_partition")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test(dataProvider = "taskScaleWritersOption")
    public void testLocalScaledPartitionedWriterWithoutSupportForMultipleWritersPerPartition(boolean taskScaleWritersEnabled)
    {
        String catalogName = "mock_report_written_bytes_without_multiple_writer_per_partition";
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(getCatalogHandle(catalogName)),
                Optional.of(MockConnectorTransactionHandle.INSTANCE),
                CONNECTOR_PARTITIONING_HANDLE);

        assertDistributedPlan(
                "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, String.valueOf(taskScaleWritersEnabled))
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, partitioningHandle,
                                        exchange(REMOTE, REPARTITION, partitioningHandle,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test(dataProvider = "taskScaleWritersOption")
    public void testLocalScaledUnpartitionedWriterDistributionWithoutSupportsReportingWrittenBytes(boolean taskScaleWritersEnabled)
    {
        assertDistributedPlan(
                "INSERT INTO unpartitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_dont_report_written_bytes")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, String.valueOf(taskScaleWritersEnabled))
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test(dataProvider = "taskScaleWritersOption")
    public void testLocalScaledPartitionedWriterWithoutSupportsForReportingWrittenBytes(boolean taskScaleWritersEnabled)
    {
        String catalogName = "mock_dont_report_written_bytes";
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(getCatalogHandle(catalogName)),
                Optional.of(MockConnectorTransactionHandle.INSTANCE),
                CONNECTOR_PARTITIONING_HANDLE);

        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, String.valueOf(taskScaleWritersEnabled))
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                project(
                                        exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                        project(
                                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))))));

        assertDistributedPlan(
                "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, String.valueOf(taskScaleWritersEnabled))
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, partitioningHandle,
                                        exchange(REMOTE, REPARTITION, partitioningHandle,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test(dataProvider = "taskScaleWritersOption")
    public void testLocalScaledPartitionedWriterWithoutSupportForReportingWrittenBytesAndPreferredPartitioning(boolean taskScaleWritersEnabled)
    {
        String catalogName = "mock_dont_report_written_bytes";
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(getCatalogHandle(catalogName)),
                Optional.of(MockConnectorTransactionHandle.INSTANCE),
                CONNECTOR_PARTITIONING_HANDLE);

        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, String.valueOf(taskScaleWritersEnabled))
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

        assertDistributedPlan(
                "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, String.valueOf(taskScaleWritersEnabled))
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, partitioningHandle,
                                        exchange(REMOTE, REPARTITION, partitioningHandle,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @DataProvider
    public Object[][] taskScaleWritersOption()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test
    public void testLocalScaledPartitionedWriterForSystemPartitioningWithEnforcedPreferredPartitioning()
    {
        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_report_written_bytes_with_multiple_writer_per_partition")
                        .setSchema("mock")
                        // Enforce preferred partitioning
                        .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                project(
                                        exchange(LOCAL, REPARTITION, SCALED_WRITER_HASH_DISTRIBUTION,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                        project(
                                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))))));

        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_report_written_bytes_with_multiple_writer_per_partition")
                        .setSchema("mock")
                        // Enforce preferred partitioning
                        .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                project(
                                        exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                        project(
                                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))))));
    }

    @Test
    public void testLocalScaledPartitionedWriterForConnectorPartitioning()
    {
        String catalogName = "mock_report_written_bytes_with_multiple_writer_per_partition";
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(getCatalogHandle(catalogName)),
                Optional.of(MockConnectorTransactionHandle.INSTANCE),
                CONNECTOR_PARTITIONING_HANDLE);
        PartitioningHandle scaledPartitioningHandle = new PartitioningHandle(
                Optional.of(getCatalogHandle(catalogName)),
                Optional.of(MockConnectorTransactionHandle.INSTANCE),
                CONNECTOR_PARTITIONING_HANDLE,
                true);

        assertDistributedPlan(
                "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, scaledPartitioningHandle,
                                        exchange(REMOTE, REPARTITION, partitioningHandle,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

        assertDistributedPlan(
                "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, partitioningHandle,
                                        exchange(REMOTE, REPARTITION, partitioningHandle,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test
    public void testLocalScaledPartitionedWriterWithEnforcedLocalPreferredPartitioning()
    {
        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_report_written_bytes_with_multiple_writer_per_partition")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                project(
                                        exchange(LOCAL, REPARTITION, SCALED_WRITER_HASH_DISTRIBUTION,
                                                exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                        project(
                                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))))));

        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testSessionBuilder()
                        .setCatalog("mock_report_written_bytes_with_multiple_writer_per_partition")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }
}
