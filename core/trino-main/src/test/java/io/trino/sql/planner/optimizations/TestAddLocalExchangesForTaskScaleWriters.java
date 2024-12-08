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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.connector.MockConnector;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MockConnectorTransactionHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;
import static io.trino.spi.statistics.TableStatistics.empty;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSession;

public class TestAddLocalExchangesForTaskScaleWriters
        extends BasePlanTest
{
    private static final ConnectorPartitioningHandle CONNECTOR_PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(testSession());
        planTester.createCatalog(
                "mock_with_scaled_writers",
                createConnectorFactory("mock_with_scaled_writers", true, true),
                ImmutableMap.of());
        planTester.createCatalog(
                "mock_without_scaled_writers",
                createConnectorFactory("mock_without_scaled_writers", true, false),
                ImmutableMap.of());
        planTester.createCatalog(
                "mock_without_multiple_writer_per_partition",
                createConnectorFactory("mock_without_multiple_writer_per_partition", false, true),
                ImmutableMap.of());
        return planTester;
    }

    private MockConnectorFactory createConnectorFactory(
            String catalogHandle,
            boolean supportsMultipleWritersPerPartition,
            boolean writerScalingEnabledWithinTask)
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle((session, tableName) -> {
                    if (tableName.getTableName().equals("source_table")
                            || tableName.getTableName().equals("system_partitioned_table")
                            || tableName.getTableName().equals("connector_partitioned_table")
                            || tableName.getTableName().equals("unpartitioned_table")) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                })
                .withWriterScalingOptions(new WriterScalingOptions(true, writerScalingEnabledWithinTask))
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
                .withGetLayoutForTableExecute((session, tableHandle) -> {
                    MockConnector.MockConnectorTableExecuteHandle tableExecuteHandle = (MockConnector.MockConnectorTableExecuteHandle) tableHandle;
                    if (tableExecuteHandle.getSchemaTableName().getTableName().equals("system_partitioned_table")) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("year")));
                    }
                    return Optional.empty();
                })
                .withTableProcedures(ImmutableSet.of(new TableProcedureMetadata(
                        "OPTIMIZE",
                        distributedWithFilteringAndRepartitioning(),
                        ImmutableList.of(PropertyMetadata.stringProperty("file_size_threshold", "file_size_threshold", "10GB", false)))))
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
    public void testLocalScaledUnpartitionedWriterDistribution()
    {
        assertDistributedPlan(
                "INSERT INTO unpartitioned_table SELECT * FROM source_table",
                testingSessionBuilder()
                        .setCatalog("mock_without_multiple_writer_per_partition")
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
                testingSessionBuilder()
                        .setCatalog("mock_without_multiple_writer_per_partition")
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

    @Test
    public void testLocalScaledUnpartitionedWriterWithPerTaskScalingDisabled()
    {
        assertDistributedPlan(
                "INSERT INTO unpartitioned_table SELECT * FROM source_table",
                testingSessionBuilder()
                        .setCatalog("mock_without_scaled_writers")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
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
                "INSERT INTO unpartitioned_table SELECT * FROM source_table",
                testingSessionBuilder()
                        .setCatalog("mock_without_scaled_writers")
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

    @Test
    public void testLocalScaledPartitionedWriterWithoutSupportForMultipleWritersPerPartition()
    {
        for (boolean taskScaleWritersEnabled : Arrays.asList(true, false)) {
            String catalogName = "mock_without_multiple_writer_per_partition";
            PartitioningHandle partitioningHandle = new PartitioningHandle(
                    Optional.of(getCatalogHandle(catalogName)),
                    Optional.of(MockConnectorTransactionHandle.INSTANCE),
                    CONNECTOR_PARTITIONING_HANDLE);

            assertDistributedPlan(
                    "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                    testingSessionBuilder()
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
    }

    @Test
    public void testLocalScaledPartitionedWriterWithPerTaskScalingDisabled()
    {
        for (boolean taskScaleWritersEnabled : Arrays.asList(true, false)) {
            String catalogName = "mock_without_scaled_writers";
            PartitioningHandle partitioningHandle = new PartitioningHandle(
                    Optional.of(getCatalogHandle(catalogName)),
                    Optional.of(MockConnectorTransactionHandle.INSTANCE),
                    CONNECTOR_PARTITIONING_HANDLE);

            assertDistributedPlan(
                    "INSERT INTO connector_partitioned_table SELECT * FROM source_table",
                    testingSessionBuilder()
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
    }

    @Test
    public void testLocalScaledPartitionedWriterForSystemPartitioningWithEnforcedPreferredPartitioning()
    {
        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testingSessionBuilder()
                        .setCatalog("mock_with_scaled_writers")
                        .setSchema("mock")
                        // Enforce preferred partitioning
                        .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, SCALED_WRITER_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testingSessionBuilder()
                        .setCatalog("mock_with_scaled_writers")
                        .setSchema("mock")
                        // Enforce preferred partitioning
                        .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test
    public void testLocalScaledPartitionedWriterForConnectorPartitioning()
    {
        String catalogName = "mock_with_scaled_writers";
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
                testingSessionBuilder()
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
                testingSessionBuilder()
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
                testingSessionBuilder()
                        .setCatalog("mock_with_scaled_writers")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, SCALED_WRITER_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

        assertDistributedPlan(
                "INSERT INTO system_partitioned_table SELECT * FROM source_table",
                testingSessionBuilder()
                        .setCatalog("mock_with_scaled_writers")
                        .setSchema("mock")
                        .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test
    public void testTableExecuteLocalScalingDisabledForPartitionedTable()
    {
        @Language("SQL") String query = "ALTER TABLE system_partitioned_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = testingSessionBuilder()
                .setCatalog("mock_with_scaled_writers")
                .setSchema("mock")
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                node(TableScanNode.class))))));
    }

    @Test
    public void testTableExecuteLocalScalingDisabledForUnpartitionedTable()
    {
        @Language("SQL") String query = "ALTER TABLE unpartitioned_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = testingSessionBuilder()
                .setCatalog("mock_with_scaled_writers")
                .setSchema("mock")
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION,
                                                node(TableScanNode.class))))));
    }

    private SessionBuilder testingSessionBuilder()
    {
        return Session.builder(getPlanTester().getDefaultSession());
    }
}
