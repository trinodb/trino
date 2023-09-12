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
import io.trino.connector.MockConnector;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.TestTableScanNodePartitioning;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.SystemSessionProperties.MAX_WRITER_TASKS_COUNT;
import static io.trino.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestLimitMaxWriterNodesCount
        extends BasePlanTest
{
    private static final String partitionedTable = "partitioned_target_table";
    private static final String unPartitionedTable = "unpartitioned_target_table";
    private static final String bucketedTable = "partitioned_bucketed_target_table";
    private static final String sourceTable = "source_table";
    private static final String catalogName = "mock";
    private static final String catalogNameWithMaxWriterTasksSpecified = "mock_with_max_writer_tasks";
    public static final ConnectorPartitioningHandle SINGLE_BUCKET_HANDLE = new ConnectorPartitioningHandle() {};

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        List<String> tables = ImmutableList.of(partitionedTable, unPartitionedTable, sourceTable, bucketedTable);
        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("default")
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog(
                catalogName,
                prepareConnectorFactory(catalogName, OptionalInt.empty(), tables),
                ImmutableMap.of());
        queryRunner.createCatalog(
                catalogNameWithMaxWriterTasksSpecified,
                prepareConnectorFactory(catalogNameWithMaxWriterTasksSpecified, OptionalInt.of(1), tables),
                ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory prepareConnectorFactory(String catalogName, OptionalInt maxWriterTasks, List<String> tables)
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle(((session, tableName) -> {
                    if (tables.contains(tableName.getTableName())) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                }))
                .withWriterScalingOptions(WriterScalingOptions.ENABLED)
                .withGetInsertLayout((session, tableMetadata) -> {
                    if (tableMetadata.getTableName().equals(partitionedTable)) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("column_a")));
                    }
                    if (tableMetadata.getTableName().equals(bucketedTable)) {
                        return Optional.of(new ConnectorTableLayout(SINGLE_BUCKET_HANDLE, ImmutableList.of("column_a")));
                    }
                    return Optional.empty();
                })
                .withGetNewTableLayout((session, tableMetadata) -> {
                    if (tableMetadata.getTable().getTableName().equals(partitionedTable)) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("column_a")));
                    }
                    if (tableMetadata.getTable().getTableName().equals(bucketedTable)) {
                        return Optional.of(new ConnectorTableLayout(SINGLE_BUCKET_HANDLE, ImmutableList.of("column_a")));
                    }
                    return Optional.empty();
                })
                .withGetLayoutForTableExecute((session, tableHandle) -> {
                    MockConnector.MockConnectorTableExecuteHandle tableExecuteHandle = (MockConnector.MockConnectorTableExecuteHandle) tableHandle;
                    if (tableExecuteHandle.getSchemaTableName().getTableName().equals(partitionedTable)) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("column_a")));
                    }
                    return Optional.empty();
                })
                .withTableProcedures(ImmutableSet.of(new TableProcedureMetadata(
                        "OPTIMIZE",
                        distributedWithFilteringAndRepartitioning(),
                        ImmutableList.of(PropertyMetadata.stringProperty("file_size_threshold", "file_size_threshold", "10GB", false)))))
                .withPartitionProvider(new TestTableScanNodePartitioning.TestPartitioningProvider(new InMemoryNodeManager()))
                .withMaxWriterTasks(maxWriterTasks)
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata("column_a", VARCHAR),
                        new ColumnMetadata("column_b", VARCHAR)))
                .withName(catalogName)
                .build();
    }

    @Test
    public void testPlanWhenInsertToUnpartitionedTableScaleWritersDisabled()
    {
        @Language("SQL") String query = "INSERT INTO unpartitioned_target_table VALUES ('one', 'two')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        // partitionCount for writing stage should be set to because session variable MAX_WRITER_TASKS_COUNT is set to 2
                                        exchange(REMOTE, FIXED_ARBITRARY_DISTRIBUTION, Optional.of(2),
                                                values("column_a", "column_b"))))));
    }

    @Test
    public void testPlanWhenInsertToUnpartitionedTableScaleWritersEnabled()
    {
        @Language("SQL") String query = "INSERT INTO unpartitioned_target_table VALUES ('one', 'two')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "true")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        // partitionCount for writing stage should be set to because session variable MAX_WRITER_TASKS_COUNT is set to 2
                                        exchange(REMOTE, SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, Optional.of(2),
                                                values("column_a", "column_b"))))));
    }

    @Test
    public void testPlanWhenInsertToUnpartitionedSourceDistribution()
    {
        @Language("SQL") String query = "INSERT INTO unpartitioned_target_table VALUES ('one', 'two')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        values("column_a", "column_b")))));
    }

    @Test
    public void testPlanWhenInsertToPartitionedTablePreferredPartitioningEnabled()
    {
        @Language("SQL") String query = "INSERT INTO partitioned_target_table VALUES ('one', 'two'), ('three', 'four')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                project(
                                exchange(LOCAL,
                                        // partitionCount for writing stage should be set to because session variable MAX_WRITER_TASKS_COUNT is set to 2
                                        exchange(REMOTE, SCALED_WRITER_HASH_DISTRIBUTION, Optional.of(2),
                                                project(
                                                        values("column_a", "column_b"))))))));
    }

    @Test
    public void testPlanWhenInsertToPartitionedAndBucketedTable()
    {
        @Language("SQL") String query = "INSERT INTO partitioned_bucketed_target_table VALUES ('one', 'two'), ('three', 'four')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                    exchange(LOCAL,
                                            // partitionCount for writing stage is empty because here partitioning is not system partitioning here
                                            exchange(REMOTE, Optional.empty(),
                                                        values("column_a", "column_b"))))));
    }

    @Test
    public void testPlanWhenMaxWriterTasksSpecified()
    {
        @Language("SQL") String query = "INSERT INTO partitioned_target_table VALUES ('one', 'two'), ('three', 'four')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setCatalog(catalogNameWithMaxWriterTasksSpecified)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                project(
                                        exchange(LOCAL,
                                                // partitionCount for writing stage should be set to 4 because it was specified by connector
                                                exchange(REMOTE, SCALED_WRITER_HASH_DISTRIBUTION, Optional.of(1),
                                                        project(
                                                                values("column_a", "column_b"))))))));
    }

    @Test
    public void testPlanWhenRetryPolicyIsTask()
    {
        @Language("SQL") String query = "INSERT INTO partitioned_target_table VALUES ('one', 'two'), ('three', 'four')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setSystemProperty(RETRY_POLICY, "TASK")
                .setCatalog(catalogNameWithMaxWriterTasksSpecified)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                project(
                                        exchange(LOCAL,
                                                exchange(REMOTE, SCALED_WRITER_HASH_DISTRIBUTION, Optional.empty(),
                                                        project(
                                                                values("column_a", "column_b"))))))));
    }

    @Test
    public void testPlanWhenExecuteOnUnpartitionedTableScaleWritersDisabled()
    {
        @Language("SQL") String query = "ALTER TABLE unpartitioned_target_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        // partitionCount for writing stage should be set to because session variable MAX_WRITER_TASKS_COUNT is set to 2
                                        exchange(REMOTE, FIXED_ARBITRARY_DISTRIBUTION, Optional.of(2),
                                                tableScan(unPartitionedTable))))));
    }

    @Test
    public void testPlanWhenTableExecuteToUnpartitionedTableScaleWritersEnabled()
    {
        @Language("SQL") String query = "ALTER TABLE unpartitioned_target_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "true")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        // partitionCount for writing stage should be set to because session variable MAX_WRITER_TASKS_COUNT is set to 2
                                        exchange(REMOTE, SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, Optional.of(2),
                                                tableScan(unPartitionedTable))))));
    }

    @Test
    public void testPlanWhenTableExecuteToUnpartitionedSourceDistribution()
    {
        @Language("SQL") String query = "ALTER TABLE unpartitioned_target_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        tableScan(unPartitionedTable)))));
    }

    @Test
    public void testPlanWhenTableExecuteToPartitionedTablePreferredPartitioningEnabled()
    {
        @Language("SQL") String query = "ALTER TABLE partitioned_target_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setCatalog(catalogName)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                project(
                                        exchange(LOCAL,
                                                // partitionCount for writing stage should be set to because session variable MAX_WRITER_TASKS_COUNT is set to 2
                                                exchange(REMOTE, SCALED_WRITER_HASH_DISTRIBUTION, Optional.of(2),
                                                        project(
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testPlanTableExecuteWhenMaxWriterTasksSpecified()
    {
        @Language("SQL") String query = "ALTER TABLE partitioned_target_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setCatalog(catalogNameWithMaxWriterTasksSpecified)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                    project(
                                        exchange(LOCAL,
                                                // partitionCount for writing stage should be set to 4 because it was specified by connector
                                                exchange(REMOTE, SCALED_WRITER_HASH_DISTRIBUTION, Optional.of(1),
                                                        project(
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testPlanTableExecuteWhenRetryPolicyIsTask()
    {
        @Language("SQL") String query = "ALTER TABLE partitioned_target_table EXECUTE optimize(file_size_threshold => '10MB')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_TASKS_COUNT, "2")
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setSystemProperty(RETRY_POLICY, "TASK")
                .setCatalog(catalogNameWithMaxWriterTasksSpecified)
                .build();

        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableExecuteNode.class,
                                project(
                                        exchange(LOCAL,
                                                // partitionCount for writing stage is empty because it is FTE mode
                                                exchange(REMOTE, SCALED_WRITER_HASH_DISTRIBUTION, Optional.empty(),
                                                        project(
                                                                node(TableScanNode.class))))))));
    }
}
