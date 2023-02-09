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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.MAX_WRITER_NODES_COUNT;
import static io.trino.SystemSessionProperties.PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestLimitMaxWriterNodesCount
        extends BasePlanTest
{
    private static final String partitionedTable = "partitioned_target_table";
    private static final String unPartitionedTable = "unpartitioned_target_table";
    private static final String sourceTable = "source_table";

    private static final String catalogName = "mock";

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        List<String> tables = ImmutableList.of(partitionedTable, unPartitionedTable, sourceTable);
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withGetTableHandle(((session, tableName) -> {
                    if (tables.contains(tableName.getTableName())) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                }))
                .withGetInsertLayout((session, tableMetadata) -> {
                    if (tableMetadata.getTableName().equals(partitionedTable)) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("column_a")));
                    }
                    return Optional.empty();
                })
                .withGetNewTableLayout((session, tableMetadata) -> {
                    if (tableMetadata.getTable().getTableName().equals(partitionedTable)) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("column_a")));
                    }
                    return Optional.empty();
                })
                .withSupportsReportingWrittenBytes(true)
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata("column_a", VARCHAR),
                        new ColumnMetadata("column_b", VARCHAR)))
                .withName(catalogName)
                .build();

        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("default")
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog(
                catalogName,
                connectorFactory,
                ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testPlanWhenInsertToPartitionedTable()
    {
        @Language("SQL") String query = "INSERT INTO partitioned_target_table VALUES ('one', 'two'), ('three', 'four')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_NODES_COUNT, "2")
                .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
                .build();

        // TestLimitMaxWriterNodesCount optimizer rule should fire and set the partitionCount to 2 for remote exchanges
        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                project(
                                exchange(LOCAL,
                                        exchange(REMOTE, Optional.of(2),
                                                project(
                                                        values("column_a", "column_b"))))))));
    }

    @Test
    public void testPlanWhenInsertToUnpartitionedTableScaleWritersDisabled()
    {
        @Language("SQL") String query = "INSERT INTO unpartitioned_target_table VALUES ('one', 'two')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_NODES_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "false")
                .build();

        // TestLimitMaxWriterNodesCount optimizer rule should fire and set the partitionCount to 2 for remote exchanges
        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        exchange(REMOTE, Optional.of(2),
                                                values("column_a", "column_b"))))));
    }

    @Test
    public void testPlanWhenInsertToUnpartitionedTableScaleWritersEnabled()
    {
        @Language("SQL") String query = "INSERT INTO unpartitioned_target_table VALUES ('one', 'two')";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_NODES_COUNT, "2")
                .setSystemProperty(SCALE_WRITERS, "true")
                .build();

        // TestLimitMaxWriterNodesCount optimizer rule should not fire because scale writers is enabled - no partitioning
        assertDistributedPlan(
                query,
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        exchange(REMOTE, Optional.empty(),
                                                values("column_a", "column_b"))))));
    }

    @Test
    public void testPlanWhenThereIsNoTableWriter()
    {
        @Language("SQL") String query = "SELECT count(*) FROM source_table";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_WRITER_NODES_COUNT, "2")
                .build();

        // TestLimitMaxWriterNodesCount optimizer rule should be skipped when there is no TableWriterNode
        assertDistributedPlan(
                query,
                session,
                output(
                        node(AggregationNode.class,
                                exchange(LOCAL, Optional.empty(),
                                        exchange(REMOTE, Optional.empty(),
                                                node(AggregationNode.class,
                                                        node(TableScanNode.class)))))));
    }
}
