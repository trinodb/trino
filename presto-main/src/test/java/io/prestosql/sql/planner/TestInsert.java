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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.connector.MockConnectorTableHandle;
import io.prestosql.plugin.tpch.TpchPartitioningHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static io.prestosql.SystemSessionProperties.TASK_WRITER_COUNT;
import static io.prestosql.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestInsert
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("schema");

        LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());
        queryRunner.createCatalog(
                "mock",
                MockConnectorFactory.builder()
                        .withGetTableHandle((session, schemaTableName) -> {
                            if (schemaTableName.getTableName().equals("test_table_preferred_partitioning")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            return null;
                        })
                        .withGetColumns(name -> ImmutableList.of(
                                new ColumnMetadata("column1", INTEGER),
                                new ColumnMetadata("column2", INTEGER)))
                        .withGetInsertLayout((session, tableName) -> {
                            if (tableName.getTableName().equals("test_table_preferred_partitioning")) {
                                return Optional.of(new ConnectorNewTableLayout(ImmutableList.of("column1")));
                            }

                            return Optional.empty();
                        })
                        .withGetNewTableLayout((session, tableMetadata) -> {
                            if (tableMetadata.getTable().getTableName().equals("new_test_table_preferred_partitioning")) {
                                return Optional.of(new ConnectorNewTableLayout(ImmutableList.of("column1")));
                            }

                            if (tableMetadata.getTable().getTableName().equals("new_test_table_required_partitioning")) {
                                return Optional.of(new ConnectorNewTableLayout(new TpchPartitioningHandle("orders", 10), ImmutableList.of("column1")));
                            }

                            if (tableMetadata.getTable().getTableName().equals("new_test_table_unpartitioned")) {
                                return Optional.empty();
                            }

                            return Optional.empty();
                        })
                        .build(),
                ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testInsertWithPreferredPartitioning()
    {
        assertDistributedPlan(
                "INSERT into test_table_preferred_partitioning VALUES (1, 2)",
                withPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                anyTree(
                                        exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                        anyTree(values("column1", "column2"))))))));
    }

    @Test
    public void testInsertWithoutPreferredPartitioningEnabled()
    {
        assertDistributedPlan(
                "INSERT into test_table_preferred_partitioning VALUES (1, 2)",
                withoutPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                // round robin
                                exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column1", "column2")))));
    }

    @Test
    public void testCreateTableAsSelectWithPreferredPartitioning()
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_preferred_partitioning (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                withPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                anyTree(
                                        exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                        anyTree(values("column1", "column2"))))))));
    }

    @Test
    public void testCreateTableAsSelectWithPreferredPartitioningAndNoPartitioningColumns()
    {
        // cannot use preferred partitioning as CTAS does not use partitioning columns
        assertDistributedPlan(
                "CREATE TABLE new_test_table_preferred_partitioning (column2) AS SELECT * FROM (VALUES 2) t(column2)",
                withPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                // round robin
                                exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column2")))));
    }

    @Test
    public void testCreateTableAsSelectWithoutPreferredPartitioningEnabled()
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_preferred_partitioning (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                withoutPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                // round robin
                                exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column1", "column2")))));
    }

    @Test
    public void testCreateTableAsSelectWithRequiredPartitioning()
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_required_partitioning (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                withPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, GATHER, ImmutableList.of(), ImmutableSet.of(),
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                values("column1", "column2"))))));
    }

    @Test
    public void testCreateTableAsSelectUnpartitioned()
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_unpartitioned (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                withoutPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                // round robin
                                exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column1", "column2")))));
    }

    private Session withPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setSystemProperty(TASK_WRITER_COUNT, "16")
                .build();
    }

    private Session withoutPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setSystemProperty(TASK_WRITER_COUNT, "16")
                .build();
    }
}
