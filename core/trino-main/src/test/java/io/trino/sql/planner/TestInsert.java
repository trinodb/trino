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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.plugin.tpch.TpchPartitioningHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.SystemSessionProperties.TASK_WRITER_COUNT;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSessionBuilder;

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

                            if (schemaTableName.getTableName().equals("test_table_required_partitioning")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            return null;
                        })
                        .withGetColumns(name -> ImmutableList.of(
                                new ColumnMetadata("column1", INTEGER),
                                new ColumnMetadata("column2", INTEGER)))
                        .withGetInsertLayout((session, tableName) -> {
                            if (tableName.getTableName().equals("test_table_preferred_partitioning")) {
                                return Optional.of(new ConnectorTableLayout(ImmutableList.of("column1")));
                            }

                            if (tableName.getTableName().equals("test_table_required_partitioning")) {
                                return Optional.of(new ConnectorTableLayout(new TpchPartitioningHandle("orders", 10), ImmutableList.of("column1")));
                            }

                            return Optional.empty();
                        })
                        .withGetNewTableLayout((session, tableMetadata) -> {
                            if (tableMetadata.getTable().getTableName().equals("new_test_table_preferred_partitioning")) {
                                return Optional.of(new ConnectorTableLayout(ImmutableList.of("column1")));
                            }

                            if (tableMetadata.getTable().getTableName().equals("new_test_table_required_partitioning")) {
                                return Optional.of(new ConnectorTableLayout(new TpchPartitioningHandle("orders", 10), ImmutableList.of("column1")));
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
                withForcedPreferredPartitioning(),
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
                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column1", "column2")))));
    }

    @Test
    public void testInsertWithRequiredPartitioning()
    {
        testInsertWithRequiredPartitioning(withForcedPreferredPartitioning());
        testInsertWithRequiredPartitioning(withoutPreferredPartitioning());
    }

    private void testInsertWithRequiredPartitioning(Session session)
    {
        assertDistributedPlan(
                "INSERT into test_table_required_partitioning VALUES (1, 2)",
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                values("column1", "column2"))
                                                .with(exchangeWithoutSystemPartitioning()))
                                        .with(exchangeWithoutSystemPartitioning()))));
    }

    @Test
    public void testCreateTableAsSelectWithPreferredPartitioning()
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_preferred_partitioning (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                withForcedPreferredPartitioning(),
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
                withForcedPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                // round robin
                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
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
                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column1", "column2")))));
    }

    @Test
    public void testCreateTableAsSelectWithRequiredPartitioning()
    {
        testCreateTableAsSelectWithRequiredPartitioning(withForcedPreferredPartitioning());
        testCreateTableAsSelectWithRequiredPartitioning(withoutPreferredPartitioning());
    }

    private void testCreateTableAsSelectWithRequiredPartitioning(Session session)
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_required_partitioning (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                session,
                anyTree(
                        node(TableWriterNode.class,
                                exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("column1"),
                                                values("column1", "column2"))
                                                .with(exchangeWithoutSystemPartitioning()))
                                        .with(exchangeWithoutSystemPartitioning()))));
    }

    @Test
    public void testCreateTableAsSelectUnpartitioned()
    {
        assertDistributedPlan(
                "CREATE TABLE new_test_table_unpartitioned (column1, column2) AS SELECT * FROM (VALUES (1, 2)) t(column1, column2)",
                withForcedPreferredPartitioning(),
                anyTree(
                        node(TableWriterNode.class,
                                // round robin
                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of(),
                                        values("column1", "column2")))));
    }

    private Matcher exchangeWithoutSystemPartitioning()
    {
        return new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof ExchangeNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                return new MatchResult(!(((ExchangeNode) node).getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle() instanceof SystemPartitioningHandle));
            }
        };
    }

    private Session withForcedPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "16")
                .setSystemProperty(TASK_WRITER_COUNT, "16")
                .build();
    }

    private Session withoutPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                .setSystemProperty(TASK_WRITER_COUNT, "16")
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "2") // force parallel plan even on test nodes with single CPU
                .build();
    }
}
