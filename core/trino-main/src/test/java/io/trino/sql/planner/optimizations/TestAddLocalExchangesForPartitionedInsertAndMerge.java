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
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.SystemSessionProperties.TASK_WRITER_COUNT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.mergeWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAddLocalExchangesForPartitionedInsertAndMerge
        extends BasePlanTest
{
    private static final PartitioningScheme INSERT_PARTITIONING_SCHEME = new PartitioningScheme(
            Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.of(new Symbol("year"))),
            ImmutableList.of(new Symbol("customer"), new Symbol("year")));
    private static final PartitioningHandle MERGE_PARTITIONING_HANDLE = new PartitioningHandle(
            Optional.empty(),
            Optional.empty(),
            new MergePartitioningHandle(
                    Optional.of(INSERT_PARTITIONING_SCHEME),
                    Optional.empty()));

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("mock_merge_and_insert")
                .setSchema("mock")
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                .setSystemProperty(SCALE_WRITERS, "false")
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog("mock_merge_and_insert", createMergeConnectorFactory(), ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory createMergeConnectorFactory()
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle(((session, schemaTableName) -> {
                    if (schemaTableName.getTableName().equals("source_table")) {
                        return new MockConnectorTableHandle(schemaTableName);
                    }
                    if (schemaTableName.getTableName().equals("target_table")) {
                        return new MockConnectorTableHandle(schemaTableName);
                    }
                    return null;
                }))
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata("customer", INTEGER),
                        new ColumnMetadata("year", INTEGER)))
                .withGetInsertLayout((session, tableName) -> {
                    if (tableName.getTableName().equals("source_table") || tableName.getTableName().equals("target_table")) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("year")));
                    }

                    return Optional.empty();
                })
                .withName("mock_merge_and_insert")
                .build();
    }

    @Test
    public void testTaskWriterCountHasNoEffectOnMergeOperation()
    {
        @Language("SQL") String query = """
                MERGE INTO target_table t USING source_table s
                    ON t.customer = s.customer
                    WHEN MATCHED
                        THEN DELETE
                """;

        assertDistributedPlan(
                query,
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "1")
                        .setSystemProperty(TASK_WRITER_COUNT, "8")
                        .build(),
                anyTree(
                        mergeWriter(
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, MERGE_PARTITIONING_HANDLE,
                                                anyTree(
                                                        node(TableScanNode.class)))))));

        assertDistributedPlan(
                query,
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "4")
                        .setSystemProperty(TASK_WRITER_COUNT, "1")
                        .build(),
                anyTree(
                        mergeWriter(
                                exchange(LOCAL, REPARTITION, MERGE_PARTITIONING_HANDLE,
                                        exchange(REMOTE, REPARTITION, MERGE_PARTITIONING_HANDLE,
                                                anyTree(
                                                        node(TableScanNode.class)))))));
    }

    @Test
    public void testTaskWriterCountHasNoEffectOnPartitionedInsertOperation()
    {
        @Language("SQL") String query = "INSERT INTO target_table SELECT * FROM source_table";

        assertDistributedPlan(
                query,
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "1")
                        .setSystemProperty(TASK_WRITER_COUNT, "8")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        project(
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                        anyTree(
                                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))))));

        assertDistributedPlan(
                query,
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "4")
                        .setSystemProperty(TASK_WRITER_COUNT, "1")
                        .build(),
                anyTree(
                        tableWriter(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                project(
                                        exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                        anyTree(
                                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))))));
    }
}
