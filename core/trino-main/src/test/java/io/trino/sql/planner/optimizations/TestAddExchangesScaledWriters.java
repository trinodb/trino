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
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.mergeWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableExecute;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAddExchangesScaledWriters
        extends BasePlanTest
{
    private static final PartitioningScheme INSERT_PARTITIONING_SCHEME = new PartitioningScheme(
            Partitioning.create(
                    // Scale writers is disabled for optimize command
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.of(new Symbol(INTEGER, "year"))),
            ImmutableList.of(new Symbol(INTEGER, "customer"), new Symbol(INTEGER, "year")));
    private static final PartitioningHandle MERGE_PARTITIONING_HANDLE = new PartitioningHandle(
            Optional.empty(),
            Optional.empty(),
            new MergePartitioningHandle(
                    Optional.of(INSERT_PARTITIONING_SCHEME),
                    Optional.empty()),
            // Scaled writers is disabled for merge
            false);

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        PlanTester planTester = PlanTester.create(session);
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        planTester.createCatalog("catalog_with_scaled_writers", createConnectorFactory("catalog_with_scaled_writers", true), ImmutableMap.of());
        planTester.createCatalog("catalog_without_scaled_writers", createConnectorFactory("catalog_without_scaled_writers", false), ImmutableMap.of());
        planTester.createCatalog("mock_merge_and_insert", createMergeConnectorFactory(), ImmutableMap.of());
        return planTester;
    }

    private MockConnectorFactory createMergeConnectorFactory()
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle((session, schemaTableName) -> {
                    if (schemaTableName.getTableName().equals("source_table")) {
                        return new MockConnectorTableHandle(schemaTableName);
                    }
                    if (schemaTableName.getTableName().equals("target_table")) {
                        return new MockConnectorTableHandle(schemaTableName);
                    }
                    return null;
                })
                .withGetLayoutForTableExecute((session, tableHandle) -> {
                    MockConnector.MockConnectorTableExecuteHandle tableExecuteHandle = (MockConnector.MockConnectorTableExecuteHandle) tableHandle;
                    if (tableExecuteHandle.getSchemaTableName().getTableName().equals("target_table")) {
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
                    if (tableName.getTableName().equals("source_table") || tableName.getTableName().equals("target_table")) {
                        return Optional.of(new ConnectorTableLayout(ImmutableList.of("year")));
                    }

                    return Optional.empty();
                })
                .withWriterScalingOptions(new WriterScalingOptions(true, true))
                .withName("mock_merge_and_insert")
                .build();
    }

    private MockConnectorFactory createConnectorFactory(String name, boolean writerScalingEnabledAcrossTasks)
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle((session, schemaTableName) -> null)
                .withName(name)
                .withWriterScalingOptions(new WriterScalingOptions(writerScalingEnabledAcrossTasks, true))
                .build();
    }

    @Test
    public void testScaledWriters()
    {
        for (boolean isScaleWritersEnabled : Arrays.asList(true, false)) {
            Session session = testSessionBuilder()
                    .setSystemProperty("scale_writers", Boolean.toString(isScaleWritersEnabled))
                    .build();

            @Language("SQL")
            String query = "CREATE TABLE catalog_with_scaled_writers.mock.test AS SELECT * FROM tpch.tiny.nation";
            SubPlan subPlan = subplan(query, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, session);
            if (isScaleWritersEnabled) {
                assertThat(subPlan.getAllFragments().get(1).getPartitioning().getConnectorHandle()).isEqualTo(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION.getConnectorHandle());
            }
            else {
                subPlan.getAllFragments().forEach(
                        fragment -> assertThat(fragment.getPartitioning().getConnectorHandle()).isNotEqualTo(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION.getConnectorHandle()));
            }
        }
    }

    @Test
    public void testScaledWritersWithTasksScalingDisabled()
    {
        for (boolean isScaleWritersEnabled : Arrays.asList(true, false)) {
            Session session = testSessionBuilder()
                    .setSystemProperty("scale_writers", Boolean.toString(isScaleWritersEnabled))
                    .build();

            @Language("SQL")
            String query = "CREATE TABLE catalog_without_scaled_writers.mock.test AS SELECT * FROM tpch.tiny.nation";
            SubPlan subPlan = subplan(query, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, session);
            subPlan.getAllFragments().forEach(
                    fragment -> assertThat(fragment.getPartitioning().getConnectorHandle()).isNotEqualTo(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION.getConnectorHandle()));
        }
    }

    @Test
    public void testScaleWritersDisabledForMerge()
    {
        @Language("SQL") String query = """
                MERGE INTO target_table t USING source_table s
                    ON t.customer = s.customer
                    WHEN MATCHED
                        THEN DELETE
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setCatalog("mock_merge_and_insert")
                        .setSchema("mock")
                        .setSystemProperty(SCALE_WRITERS, "true")
                        .setSystemProperty(TASK_MAX_WRITER_COUNT, "1")
                        .build(),
                anyTree(
                        mergeWriter(
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, MERGE_PARTITIONING_HANDLE,
                                                anyTree(
                                                        node(TableScanNode.class)))))));

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setCatalog("mock_merge_and_insert")
                        .setSchema("mock")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .setSystemProperty(TASK_MAX_WRITER_COUNT, "1")
                        .build(),
                anyTree(
                        mergeWriter(
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, MERGE_PARTITIONING_HANDLE,
                                                anyTree(
                                                        node(TableScanNode.class)))))));
    }

    @Test
    public void testScaleWritersDisabledForOptimizeOnPartitionedTable()
    {
        assertDistributedPlan(
                "ALTER TABLE target_table EXECUTE OPTIMIZE(file_size_threshold => '10MB')",
                Session.builder(getPlanTester().getDefaultSession())
                        .setCatalog("mock_merge_and_insert")
                        .setSchema("mock")
                        .setSystemProperty(SCALE_WRITERS, "true")
                        .build(),
                anyTree(
                        tableExecute(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                tableScan("target_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

        assertDistributedPlan(
                "ALTER TABLE target_table EXECUTE OPTIMIZE(file_size_threshold => '10MB')",
                Session.builder(getPlanTester().getDefaultSession())
                        .setCatalog("mock_merge_and_insert")
                        .setSchema("mock")
                        .setSystemProperty(SCALE_WRITERS, "false")
                        .build(),
                anyTree(
                        tableExecute(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                tableScan("target_table", ImmutableMap.of("customer", "customer", "year", "year")))))));
    }

    @Test
    public void testScaleWritersEnabledForOptimizeOnUnPartitionedTable() {
        assertDistributedPlan(
                "ALTER TABLE source_table EXECUTE OPTIMIZE(file_size_threshold => '10MB')",
                Session.builder(getPlanTester().getDefaultSession())
                        .setCatalog("mock_merge_and_insert")
                        .setSchema("mock")
                        .setSystemProperty(SCALE_WRITERS, "true")
                        .build(),
                anyTree(
                        tableExecute(
                                ImmutableList.of("customer", "year"),
                                ImmutableList.of("customer", "year"),
                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                        exchange(REMOTE, REPARTITION, SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION,
                                                tableScan("source_table", ImmutableMap.of("customer", "customer", "year", "year")))))));

    }
}
