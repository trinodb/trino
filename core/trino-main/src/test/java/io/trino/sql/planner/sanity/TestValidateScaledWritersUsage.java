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
package io.trino.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestValidateScaledWritersUsage
        extends BasePlanTest
{
    private static final PartitioningHandle CUSTOM_HANDLE = new PartitioningHandle(
            Optional.of(TEST_CATALOG_HANDLE),
            Optional.of(new ConnectorTransactionHandle() { }),
            new ConnectorPartitioningHandle() { },
            true);

    private PlanTester planTester;
    private PlannerContext plannerContext;
    private PlanBuilder planBuilder;
    private Symbol symbol;
    private TableScanNode tableScanNode;
    private CatalogHandle catalog;
    private SchemaTableName schemaTableName;

    @BeforeAll
    public void setup()
    {
        schemaTableName = new SchemaTableName("any", "any");
        catalog = createTestCatalogHandle("catalog");
        planTester = PlanTester.create(TEST_SESSION);
        planTester.createCatalog(catalog.getCatalogName(), createConnectorFactory(catalog.getCatalogName()), ImmutableMap.of());
        plannerContext = planTester.getPlannerContext();
        planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), plannerContext, TEST_SESSION);
        TableHandle nationTableHandle = new TableHandle(
                catalog,
                new TpchTableHandle("sf1", "nation", 1.0),
                TestingTransactionHandle.create());
        TpchColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        symbol = new Symbol("nationkey");
        tableScanNode = planBuilder.tableScan(nationTableHandle, ImmutableList.of(symbol), ImmutableMap.of(symbol, nationkeyColumnHandle));
    }

    @AfterAll
    public void tearDown()
    {
        planTester.close();
        planTester = null;
        plannerContext = null;
        planBuilder = null;
        tableScanNode = null;
        catalog = null;
    }

    private MockConnectorFactory createConnectorFactory(String name)
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle((session, schemaTableName) -> null)
                .withName(name)
                .build();
    }

    @Test
    public void testScaledWritersUsedAndTargetSupportsIt()
    {
        testScaledWritersUsedAndTargetSupportsIt(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION);
        testScaledWritersUsedAndTargetSupportsIt(SCALED_WRITER_HASH_DISTRIBUTION);
        testScaledWritersUsedAndTargetSupportsIt(CUSTOM_HANDLE);
    }

    private void testScaledWritersUsedAndTargetSupportsIt(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalog, schemaTableName, true, WriterScalingOptions.ENABLED, false),
                                tableWriterSource,
                                symbol)));
        validatePlan(root);
    }

    @Test
    public void testScaledWritersUsedAndTargetDoesNotSupportScalingPerTask()
    {
        testScaledWritersUsedAndTargetDoesNotSupportScalingPerTask(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION);
        testScaledWritersUsedAndTargetDoesNotSupportScalingPerTask(SCALED_WRITER_HASH_DISTRIBUTION);
        testScaledWritersUsedAndTargetDoesNotSupportScalingPerTask(CUSTOM_HANDLE);
    }

    private void testScaledWritersUsedAndTargetDoesNotSupportScalingPerTask(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .scope(ExchangeNode.Scope.LOCAL)
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalog, schemaTableName, true, new WriterScalingOptions(true, false), false),
                                tableWriterSource,
                                symbol)));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The scaled writer per task partitioning scheme is set but writer target catalog:INSTANCE doesn't support it");
    }

    @Test
    public void testScaledWritersUsedAndTargetDoesNotSupportScalingAcrossTasks()
    {
        testScaledWritersUsedAndTargetDoesNotSupportScalingAcrossTasks(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION);
        testScaledWritersUsedAndTargetDoesNotSupportScalingAcrossTasks(SCALED_WRITER_HASH_DISTRIBUTION);
        testScaledWritersUsedAndTargetDoesNotSupportScalingAcrossTasks(CUSTOM_HANDLE);
    }

    private void testScaledWritersUsedAndTargetDoesNotSupportScalingAcrossTasks(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .scope(ExchangeNode.Scope.REMOTE)
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalog, schemaTableName, true, new WriterScalingOptions(false, true), false),
                                tableWriterSource,
                                symbol)));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The scaled writer across tasks partitioning scheme is set but writer target catalog:INSTANCE doesn't support it");
    }

    @Test
    public void testScaledWriterUsedAndTargetDoesNotSupportMultipleWritersPerPartition()
    {
        testScaledWriterUsedAndTargetDoesNotSupportMultipleWritersPerPartition(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION);
        testScaledWriterUsedAndTargetDoesNotSupportMultipleWritersPerPartition(SCALED_WRITER_HASH_DISTRIBUTION);
        testScaledWriterUsedAndTargetDoesNotSupportMultipleWritersPerPartition(CUSTOM_HANDLE);
    }

    private void testScaledWriterUsedAndTargetDoesNotSupportMultipleWritersPerPartition(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalog, schemaTableName, false, WriterScalingOptions.ENABLED, false),
                                tableWriterSource,
                                symbol)));

        if (scaledWriterPartitionHandle == SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) {
            validatePlan(root);
        }
        else {
            assertThatThrownBy(() -> validatePlan(root))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The hash scaled writer partitioning scheme is set for the partitioned write but writer target catalog:INSTANCE doesn't support multiple writers per partition");
        }
    }

    @Test
    public void testScaledWriterWithMultipleSourceExchangesAndTargetDoesNotSupportMultipleWritersPerPartition()
    {
        testScaledWriterWithMultipleSourceExchangesAndTargetDoesNotSupportMultipleWritersPerPartition(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION);
        testScaledWriterWithMultipleSourceExchangesAndTargetDoesNotSupportMultipleWritersPerPartition(SCALED_WRITER_HASH_DISTRIBUTION);
        testScaledWriterWithMultipleSourceExchangesAndTargetDoesNotSupportMultipleWritersPerPartition(CUSTOM_HANDLE);
    }

    private void testScaledWriterWithMultipleSourceExchangesAndTargetDoesNotSupportMultipleWritersPerPartition(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol, symbol)))
                        .addInputsSet(symbol, symbol)
                        .addInputsSet(symbol, symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode)))
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalog, schemaTableName, false, WriterScalingOptions.ENABLED, false),
                                tableWriterSource,
                                symbol)));

        if (scaledWriterPartitionHandle == SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) {
            validatePlan(root);
        }
        else {
            assertThatThrownBy(() -> validatePlan(root))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The hash scaled writer partitioning scheme is set for the partitioned write but writer target catalog:INSTANCE doesn't support multiple writers per partition");
        }
    }

    private void validatePlan(PlanNode root)
    {
        planTester.inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            plannerContext.getMetadata().getCatalogHandle(session, catalog.getCatalogName());
            new ValidateScaledWritersUsage().validate(
                    root,
                    session,
                    plannerContext,
                    new IrTypeAnalyzer(plannerContext),
                    TypeProvider.empty(),
                    WarningCollector.NOOP);
            return null;
        });
    }
}
