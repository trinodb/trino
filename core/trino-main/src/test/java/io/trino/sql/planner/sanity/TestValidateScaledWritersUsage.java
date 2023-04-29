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
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValidateScaledWritersUsage
        extends BasePlanTest
{
    private LocalQueryRunner queryRunner;
    private PlannerContext plannerContext;
    private PlanBuilder planBuilder;
    private Symbol symbol;
    private TableScanNode tableScanNode;
    private CatalogHandle catalogSupportingScaledWriters;
    private CatalogHandle catalogNotSupportingScaledWriters;
    private SchemaTableName schemaTableName;

    @BeforeClass
    public void setup()
    {
        schemaTableName = new SchemaTableName("any", "any");
        catalogSupportingScaledWriters = createTestCatalogHandle("bytes_written_reported");
        catalogNotSupportingScaledWriters = createTestCatalogHandle("no_bytes_written_reported");
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.createCatalog(catalogSupportingScaledWriters.getCatalogName(), createConnectorFactorySupportingReportingBytesWritten(true, catalogSupportingScaledWriters.getCatalogName()), ImmutableMap.of());
        queryRunner.createCatalog(catalogNotSupportingScaledWriters.getCatalogName(), createConnectorFactorySupportingReportingBytesWritten(false, catalogNotSupportingScaledWriters.getCatalogName()), ImmutableMap.of());
        plannerContext = queryRunner.getPlannerContext();
        planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), plannerContext.getMetadata(), TEST_SESSION);
        TableHandle nationTableHandle = new TableHandle(
                catalogSupportingScaledWriters,
                new TpchTableHandle("sf1", "nation", 1.0),
                TestingTransactionHandle.create());
        TpchColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        symbol = new Symbol("nationkey");
        tableScanNode = planBuilder.tableScan(nationTableHandle, ImmutableList.of(symbol), ImmutableMap.of(symbol, nationkeyColumnHandle));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        plannerContext = null;
        planBuilder = null;
        tableScanNode = null;
        catalogSupportingScaledWriters = null;
        catalogNotSupportingScaledWriters = null;
    }

    private MockConnectorFactory createConnectorFactorySupportingReportingBytesWritten(boolean supportsWrittenBytes, String name)
    {
        return MockConnectorFactory.builder()
                .withSupportsReportingWrittenBytes(supportsWrittenBytes)
                .withGetTableHandle(((session, schemaTableName) -> null))
                .withName(name)
                .build();
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWritersUsedAndTargetSupportsIt(PartitioningHandle scaledWriterPartitionHandle)
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
                                planBuilder.createTarget(catalogSupportingScaledWriters, schemaTableName, true, true),
                                tableWriterSource,
                                symbol)));
        validatePlan(root);
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWritersUsedAndTargetDoesNotSupportReportingWrittenBytes(PartitioningHandle scaledWriterPartitionHandle)
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
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false, true),
                                tableWriterSource,
                                symbol)));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The scaled writer partitioning scheme is set but writer target no_bytes_written_reported:INSTANCE doesn't support reporting physical written bytes");
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWritersWithMultipleSourceExchangesAndTargetDoesNotSupportReportingWrittenBytes(PartitioningHandle scaledWriterPartitionHandle)
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
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false, true),
                                tableWriterSource,
                                symbol)));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The scaled writer partitioning scheme is set but writer target no_bytes_written_reported:INSTANCE doesn't support reporting physical written bytes");
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWritersWithMultipleSourceExchangesAndTargetSupportIt(PartitioningHandle scaledWriterPartitionHandle)
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
                                planBuilder.createTarget(catalogSupportingScaledWriters, schemaTableName, true, true),
                                tableWriterSource,
                                symbol)));
        validatePlan(root);
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWritersUsedAboveTableWriterInThePlanTree(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false, true),
                                tableWriterSource,
                                symbol)));
        validatePlan(root);
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWritersTwoTableWritersNodes(PartitioningHandle scaledWriterPartitionHandle)
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.tableWriter(
                                ImmutableList.of(symbol),
                                ImmutableList.of("column_a"),
                                Optional.empty(),
                                planBuilder.createTarget(catalogSupportingScaledWriters, schemaTableName, true, true),
                                planBuilder.exchange(innerExchange ->
                                        innerExchange
                                                .partitioningScheme(new PartitioningScheme(Partitioning.create(scaledWriterPartitionHandle, ImmutableList.of()), ImmutableList.of(symbol)))
                                                .addInputsSet(symbol)
                                                .addSource(tableScanNode)),
                                symbol)));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false, true),
                                tableWriterSource,
                                symbol)));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The scaled writer partitioning scheme is set but writer target no_bytes_written_reported:INSTANCE doesn't support reporting physical written bytes");
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWriterUsedAndTargetDoesNotSupportMultipleWritersPerPartition(PartitioningHandle scaledWriterPartitionHandle)
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
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, true, false),
                                tableWriterSource,
                                symbol)));

        if (scaledWriterPartitionHandle == SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) {
            validatePlan(root);
        }
        else {
            assertThatThrownBy(() -> validatePlan(root))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The scaled writer partitioning scheme is set for the partitioned write but writer target no_bytes_written_reported:INSTANCE doesn't support multiple writers per partition");
        }
    }

    @Test(dataProvider = "scaledWriterPartitioningHandles")
    public void testScaledWriterWithMultipleSourceExchangesAndTargetDoesNotSupportMultipleWritersPerPartition(PartitioningHandle scaledWriterPartitionHandle)
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
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, true, false),
                                tableWriterSource,
                                symbol)));

        if (scaledWriterPartitionHandle == SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) {
            validatePlan(root);
        }
        else {
            assertThatThrownBy(() -> validatePlan(root))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The scaled writer partitioning scheme is set for the partitioned write but writer target no_bytes_written_reported:INSTANCE doesn't support multiple writers per partition");
        }
    }

    @DataProvider
    public Object[][] scaledWriterPartitioningHandles()
    {
        return new Object[][] {
                {SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION},
                {SCALED_WRITER_HASH_DISTRIBUTION},
                {new PartitioningHandle(
                        Optional.of(TEST_CATALOG_HANDLE),
                        Optional.of(new ConnectorTransactionHandle() {}),
                        new ConnectorPartitioningHandle() {},
                        true)}
        };
    }

    private void validatePlan(PlanNode root)
    {
        queryRunner.inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            plannerContext.getMetadata().getCatalogHandle(session, catalogSupportingScaledWriters.getCatalogName());
            plannerContext.getMetadata().getCatalogHandle(session, catalogNotSupportingScaledWriters.getCatalogName());
            new ValidateScaledWritersUsage().validate(
                    root,
                    session,
                    plannerContext,
                    createTestingTypeAnalyzer(plannerContext),
                    TypeProvider.empty(),
                    WarningCollector.NOOP);
            return null;
        });
    }
}
