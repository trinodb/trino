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
import io.trino.connector.CatalogHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Partitioning;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValidateScaledWritersUsage
        extends BasePlanTest
{
    private PlannerContext plannerContext;
    private PlanBuilder planBuilder;
    private Symbol symbol;
    private TableScanNode tableScanNode;
    private CatalogHandle catalogSupportingScaledWriters;
    private CatalogHandle catalogNotSupportingScaledWriters;
    private LocalQueryRunner queryRunner;
    private SchemaTableName schemaTableName;

    @BeforeClass
    public void setup()
    {
        schemaTableName = new SchemaTableName("any", "any");
        catalogSupportingScaledWriters = createRootCatalogHandle("bytes_written_reported");
        catalogNotSupportingScaledWriters = createRootCatalogHandle("no_bytes_written_reported");
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

    private MockConnectorFactory createConnectorFactorySupportingReportingBytesWritten(boolean supportsWrittenBytes, String name)
    {
        return MockConnectorFactory.builder()
                .withSupportsReportingWrittenBytes(supportsWrittenBytes)
                .withGetTableHandle(((session, schemaTableName) -> null))
                .withName(name)
                .build();
    }

    @Test
    public void testScaledWritersUsedAndTargetSupportsIt()
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalogSupportingScaledWriters, schemaTableName, true),
                                tableWriterSource,
                                symbol,
                                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))));
        validatePlan(root);
    }

    @Test
    public void testScaledWritersUsedAndTargetDoesNotSupportIt()
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                                        .addInputsSet(symbol)
                                        .addSource(tableScanNode))));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false),
                                tableWriterSource,
                                symbol,
                                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The partitioning scheme is set to SCALED_WRITER_DISTRIBUTION but writer target no_bytes_written_reported:INSTANCE does support for it");
    }

    @Test
    public void testScaledWritersUsedAndTargetDoesNotSupportItMultipleSourceExchanges()
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol, symbol)))
                        .addInputsSet(symbol, symbol)
                        .addInputsSet(symbol, symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
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
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false),
                                tableWriterSource,
                                symbol,
                                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The partitioning scheme is set to SCALED_WRITER_DISTRIBUTION but writer target no_bytes_written_reported:INSTANCE does support for it");
    }

    @Test
    public void testScaledWritersUsedAndTargetSupportsItMultipleSourceExchanges()
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol, symbol)))
                        .addInputsSet(symbol, symbol)
                        .addInputsSet(symbol, symbol)
                        .addSource(planBuilder.exchange(innerExchange ->
                                innerExchange
                                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
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
                                planBuilder.createTarget(catalogSupportingScaledWriters, schemaTableName, true),
                                tableWriterSource,
                                symbol,
                                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))));
        validatePlan(root);
    }

    @Test
    public void testScaledWritersUsedAboveTableWriterInThePlanTree()
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
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false),
                                tableWriterSource,
                                symbol,
                                new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))));
        validatePlan(root);
    }

    @Test
    public void testScaledWritersTwoTableWritersNodes()
    {
        PlanNode tableWriterSource = planBuilder.exchange(ex ->
                ex
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                        .addInputsSet(symbol)
                        .addSource(planBuilder.tableWriter(
                                ImmutableList.of(symbol),
                                ImmutableList.of("column_a"),
                                Optional.empty(),
                                Optional.empty(),
                                planBuilder.createTarget(catalogSupportingScaledWriters, schemaTableName, true),
                                planBuilder.exchange(innerExchange ->
                                        innerExchange
                                                .partitioningScheme(new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))
                                                .addInputsSet(symbol)
                                                .addSource(tableScanNode)),
                                symbol)));
        PlanNode root = planBuilder.output(
                outputBuilder -> outputBuilder
                        .source(planBuilder.tableWithExchangeCreate(
                                planBuilder.createTarget(catalogNotSupportingScaledWriters, schemaTableName, false),
                                tableWriterSource,
                                symbol,
                                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)))));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The partitioning scheme is set to SCALED_WRITER_DISTRIBUTION but writer target no_bytes_written_reported:INSTANCE does support for it");
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
