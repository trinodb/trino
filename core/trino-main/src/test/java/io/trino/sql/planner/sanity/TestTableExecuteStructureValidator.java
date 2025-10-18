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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.testing.TestingTableExecuteHandle;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTableExecuteStructureValidator
        extends BasePlanTest
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final TableExecuteStructureValidator validator = new TableExecuteStructureValidator();
    private final Expression predicate = new Constant(BIGINT, 1L);
    private final Symbol symbol = new Symbol(BIGINT, "bigint");
    private final List<Symbol> symbols = ImmutableList.of(symbol);
    private final List<String> columnNames = ImmutableList.of("bigint");
    private final Assignments assignments = Assignments.of(symbol, predicate);
    private final Map<Symbol, ColumnHandle> assignmentsMap = ImmutableMap.of(symbol, new ColumnHandle() {});
    private final Optional<OrderingScheme> orderingSchema = Optional.empty();
    private final Optional<PartitioningScheme> partitioningSchema = Optional.empty();
    private final Optional<StatisticAggregations> statisticAggregations = Optional.empty();
    private final Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor = Optional.empty();

    private TableScanNode tableScanNode;
    private TableWriterNode.TableExecuteTarget tableExecuteTarget;
    private PartitioningScheme partitioningScheme;

    @BeforeAll
    void setup()
    {
        CatalogHandle catalogHandle = getCurrentCatalogHandle();

        TableHandle nationTableHandle = new TableHandle(
                catalogHandle,
                new TpchTableHandle("sf1", "nation", 1.0),
                TpchTransactionHandle.INSTANCE);

        tableScanNode = new TableScanNode(
                idAllocator.getNextId(),
                nationTableHandle,
                symbols,
                assignmentsMap,
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());

        tableExecuteTarget = new TableWriterNode.TableExecuteTarget(
                new TableExecuteHandle(
                        TEST_CATALOG_HANDLE,
                        TestingTransactionHandle.create(),
                        new TestingTableExecuteHandle()),
                Optional.empty(),
                new SchemaTableName("schemaName", "tableName"),
                WriterScalingOptions.DISABLED);

        partitioningScheme = new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, symbols), symbols);
    }

    @Test
    void testValidateSuccessfulWithExecuteNode()
    {
        PlanNode root = new OutputNode(idAllocator.getNextId(),
                new ExplainAnalyzeNode(idAllocator.getNextId(),
                        new TableExecuteNode(idAllocator.getNextId(),
                                new ExchangeNode(idAllocator.getNextId(),
                                        REPARTITION,
                                        LOCAL,
                                        partitioningScheme,
                                        ImmutableList.of(new TableFinishNode(idAllocator.getNextId(),
                                                new ExchangeNode(idAllocator.getNextId(),
                                                        REPARTITION,
                                                        LOCAL,
                                                        partitioningScheme,
                                                        ImmutableList.of(
                                                                new ProjectNode(idAllocator.getNextId(),
                                                                        tableScanNode,
                                                                        assignments)),
                                                        ImmutableList.of(symbols),
                                                        orderingSchema),
                                                tableExecuteTarget,
                                                symbol,
                                                statisticAggregations,
                                                statisticsAggregationDescriptor)),
                                        ImmutableList.of(symbols),
                                        orderingSchema),
                                tableExecuteTarget,
                                symbol,
                                symbol,
                                symbols,
                                columnNames,
                                partitioningSchema),
                        symbol,
                        symbols,
                        false),
                columnNames,
                symbols);
        validator.validate(root, null, PLANNER_CONTEXT, WarningCollector.NOOP);
    }

    @Test
    void testValidateSuccessfulWithoutExecuteNode()
    {
        PlanNode root = new OutputNode(idAllocator.getNextId(),
                new ExplainAnalyzeNode(idAllocator.getNextId(),
                        new ExchangeNode(idAllocator.getNextId(),
                                REPARTITION,
                                LOCAL,
                                partitioningScheme,
                                ImmutableList.of(new TableFinishNode(idAllocator.getNextId(),
                                        new ExchangeNode(idAllocator.getNextId(),
                                                REPARTITION,
                                                LOCAL,
                                                partitioningScheme,
                                                ImmutableList.of(
                                                        new ProjectNode(idAllocator.getNextId(),
                                                                tableScanNode,
                                                                assignments)),
                                                ImmutableList.of(symbols),
                                                orderingSchema),
                                        tableExecuteTarget,
                                        symbol,
                                        statisticAggregations,
                                        statisticsAggregationDescriptor)),
                                ImmutableList.of(symbols),
                                orderingSchema),
                        symbol,
                        symbols,
                        false),
                columnNames,
                symbols);
        validator.validate(root, null, PLANNER_CONTEXT, WarningCollector.NOOP);
    }

    @Test
    void testValidateFailed()
    {
        PlanNode root = new OutputNode(idAllocator.getNextId(),
                new ExplainAnalyzeNode(idAllocator.getNextId(),
                        new TableExecuteNode(idAllocator.getNextId(),
                                new ExchangeNode(idAllocator.getNextId(),
                                        REPARTITION,
                                        LOCAL,
                                        partitioningScheme,
                                        ImmutableList.of(new FilterNode(idAllocator.getNextId(),
                                                new ExchangeNode(idAllocator.getNextId(),
                                                        REPARTITION,
                                                        LOCAL,
                                                        partitioningScheme,
                                                        ImmutableList.of(new TableFinishNode(idAllocator.getNextId(),
                                                                new ExchangeNode(idAllocator.getNextId(),
                                                                        REPARTITION,
                                                                        LOCAL,
                                                                        partitioningScheme,
                                                                        ImmutableList.of(
                                                                                new ProjectNode(idAllocator.getNextId(),
                                                                                        tableScanNode,
                                                                                        assignments)),
                                                                        ImmutableList.of(symbols),
                                                                        orderingSchema),
                                                                tableExecuteTarget,
                                                                symbol,
                                                                statisticAggregations,
                                                                statisticsAggregationDescriptor)),
                                                        ImmutableList.of(symbols),
                                                        orderingSchema),
                                                predicate)),
                                        ImmutableList.of(symbols),
                                        orderingSchema),
                                tableExecuteTarget,
                                symbol,
                                symbol,
                                symbols,
                                columnNames,
                                partitioningSchema),
                        symbol,
                        symbols,
                        false),
                columnNames,
                symbols);
        assertThatThrownBy(() -> validator.validate(root, null, PLANNER_CONTEXT, WarningCollector.NOOP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected FilterNode found in plan; probably connector was not able to handle provided WHERE expression");
    }
}
