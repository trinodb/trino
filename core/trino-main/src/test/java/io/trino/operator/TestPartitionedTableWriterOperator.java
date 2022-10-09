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

package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.OutputTableHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.split.PageSinkManager;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.PartitionedTableWriterOperator.PartitionedTableWriterOperatorFactory;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionedTableWriterOperator
        extends BaseTableWriterOperatorTest
{
    private static final int PARTITION_COUNT = 32;

    private PartitionFunctionFactory partitionFunctionFactory;

    @BeforeClass
    public void setUpPartitionFunctionFactory()
    {
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(new FinalizerService())));
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(
                nodeScheduler,
                blockTypeOperators,
                CatalogServiceProvider.fail());
        partitionFunctionFactory = new PartitionFunctionFactory(nodePartitioningManager, blockTypeOperators);
    }

    @Test
    public void testPartitionPhysicalWrittenBytes()
            throws Exception
    {
        TestPageSinkProvider testPageSinkProvider = new TestPageSinkProvider(TestPageSink::new);
        Session session = TEST_SESSION;
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        try (Operator operator = createTableWriterOperator(
                session,
                driverContext,
                testPageSinkProvider,
                new DevNullOperator.DevNullOperatorFactory(1, new PlanNodeId("test")),
                ImmutableList.of(BIGINT, VARBINARY))) {
            operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(1).row(5).row(5).build().get(0));
            operator.addInput(rowPagesBuilder(BIGINT).row(8).row(8).row(9).row(9).row(9).build().get(0));

            Map<Integer, Long> partitionPhysicalWrittenBytes = driverContext.getPartitionPhysicalWrittenBytes();
            long physicalWrittenDataSize = driverContext.getPhysicalWrittenDataSize();

            List<Long> expectedPartitionPhysicalWrittenBytes = testPageSinkProvider.getPageSinks().stream()
                    .map(ConnectorPageSink::getCompletedBytes)
                    .collect(toImmutableList());
            long expectedPhysicalWrittenBytes = expectedPartitionPhysicalWrittenBytes.stream().mapToLong(i -> i).sum();

            List<Long> actualPartitionPhysicalWrittenBytes = partitionPhysicalWrittenBytes.values().stream().collect(toImmutableList());

            // assert that the number of partitions should be 4 since we are writing 4 different values.
            assertThat(partitionPhysicalWrittenBytes.size()).isEqualTo(4);

            assertThat(actualPartitionPhysicalWrittenBytes).containsAll(expectedPartitionPhysicalWrittenBytes);
            assertThat(physicalWrittenDataSize).isEqualTo(expectedPhysicalWrittenBytes);
        }
    }

    @Test
    public void testPagePartitionedIntoPageSinks()
            throws Exception
    {
        TestPageSinkProvider testPageSinkProvider = new TestPageSinkProvider(TestPageSink::new);
        try (Operator operator = createTableWriterOperator(testPageSinkProvider)) {
            operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(1).row(5).row(5).build().get(0));
            operator.addInput(rowPagesBuilder(BIGINT).row(8).row(8).row(9).row(9).row(9).build().get(0));

            List<Page> partitionPages = testPageSinkProvider.getPageSinks().stream()
                    .flatMap(pageSink -> ((TestPageSink) pageSink).getPages().stream())
                    .collect(toImmutableList());

            assertThat(partitionPages.size()).isEqualTo(4);

            List<List<Integer>> partitionValues = partitionPages.stream()
                    .map(page -> {
                        ImmutableList.Builder<Integer> values = ImmutableList.builder();
                        Block block = page.getBlock(0);
                        for (int i = 0; i < block.getPositionCount(); i++) {
                            values.add(block.getInt(i, 0));
                        }
                        return values.build();
                    })
                    .collect(toImmutableList());

            assertThat(partitionValues)
                    .contains(
                            ImmutableList.of(1, 1, 1),
                            ImmutableList.of(5, 5),
                            ImmutableList.of(8, 8),
                            ImmutableList.of(9, 9, 9));
        }
    }

    @Override
    protected AbstractTableWriterOperator createTableWriterOperator(
            Session session,
            DriverContext driverContext,
            BaseTableWriterOperatorTest.TestPageSinkProvider pageSinkProvider,
            OperatorFactory statisticsAggregation,
            List<Type> outputTypes)

    {
        PageSinkManager pageSinkManager = new PageSinkManager(catalogHandle -> {
            checkArgument(catalogHandle.equals(TEST_CATALOG_HANDLE));
            return pageSinkProvider;
        });
        SchemaTableName schemaTableName = new SchemaTableName("testSchema", "testTable");
        PartitionedTableWriterOperatorFactory factory = new PartitionedTableWriterOperatorFactory(
                0,
                new PlanNodeId("test"),
                pageSinkManager,
                new CreateTarget(
                        new OutputTableHandle(
                                TEST_CATALOG_HANDLE,
                                schemaTableName,
                                new ConnectorTransactionHandle() {
                                },
                                new ConnectorOutputTableHandle() {
                                }),
                        schemaTableName,
                        false),
                ImmutableList.of(0),
                session,
                statisticsAggregation,
                outputTypes,
                partitionFunctionFactory,
                FIXED_HASH_DISTRIBUTION,
                ImmutableList.of(0),
                ImmutableList.of(BIGINT),
                PARTITION_COUNT);
        return (AbstractTableWriterOperator) factory.createOperator(driverContext);
    }
}
