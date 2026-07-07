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
import com.google.common.collect.ImmutableMultimap;
import io.airlift.slice.Slice;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.MergeHandle;
import io.trino.operator.MergeWriterOperator.MergeWriterOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.split.PageSinkManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode.MergeParadigmAndTypes;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class TestMergeWriterOperator
{
    private static final long FINISH_MEMORY_USAGE = 12345;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMergeSinkMemoryUsageIsReported()
            throws Exception
    {
        MemoryReportingMergeSinkProvider mergeSinkProvider = new MemoryReportingMergeSinkProvider();
        PageSinkManager pageSinkManager = new PageSinkManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, mergeSinkProvider));
        MergeWriterOperatorFactory factory = new MergeWriterOperatorFactory(
                0,
                new PlanNodeId("test"),
                pageSinkManager,
                mergeTarget(),
                Optional.empty(),
                TEST_SESSION,
                Function.identity());

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        Operator operator = factory.createOperator(driverContext);
        OperatorContext operatorContext = operator.getOperatorContext();

        assertThat(operatorContext.getOperatorMemoryContext().getUserMemory()).isEqualTo(0);

        // memory reported by the merge sink while storing pages must be reflected in the operator memory context
        operator.addInput(createMergePage(42));
        long reportedMemory = mergeSinkProvider.sink().reportedMemory();
        assertThat(reportedMemory).isGreaterThan(0);
        assertThat(operatorContext.getOperatorMemoryContext().getUserMemory()).isEqualTo(reportedMemory);

        // memory reported from within ConnectorMergeSink.finish() must also be reflected
        operator.finish();
        assertThat(operatorContext.getOperatorMemoryContext().getUserMemory()).isEqualTo(FINISH_MEMORY_USAGE);
        assertThat(operator.getOutput()).isNotNull();
        assertThat(operator.isFinished()).isTrue();

        // closing the operator releases the memory
        operator.close();
        assertThat(operatorContext.getOperatorMemoryContext().getUserMemory()).isEqualTo(0);
    }

    private static MergeTarget mergeTarget()
    {
        ConnectorMergeTableHandle mergeTableHandle = new ConnectorMergeTableHandle()
        {
            @Override
            public ConnectorTableHandle getTableHandle()
            {
                return TEST_TABLE_HANDLE.connectorHandle();
            }
        };
        return new MergeTarget(
                TEST_TABLE_HANDLE,
                Optional.of(new MergeHandle(TEST_TABLE_HANDLE, mergeTableHandle)),
                new SchemaTableName("testSchema", "testTable"),
                new MergeParadigmAndTypes(Optional.of(DELETE_ROW_AND_INSERT_ROW), ImmutableList.of(BIGINT), ImmutableList.of("column"), BIGINT),
                ImmutableList.of(),
                ImmutableMultimap.of());
    }

    private static Page createMergePage(long value)
    {
        BlockBuilder dataColumn = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(dataColumn, value);
        BlockBuilder insertFromUpdateColumn = TINYINT.createFixedSizeBlockBuilder(1);
        TINYINT.writeLong(insertFromUpdateColumn, 0);
        return new Page(dataColumn.build(), insertFromUpdateColumn.build());
    }

    private static class MemoryReportingMergeSinkProvider
            implements ConnectorPageSinkProvider
    {
        private MemoryReportingMergeSink sink;

        public MemoryReportingMergeSink sink()
        {
            return requireNonNull(sink, "merge sink not created yet");
        }

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorOutputTableHandle outputTableHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorInsertTableHandle insertTableHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorMergeSink createMergeSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorMergeTableHandle mergeHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId,
                MemoryContext memoryContext)
        {
            sink = new MemoryReportingMergeSink(memoryContext);
            return sink;
        }
    }

    private static class MemoryReportingMergeSink
            implements ConnectorMergeSink
    {
        private final MemoryContext memoryContext;
        private long reportedMemory;

        public MemoryReportingMergeSink(MemoryContext memoryContext)
        {
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        }

        public long reportedMemory()
        {
            return reportedMemory;
        }

        @Override
        public void storeMergedRows(Page page)
        {
            reportedMemory += page.getRetainedSizeInBytes();
            memoryContext.setBytes(reportedMemory);
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            // simulates memory used while producing the merge result, e.g. by a file reader
            memoryContext.setBytes(FINISH_MEMORY_USAGE);
            return completedFuture(ImmutableList.of());
        }
    }
}
