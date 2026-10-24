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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.memory.MemoryPool;
import io.trino.metadata.Split;
import io.trino.operator.DriverContext;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.TableScanOperator.TableScanOperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.project.InputPageProjection;
import io.trino.operator.project.PageProcessor;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiPageSourceProvider
{
    private static final List<ColumnHandle> COLUMNS = ImmutableList.of(createBaseColumn("_hoodie_record_key", 2, HIVE_STRING, VARCHAR, REGULAR, Optional.empty()));

    @Test
    void testCachedMemoryUsage()
            throws IOException
    {
        testMemoryUsage(true);
    }

    @Test
    void testStreamingMemoryUsage()
            throws IOException
    {
        testMemoryUsage(false);
    }

    private static void testMemoryUsage(boolean cacheFile)
            throws IOException
    {
        AtomicLong memoryUsage = new AtomicLong();
        try (ConnectorPageSource pageSource = createPageSource(cacheFile, memoryUsage::set)) {
            if (cacheFile) {
                assertThat(memoryUsage.get()).isPositive();
            }
            else {
                assertThat(memoryUsage.get()).isZero();
            }

            SourcePage page = pageSource.getNextSourcePage();
            assertThat(page).isNotNull();
            assertThat(page.getPositionCount()).isPositive();
            assertThat(VARCHAR.getSlice(page.getBlock(0), 0).length()).isPositive();
            assertThat(memoryUsage.get()).isPositive();
        }
        assertThat(memoryUsage.get()).isZero();
    }

    @Test
    void testMemoryLimitDuringCreation()
    {
        TrinoException memoryLimitExceeded = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Test memory limit exceeded");
        assertThatThrownBy(() -> createPageSource(true, bytes -> {
            if (bytes > 0) {
                throw memoryLimitExceeded;
            }
        })).isSameAs(memoryLimitExceeded);
    }

    @Test
    void testMemoryLimitDuringRead()
            throws IOException
    {
        TrinoException memoryLimitExceeded = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Test memory limit exceeded");
        try (ConnectorPageSource pageSource = createPageSource(false, bytes -> {
            if (bytes > 0) {
                throw memoryLimitExceeded;
            }
        })) {
            long completedBytes = pageSource.getCompletedBytes();
            assertThatThrownBy(() -> {
                SourcePage page = pageSource.getNextSourcePage();
                assertThat(page).isNotNull();
                page.getBlock(0);
            }).isSameAs(memoryLimitExceeded);
            // Reject the reservation before reading column data, rather than reporting memory after the read returns.
            assertThat(pageSource.getCompletedBytes()).isEqualTo(completedBytes);
        }
    }

    @Test
    void testCachedMemoryReleasedAtEndOfSplit()
            throws IOException
    {
        testMemoryReleasedAtEndOfSplit(true);
    }

    @Test
    void testStreamingMemoryReleasedAtEndOfSplit()
            throws IOException
    {
        testMemoryReleasedAtEndOfSplit(false);
    }

    private static void testMemoryReleasedAtEndOfSplit(boolean cacheFile)
            throws IOException
    {
        AtomicLong memoryUsage = new AtomicLong();
        try (ConnectorPageSource pageSource = createPageSource(cacheFile, memoryUsage::set)) {
            int positions = 0;
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page != null) {
                    page.getBlock(0);
                    positions += page.getPositionCount();
                    assertThat(memoryUsage.get()).isPositive();
                }
            }
            assertThat(positions).isEqualTo(2);
            // Reaching EOF releases the reader's memory without an explicit close from the caller.
            assertThat(memoryUsage.get()).isZero();
        }
        assertThat(memoryUsage.get()).isZero();
    }

    @Test
    void testCachedCloseBeforeRead()
            throws IOException
    {
        testCloseBeforeRead(true);
    }

    @Test
    void testStreamingCloseBeforeRead()
            throws IOException
    {
        testCloseBeforeRead(false);
    }

    private static void testCloseBeforeRead(boolean cacheFile)
            throws IOException
    {
        AtomicLong memoryUsage = new AtomicLong();
        try (ConnectorPageSource pageSource = createPageSource(cacheFile, memoryUsage::set)) {
            pageSource.close();
            assertThat(pageSource.isFinished()).isTrue();
            assertThat(memoryUsage.get()).isZero();
        }
        assertThat(memoryUsage.get()).isZero();
    }

    @Test
    void testTableScanMemoryTracking()
            throws Exception
    {
        testOperatorMemoryTracking(false);
    }

    @Test
    void testScanFilterAndProjectMemoryTracking()
            throws Exception
    {
        testOperatorMemoryTracking(true);
    }

    private static void testOperatorMemoryTracking(boolean scanFilterAndProject)
            throws Exception
    {
        try (ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hudi-memory-test-%s"));
                ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed("hudi-memory-test-scheduled-%s"))) {
            TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);
            DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
            MemoryPool memoryPool = taskContext.getQueryContext().getMemoryPool();
            AtomicLong reportedBytes = new AtomicLong();
            PageSourceProvider pageSourceProvider = (_, _, _, _, _, _, memoryContext) -> {
                try {
                    return createPageSource(false, bytes -> {
                        memoryContext.setBytes(bytes);
                        reportedBytes.set(bytes);
                    });
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            SourceOperatorFactory factory;
            if (scanFilterAndProject) {
                factory = new ScanFilterAndProjectOperatorFactory(
                        0,
                        new PlanNodeId("test"),
                        new PlanNodeId("source"),
                        (_, _) -> pageSourceProvider,
                        _ -> new PageProcessor(Optional.empty(), ImmutableList.of(new InputPageProjection(0))),
                        TEST_TABLE_HANDLE,
                        Optional.empty(),
                        COLUMNS,
                        EMPTY,
                        ImmutableList.of(VARCHAR),
                        DataSize.ofBytes(0),
                        0,
                        newSimpleAggregatedMemoryContext());
            }
            else {
                factory = new TableScanOperatorFactory(
                        0,
                        new PlanNodeId("test"),
                        new PlanNodeId("source"),
                        (_, _) -> pageSourceProvider,
                        TEST_TABLE_HANDLE,
                        Optional.empty(),
                        COLUMNS,
                        ImmutableList.of(VARCHAR),
                        newSimpleAggregatedMemoryContext());
            }
            try (SourceOperator operator = factory.createOperator(driverContext)) {
                operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
                operator.noMoreSplits();
                assertThat(memoryPool.getReservedBytes()).isZero();

                Page page = operator.getOutput();
                assertThat(page).isNotNull();
                assertThat(page.getPositionCount()).isPositive();
                assertThat(reportedBytes.get()).isPositive();
                assertThat(driverContext.getMemoryUsage()).isGreaterThanOrEqualTo(reportedBytes.get());
                assertThat(memoryPool.getReservedBytes()).isEqualTo(driverContext.getMemoryUsage());
            }
            assertThat(reportedBytes.get()).isZero();
            assertThat(memoryPool.getReservedBytes()).isZero();
            factory.noMoreOperators();
        }
    }

    private static ConnectorPageSource createPageSource(boolean cacheFile, MemoryContext memoryContext)
            throws IOException
    {
        ParquetReaderConfig config = new ParquetReaderConfig();
        if (!cacheFile) {
            config.setSmallFileThreshold(DataSize.of(0, BYTE));
        }
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new HudiSessionProperties(new HudiConfig(), config).getSessionProperties())
                .build();
        MemoryFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///test.parquet");
        byte[] data = toByteArray(getResource("hudi-testing-data/hudi_non_part_cow/05b0f4ec-00fb-49f2-a1e2-7f510f3da93b-0_0-27-28_20231127051653361.parquet"));
        fileSystemFactory.create(session).newOutputFile(location).createOrOverwrite(data);
        HudiSplit split = new HudiSplit(
                location.toString(),
                0,
                data.length,
                data.length,
                0,
                TupleDomain.all(),
                ImmutableList.of(),
                SplitWeight.standard(),
                Optional.empty());
        return new HudiPageSourceProvider(fileSystemFactory, new FileFormatDataSourceStats(), config)
                .createPageSource(
                        TEST_TABLE_HANDLE.transaction(),
                        session,
                        split,
                        TEST_TABLE_HANDLE.connectorHandle(),
                        Optional.empty(),
                        COLUMNS,
                        EMPTY,
                        memoryContext);
    }
}
