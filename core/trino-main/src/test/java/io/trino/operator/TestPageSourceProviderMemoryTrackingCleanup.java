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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestPageSourceProviderMemoryTrackingCleanup
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    void testTableScanUntracksMemoryWhenPageSourceCloseFails()
    {
        TrackingPageSourceProvider pageSourceProvider = new TrackingPageSourceProvider();
        PlanNodeId sourceId = new PlanNodeId("source");
        DriverContext driverContext = newDriverContext();
        TableScanOperator operator = new TableScanOperator(
                driverContext.addOperatorContext(0, new PlanNodeId("test"), "test"),
                sourceId,
                pageSourceProvider,
                TEST_TABLE_HANDLE,
                Optional.empty(),
                ImmutableList.of());

        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        assertThat(operator.getOutput()).isNull();
        assertThat(pageSourceProvider.pageSourceCreated).isTrue();

        assertThatThrownBy(operator::finish)
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("close failed");
        pageSourceProvider.assertMemoryUntracked();
    }

    @Test
    void testScanFilterAndProjectUntracksMemoryWhenPageSourceCloseFails()
    {
        TrackingPageSourceProvider pageSourceProvider = new TrackingPageSourceProvider();
        ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("source"),
                _ -> pageSourceProvider,
                _ -> new PageProcessor(Optional.empty(), ImmutableList.of()),
                TEST_TABLE_HANDLE,
                Optional.empty(),
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(),
                DataSize.ofBytes(0),
                0);
        SourceOperator operator = factory.createOperator(newDriverContext());

        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();
        assertThat(operator.getOutput()).isNull();
        assertThat(pageSourceProvider.pageSourceCreated).isTrue();

        assertThatThrownBy(operator::close)
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("close failed");
        pageSourceProvider.assertMemoryUntracked();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static final class TrackingPageSourceProvider
            implements PageSourceProvider
    {
        private final AtomicReference<MemoryContext> trackedMemoryContext = new AtomicReference<>();
        private final AtomicReference<MemoryContext> untrackedMemoryContext = new AtomicReference<>();
        private final AtomicInteger untrackCalls = new AtomicInteger();
        private final AtomicBoolean pageSourceCreated = new AtomicBoolean();

        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                MemoryContext memoryContext)
        {
            pageSourceCreated.set(true);
            return new CloseFailingPageSource();
        }

        @Override
        public void trackMemoryUsage(MemoryContext memoryContext)
        {
            assertThat(trackedMemoryContext.compareAndSet(null, memoryContext)).isTrue();
        }

        @Override
        public void untrackMemoryUsage(MemoryContext memoryContext)
        {
            untrackedMemoryContext.set(memoryContext);
            untrackCalls.incrementAndGet();
        }

        private void assertMemoryUntracked()
        {
            assertThat(untrackCalls).hasValue(1);
            assertThat(untrackedMemoryContext.get()).isSameAs(trackedMemoryContext.get());
        }
    }

    private static final class CloseFailingPageSource
            implements ConnectorPageSource
    {
        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            return null;
        }

        @Override
        public void close()
                throws IOException
        {
            throw new IOException("close failed");
        }
    }
}
