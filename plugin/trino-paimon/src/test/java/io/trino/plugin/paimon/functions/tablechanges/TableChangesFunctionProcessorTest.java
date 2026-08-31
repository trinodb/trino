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
package io.trino.plugin.paimon.functions.tablechanges;

import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.paimon.PaimonColumnHandle;
import io.trino.plugin.paimon.PaimonMetadataFactory;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.plugin.paimon.functions.PaimonFunctionProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

public class TableChangesFunctionProcessorTest
{
    @Test
    public void testProjectedColumnsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> new TableChangesFunctionProcessor(
                SESSION,
                handle,
                new PaimonSplit("split", 1.0),
                pageSourceProvider()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table_changes requires explicit projected columns");
    }

    @Test
    public void testProjectedColumnsRejectMalformedEntries()
    {
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(
                SESSION,
                malformedProjectedColumnsHandle(Collections.singletonList(null)),
                new PaimonSplit("split", 1.0),
                pageSourceProvider()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedColumns contains null column");

        ColumnHandle wrongColumn = new ColumnHandle() {};
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(
                SESSION,
                malformedProjectedColumnsHandle(List.of(wrongColumn)),
                new PaimonSplit("split", 1.0),
                pageSourceProvider()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon table_changes requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    @Test
    public void testConstructorArgumentsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        PaimonSplit split = new PaimonSplit("split", 1.0);
        PaimonPageSourceProvider pageSourceProvider = pageSourceProvider();

        assertThatThrownBy(() -> new TableChangesFunctionProcessor(null, handle, split, pageSourceProvider))
                .hasMessage("session is null");
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(SESSION, null, split, pageSourceProvider))
                .hasMessage("handle is null");
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(SESSION, handle, null, pageSourceProvider))
                .hasMessage("split is null");
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(SESSION, handle, split, null))
                .hasMessage("pageSourceProvider is null");
    }

    @Test
    public void testProviderArgumentsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of()),
                Optional.empty(),
                OptionalLong.empty());
        PaimonSplit split = new PaimonSplit("split", 1.0);
        TableChangesFunctionProcessorProvider provider = new TableChangesFunctionProcessorProvider(pageSourceProvider());

        assertThatThrownBy(() -> new TableChangesFunctionProcessorProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pageSourceProvider is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(null, handle, Optional.empty(), split))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, null, Optional.empty(), split))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("handle is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, handle, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("split is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, new TestingTableFunctionHandle(), Optional.empty(), split))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("handle must be PaimonTableHandle");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, handle, Optional.empty(), new TestingConnectorSplit()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("split must be PaimonSplit");
    }

    @Test
    public void testFunctionProviderArgumentsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of()),
                Optional.empty(),
                OptionalLong.empty());
        PaimonFunctionProvider provider = new PaimonFunctionProvider(
                new TableChangesFunctionProcessorProvider(pageSourceProvider()));

        assertThatThrownBy(() -> new PaimonFunctionProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableChangesFunctionProcessorProvider is null");
        assertThatThrownBy(() -> provider.getTableFunctionProcessorProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("functionHandle is null");
        assertThatThrownBy(() -> provider.getTableFunctionProcessorProvider(new TestingTableFunctionHandle()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("functionHandle must be PaimonTableHandle");

        assertThat(provider.getTableFunctionProcessorProvider(handle))
                .isNotNull();
    }

    @Test
    public void testProcessorReturnsBlockedWhenPageSourceHasNoPage()
    {
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        TestingPageSource pageSource = new TestingPageSource(blocked);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        TableFunctionProcessorState state = processor.process();

        assertThat(state).isInstanceOfSatisfying(TableFunctionProcessorState.Blocked.class, blockedState -> {
            assertThat(blockedState.getFuture()).isNotDone();
            blocked.complete(null);
            assertThat(blockedState.getFuture()).isDone();
        });
    }

    @Test
    public void testProcessorReturnsFinishedWhenPageSourceFinishesAfterNullPage()
    {
        FinishingAfterNullPageSource pageSource = new FinishingAfterNullPageSource();
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        assertThat(processor.process()).isEqualTo(TableFunctionProcessorState.Finished.FINISHED);
        assertThat(pageSource.closed()).isTrue();
    }

    @Test
    public void testProcessorClosesAlreadyFinishedPageSource()
    {
        CloseTrackingPageSource pageSource = new CloseTrackingPageSource(true);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        assertThat(processor.process()).isEqualTo(TableFunctionProcessorState.Finished.FINISHED);
        assertThat(pageSource.closed()).isTrue();
    }

    @Test
    public void testProcessorMapsTerminalCloseFailureToConnectorReadError()
    {
        CloseFailurePageSource pageSource = new CloseFailurePageSource(true, true);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        assertThatThrownBy(processor::process)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon table_changes page source");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("close failure");
                });
        assertThat(pageSource.closeCount()).isEqualTo(1);
    }

    @Test
    public void testProcessorRetriesTerminalCloseFailure()
    {
        CloseFailurePageSource pageSource = new CloseFailurePageSource(true, 1);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        Throwable firstFailure = catchThrowable(processor::process);

        assertThat(firstFailure)
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode()));
        assertThat(processor.process()).isEqualTo(TableFunctionProcessorState.Finished.FINISHED);
        assertThat(processor.process()).isEqualTo(TableFunctionProcessorState.Finished.FINISHED);
        assertThat(pageSource.closeCount()).isEqualTo(2);
    }

    @Test
    public void testProcessorMapsTerminalRuntimeCloseFailureToConnectorReadError()
    {
        RuntimeException closeFailure = new IllegalStateException("runtime close failure");
        CloseFailurePageSource pageSource = new CloseFailurePageSource(true, closeFailure);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        assertThatThrownBy(processor::process)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon table_changes page source");
                    assertThat(exception.getCause()).isSameAs(closeFailure);
                });
        assertThat(pageSource.closeCount()).isEqualTo(1);
    }

    @Test
    public void testProcessorClosesPageSourceWhenReadFails()
    {
        CloseFailurePageSource pageSource = new CloseFailurePageSource(false, true, false);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        assertThatThrownBy(processor::process)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("read failure");
        assertThat(pageSource.closed()).isTrue();
    }

    @Test
    public void testProcessorSuppressesCloseFailureOntoReadFailure()
    {
        CloseFailurePageSource pageSource = new CloseFailurePageSource(false, true, true);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        assertThatThrownBy(processor::process)
                .isInstanceOfSatisfying(IllegalStateException.class, exception -> {
                    assertThat(exception).hasMessage("read failure");
                    assertThat(exception.getSuppressed())
                            .singleElement()
                            .isInstanceOfSatisfying(IOException.class, suppressed ->
                                    assertThat(suppressed).hasMessage("close failure"));
                });
        assertThat(pageSource.closed()).isTrue();
    }

    private static PaimonPageSourceProvider pageSourceProvider()
    {
        return pageSourceProvider(null);
    }

    private static PaimonPageSourceProvider pageSourceProvider(ConnectorPageSource pageSource)
    {
        return new PaimonPageSourceProvider(
                _ -> {
                    throw new UnsupportedOperationException("filesystem is not used by this test");
                },
                new PaimonMetadataFactory(
                        new Options(),
                        _ -> {
                            throw new UnsupportedOperationException("filesystem is not used by this test");
                        },
                        TESTING_TYPE_MANAGER),
                new OrcReaderConfig(),
                new ParquetReaderConfig())
        {
            @Override
            public ConnectorPageSource createPageSource(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorSplit split,
                    ConnectorTableHandle tableHandle,
                    Optional<ConnectorTableCredentials> tableCredentials,
                    List<ColumnHandle> columns,
                    DynamicFilter dynamicFilter,
                    MemoryContext memoryContext)
            {
                if (pageSource == null) {
                    return super.createPageSource(transaction, session, split, tableHandle, tableCredentials, columns, dynamicFilter, memoryContext);
                }
                return pageSource;
            }
        };
    }

    private static PaimonTableHandle handleWithProjectedColumns()
    {
        return new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of(PaimonColumnHandle.of("id", DataTypes.INT()))),
                Optional.empty(),
                OptionalLong.empty());
    }

    private static PaimonTableHandle malformedProjectedColumnsHandle(List<?> projectedColumns)
    {
        return new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
        {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Optional<List<PaimonColumnHandle>> getProjectedColumns()
            {
                return (Optional) Optional.of(projectedColumns);
            }
        };
    }

    private record TestingTableFunctionHandle()
            implements ConnectorTableFunctionHandle {}

    private record TestingConnectorSplit()
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return List.of();
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return SplitWeight.standard();
        }
    }

    private record TestingPageSource(CompletableFuture<?> blocked)
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
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {}

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return blocked;
        }
    }

    private static final class FinishingAfterNullPageSource
            implements ConnectorPageSource
    {
        private boolean finished;
        private boolean closed;

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
            return finished;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            finished = true;
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            closed = true;
        }

        private boolean closed()
        {
            return closed;
        }
    }

    private static final class CloseTrackingPageSource
            implements ConnectorPageSource
    {
        private final boolean finished;
        private final AtomicBoolean closed = new AtomicBoolean();

        private CloseTrackingPageSource(boolean finished)
        {
            this.finished = finished;
        }

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
            return finished;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
        {
            closed.set(true);
        }

        private boolean closed()
        {
            return closed.get();
        }
    }

    private static final class CloseFailurePageSource
            implements ConnectorPageSource
    {
        private final boolean finished;
        private final boolean failOnRead;
        private final int closeFailures;
        private final RuntimeException runtimeCloseFailure;
        private final AtomicInteger closeCount = new AtomicInteger();

        private CloseFailurePageSource(boolean finished, boolean failOnClose)
        {
            this(finished, false, failOnClose ? Integer.MAX_VALUE : 0, null);
        }

        private CloseFailurePageSource(boolean finished, int closeFailures)
        {
            this(finished, false, closeFailures, null);
        }

        private CloseFailurePageSource(boolean finished, RuntimeException runtimeCloseFailure)
        {
            this(finished, false, 0, runtimeCloseFailure);
        }

        private CloseFailurePageSource(boolean finished, boolean failOnRead, boolean failOnClose)
        {
            this(finished, failOnRead, failOnClose ? Integer.MAX_VALUE : 0, null);
        }

        private CloseFailurePageSource(
                boolean finished,
                boolean failOnRead,
                int closeFailures,
                RuntimeException runtimeCloseFailure)
        {
            this.finished = finished;
            this.failOnRead = failOnRead;
            this.closeFailures = closeFailures;
            this.runtimeCloseFailure = runtimeCloseFailure;
        }

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
            return finished;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            if (failOnRead) {
                throw new IllegalStateException("read failure");
            }
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            int closeAttempt = closeCount.incrementAndGet();
            if (runtimeCloseFailure != null) {
                throw runtimeCloseFailure;
            }
            if (closeAttempt <= closeFailures) {
                throw new IOException("close failure");
            }
        }

        private boolean closed()
        {
            return closeCount.get() > 0;
        }

        private int closeCount()
        {
            return closeCount.get();
        }
    }
}
