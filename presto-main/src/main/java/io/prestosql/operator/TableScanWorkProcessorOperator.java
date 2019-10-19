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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.split.PageSourceProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.operator.PageUtils.recordMaterializedBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TableScanWorkProcessorOperator
        implements WorkProcessorSourceOperator
{
    private final WorkProcessor<Page> pages;
    private final SplitToPages splitToPages;

    public TableScanWorkProcessorOperator(
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Split> splits,
            PageSourceProvider pageSourceProvider,
            TableHandle table,
            Iterable<ColumnHandle> columns)
    {
        this.splitToPages = new SplitToPages(
                session,
                pageSourceProvider,
                table,
                columns,
                memoryTrackingContext.aggregateSystemMemoryContext());
        this.pages = splits.flatTransform(splitToPages);
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return splitToPages.getUpdatablePageSourceSupplier();
    }

    @Override
    public DataSize getPhysicalInputDataSize()
    {
        return splitToPages.getPhysicalInputDataSize();
    }

    @Override
    public long getPhysicalInputPositions()
    {
        return splitToPages.getPhysicalInputPositions();
    }

    @Override
    public DataSize getInputDataSize()
    {
        return splitToPages.getInputDataSize();
    }

    @Override
    public long getInputPositions()
    {
        return splitToPages.getInputPositions();
    }

    @Override
    public Duration getReadTime()
    {
        return splitToPages.getReadTime();
    }

    @Override
    public void close()
            throws Exception
    {
        splitToPages.close();
    }

    private static class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final Session session;
        final PageSourceProvider pageSourceProvider;
        final TableHandle table;
        final List<ColumnHandle> columns;
        final AggregatedMemoryContext aggregatedMemoryContext;

        long processedBytes;
        long processedPositions;

        ConnectorPageSource source;

        SplitToPages(
                Session session,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                AggregatedMemoryContext aggregatedMemoryContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.aggregatedMemoryContext = requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                return TransformationState.finished();
            }

            checkState(source == null, "Table scan split already set");
            source = pageSourceProvider.createPageSource(session, split, table, columns, null);
            return TransformationState.ofResult(
                    WorkProcessor.create(new ConnectorPageSourceToPages(aggregatedMemoryContext, source))
                            .map(page -> {
                                processedPositions += page.getPositionCount();
                                recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);
                                return page;
                            }));
        }

        Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
        {
            return () -> {
                if (source instanceof UpdatablePageSource) {
                    return Optional.of((UpdatablePageSource) source);
                }
                return Optional.empty();
            };
        }

        DataSize getPhysicalInputDataSize()
        {
            if (source == null) {
                return new DataSize(0, BYTE);
            }

            return new DataSize(source.getCompletedBytes(), BYTE);
        }

        long getPhysicalInputPositions()
        {
            return processedPositions;
        }

        DataSize getInputDataSize()
        {
            return new DataSize(processedBytes, BYTE);
        }

        long getInputPositions()
        {
            return processedPositions;
        }

        Duration getReadTime()
        {
            if (source == null) {
                return new Duration(0, NANOSECONDS);
            }

            return new Duration(source.getReadTimeNanos(), NANOSECONDS);
        }

        void close()
        {
            if (source != null) {
                try {
                    source.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final ConnectorPageSource pageSource;
        final LocalMemoryContext memoryContext;

        ConnectorPageSourceToPages(AggregatedMemoryContext aggregatedMemoryContext, ConnectorPageSource pageSource)
        {
            this.pageSource = pageSource;
            this.memoryContext = aggregatedMemoryContext
                    .newLocalMemoryContext(TableScanWorkProcessorOperator.class.getSimpleName());
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                memoryContext.close();
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(toListenableFuture(isBlocked));
            }

            Page page = pageSource.getNextPage();
            memoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    memoryContext.close();
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yield();
                }
            }

            // TODO: report operator stats
            return ProcessState.ofResult(page);
        }
    }
}
