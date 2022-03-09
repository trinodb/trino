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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.split.EmptySplit;
import io.trino.split.PageSourceProvider;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.PageUtils.recordMaterializedBytes;
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
            Iterable<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        this.splitToPages = new SplitToPages(
                session,
                pageSourceProvider,
                table,
                columns,
                dynamicFilter,
                memoryTrackingContext.aggregateUserMemoryContext());
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
    public long getDynamicFilterSplitsProcessed()
    {
        return splitToPages.getDynamicFilterSplitsProcessed();
    }

    @Override
    public Metrics getConnectorMetrics()
    {
        return splitToPages.source.getMetrics();
    }

    @Override
    public Duration getReadTime()
    {
        return splitToPages.getReadTime();
    }

    @Override
    public void close()
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
        final DynamicFilter dynamicFilter;
        final LocalMemoryContext memoryContext;

        long processedBytes;
        long processedPositions;
        long dynamicFilterSplitsProcessed;

        @Nullable
        ConnectorPageSource source;

        SplitToPages(
                Session session,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                AggregatedMemoryContext aggregatedMemoryContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.memoryContext = requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null")
                    .newLocalMemoryContext(TableScanWorkProcessorOperator.class.getSimpleName());
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return TransformationState.finished();
            }

            if (!dynamicFilter.getCurrentPredicate().isAll()) {
                dynamicFilterSplitsProcessed++;
            }

            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptyPageSource();
            }
            else {
                source = pageSourceProvider.createPageSource(session, split, table, columns, dynamicFilter);
            }

            return TransformationState.ofResult(
                    WorkProcessor.create(new ConnectorPageSourceToPages(source))
                            .map(page -> {
                                processedPositions += page.getPositionCount();
                                recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);
                                return page;
                            })
                            .blocking(() -> memoryContext.setBytes(source.getMemoryUsage())));
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
                return DataSize.ofBytes(0);
            }

            return DataSize.ofBytes(source.getCompletedBytes());
        }

        long getPhysicalInputPositions()
        {
            if (source == null) {
                return 0;
            }
            return source.getCompletedPositions().orElse(processedPositions);
        }

        DataSize getInputDataSize()
        {
            return DataSize.ofBytes(processedBytes);
        }

        long getInputPositions()
        {
            return processedPositions;
        }

        long getDynamicFilterSplitsProcessed()
        {
            return dynamicFilterSplitsProcessed;
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

        ConnectorPageSourceToPages(ConnectorPageSource pageSource)
        {
            this.pageSource = pageSource;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(asVoid(toListenableFuture(isBlocked)));
            }

            Page page = pageSource.getNextPage();

            if (page == null) {
                if (pageSource.isFinished()) {
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yielded();
                }
            }

            return ProcessState.ofResult(page);
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }
}
