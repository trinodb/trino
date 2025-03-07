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
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.CursorProcessorOutput;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.split.EmptySplit;
import io.trino.split.PageSourceProvider;
import io.trino.split.PageSourceProviderFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ScanFilterAndProjectOperator
        implements WorkProcessorSourceOperator
{
    private final WorkProcessor<Page> pages;
    private final PageProcessorMetrics pageProcessorMetrics = new PageProcessorMetrics();

    @Nullable
    private RecordCursor cursor;
    @Nullable
    private ConnectorPageSource pageSource;

    private long processedPositions;
    private long processedBytes;
    private long physicalBytes;
    private long physicalPositions;
    private long readTimeNanos;
    private long dynamicFilterSplitsProcessed;
    private Metrics metrics = Metrics.EMPTY;

    private ScanFilterAndProjectOperator(
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            DriverYieldSignal yieldSignal,
            WorkProcessor<Split> split,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            Iterable<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        pages = split.flatTransform(
                new SplitToPages(
                        session,
                        yieldSignal,
                        pageSourceProvider,
                        cursorProcessor,
                        pageProcessor,
                        table,
                        columns,
                        dynamicFilter,
                        types,
                        memoryTrackingContext.aggregateUserMemoryContext(),
                        minOutputPageSize,
                        minOutputPageRowCount));
    }

    @Override
    public DataSize getPhysicalInputDataSize()
    {
        return DataSize.ofBytes(physicalBytes);
    }

    @Override
    public long getPhysicalInputPositions()
    {
        return physicalPositions;
    }

    @Override
    public DataSize getInputDataSize()
    {
        return DataSize.ofBytes(processedBytes);
    }

    @Override
    public long getInputPositions()
    {
        return processedPositions;
    }

    @Override
    public Duration getReadTime()
    {
        return new Duration(readTimeNanos, NANOSECONDS);
    }

    @Override
    public long getDynamicFilterSplitsProcessed()
    {
        return dynamicFilterSplitsProcessed;
    }

    @Override
    public Metrics getConnectorMetrics()
    {
        return metrics;
    }

    @Override
    public Metrics getMetrics()
    {
        if (cursor != null) {
            return Metrics.EMPTY;
        }
        return pageProcessorMetrics.getMetrics();
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public void close()
    {
        if (pageSource != null) {
            try {
                pageSource.close();
                metrics = pageSource.getMetrics();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else if (cursor != null) {
            cursor.close();
        }
    }

    private class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final Session session;
        final DriverYieldSignal yieldSignal;
        final PageSourceProvider pageSourceProvider;
        final CursorProcessor cursorProcessor;
        final PageProcessor pageProcessor;
        final TableHandle table;
        final List<ColumnHandle> columns;
        final DynamicFilter dynamicFilter;
        final List<Type> types;
        final LocalMemoryContext memoryContext;
        final AggregatedMemoryContext localAggregatedMemoryContext;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;
        final DataSize minOutputPageSize;
        final int minOutputPageRowCount;

        SplitToPages(
                Session session,
                DriverYieldSignal yieldSignal,
                PageSourceProvider pageSourceProvider,
                CursorProcessor cursorProcessor,
                PageProcessor pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                Iterable<Type> types,
                AggregatedMemoryContext aggregatedMemoryContext,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.session = requireNonNull(session, "session is null");
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.pageSourceMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return finished();
            }

            checkState(cursor == null && pageSource == null, "Table scan split already set");

            if (!dynamicFilter.getCurrentPredicate().isAll()) {
                dynamicFilterSplitsProcessed++;
            }

            ConnectorPageSource source;
            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptyPageSource();
            }
            else {
                source = pageSourceProvider.createPageSource(session, split, table, columns, dynamicFilter);
            }

            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
                return ofResult(processColumnSource());
            }
            pageSource = source;
            return ofResult(processPageSource());
        }

        WorkProcessor<Page> processColumnSource()
        {
            return WorkProcessor
                    .create(new RecordCursorToPages(session, yieldSignal, cursorProcessor, types, pageSourceMemoryContext, outputMemoryContext))
                    .yielding(yieldSignal::isSet)
                    .blocking(() -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }

        WorkProcessor<Page> processPageSource()
        {
            ConnectorSession connectorSession = session.toConnectorSession();
            return WorkProcessor
                    .create(new ConnectorPageSourceToPages(pageSourceMemoryContext))
                    .yielding(yieldSignal::isSet)
                    .flatMap(page -> {
                        WorkProcessor<Page> workProcessor = pageProcessor.createWorkProcessor(
                                connectorSession,
                                yieldSignal,
                                outputMemoryContext,
                                pageProcessorMetrics,
                                page);
                        // Note this is monitoring the original source page not the result page
                        return workProcessor.withProcessStateMonitor(new ProcessedBytesMonitor(page, bytes -> processedBytes += bytes));
                    })
                    .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, localAggregatedMemoryContext))
                    .blocking(() -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }
    }

    private class RecordCursorToPages
            implements WorkProcessor.Process<Page>
    {
        final ConnectorSession session;
        final DriverYieldSignal yieldSignal;
        final CursorProcessor cursorProcessor;
        final PageBuilder pageBuilder;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;

        boolean finished;

        RecordCursorToPages(
                Session session,
                DriverYieldSignal yieldSignal,
                CursorProcessor cursorProcessor,
                List<Type> types,
                LocalMemoryContext pageSourceMemoryContext,
                LocalMemoryContext outputMemoryContext)
        {
            this.session = session.toConnectorSession();
            this.yieldSignal = yieldSignal;
            this.cursorProcessor = cursorProcessor;
            this.pageBuilder = new PageBuilder(types);
            this.pageSourceMemoryContext = pageSourceMemoryContext;
            this.outputMemoryContext = outputMemoryContext;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (!finished) {
                CursorProcessorOutput output = cursorProcessor.process(session, yieldSignal, cursor, pageBuilder);
                pageSourceMemoryContext.setBytes(cursor.getMemoryUsage());

                processedPositions += output.getProcessedRows();
                // TODO: derive better values for cursors
                processedBytes = cursor.getCompletedBytes();
                physicalBytes = cursor.getCompletedBytes();
                physicalPositions = processedPositions;
                readTimeNanos = cursor.getReadTimeNanos();
                if (output.isNoMoreRows()) {
                    finished = true;
                }
            }

            if (pageBuilder.isFull() || (finished && !pageBuilder.isEmpty())) {
                // only return a page if buffer is full or cursor has finished
                Page page = pageBuilder.build();
                pageBuilder.reset();
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ProcessState.ofResult(page);
            }
            if (finished) {
                checkState(pageBuilder.isEmpty());
                return ProcessState.finished();
            }
            outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            return ProcessState.yielded();
        }
    }

    static class ProcessedBytesMonitor
            implements Consumer<ProcessState<Page>>
    {
        private final SourcePage page;
        private final LongConsumer processedBytesConsumer;
        private long localProcessedBytes;

        public ProcessedBytesMonitor(SourcePage page, LongConsumer processedBytesConsumer)
        {
            this.page = requireNonNull(page, "page is null");
            this.processedBytesConsumer = requireNonNull(processedBytesConsumer, "processedBytesConsumer is null");
            localProcessedBytes = page.getSizeInBytes();
            processedBytesConsumer.accept(localProcessedBytes);
        }

        @Override
        public void accept(ProcessState<Page> state)
        {
            update();
        }

        void update()
        {
            long newProcessedBytes = page.getSizeInBytes();
            processedBytesConsumer.accept(newProcessedBytes - localProcessedBytes);
            localProcessedBytes = newProcessedBytes;
        }
    }

    private class ConnectorPageSourceToPages
            implements WorkProcessor.Process<SourcePage>
    {
        final LocalMemoryContext pageSourceMemoryContext;

        ConnectorPageSourceToPages(LocalMemoryContext pageSourceMemoryContext)
        {
            this.pageSourceMemoryContext = pageSourceMemoryContext;
        }

        @Override
        public ProcessState<SourcePage> process()
        {
            if (pageSource.isFinished()) {
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(asVoid(toListenableFuture(isBlocked)));
            }

            SourcePage page = pageSource.getNextSourcePage();
            pageSourceMemoryContext.setBytes(pageSource.getMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    return ProcessState.finished();
                }
                return ProcessState.yielded();
            }

            // update operator stats
            processedPositions += page.getPositionCount();
            physicalBytes = pageSource.getCompletedBytes();
            physicalPositions = pageSource.getCompletedPositions().orElse(processedPositions);
            readTimeNanos = pageSource.getReadTimeNanos();
            metrics = pageSource.getMetrics();

            return ProcessState.ofResult(page);
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory, WorkProcessorSourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Function<DynamicFilter, PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final DynamicFilter dynamicFilter;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProviderFactory pageSourceProvider,
                Supplier<CursorProcessor> cursorProcessor,
                Function<DynamicFilter, PageProcessor> pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = dynamicFilter;
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.pageSourceProvider = pageSourceProvider.createPageSourceProvider(table.catalogHandle());
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        @Override
        public String getOperatorType()
        {
            return ScanFilterAndProjectOperator.class.getSimpleName();
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, getOperatorType());
            return new WorkProcessorSourceOperatorAdapter(operatorContext, this);
        }

        @Override
        public WorkProcessorSourceOperator create(
                OperatorContext operatorContext,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> split)
        {
            return new ScanFilterAndProjectOperator(
                    operatorContext.getSession(),
                    memoryTrackingContext,
                    yieldSignal,
                    split,
                    pageSourceProvider,
                    cursorProcessor.get(),
                    pageProcessor.apply(dynamicFilter),
                    table,
                    columns,
                    dynamicFilter,
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
