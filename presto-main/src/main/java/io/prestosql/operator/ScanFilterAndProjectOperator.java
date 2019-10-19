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
import io.prestosql.operator.WorkProcessorSourceOperatorAdapter.AdapterWorkProcessorSourceOperatorFactory;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.CursorProcessorOutput;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.PageUtils.recordMaterializedBytes;
import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static io.prestosql.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ScanFilterAndProjectOperator
        implements WorkProcessorSourceOperator
{
    private final WorkProcessor<Page> pages;

    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private long processedPositions;
    private long processedBytes;
    private long physicalBytes;
    private long readTimeNanos;

    private ScanFilterAndProjectOperator(
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            DriverYieldSignal yieldSignal,
            WorkProcessor<Split> splits,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Supplier<TupleDomain<ColumnHandle>> dynamicFilter,
            Iterable<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount,
            boolean avoidPageMaterialization)
    {
        pages = splits.flatTransform(
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
                        requireNonNull(memoryTrackingContext, "memoryTrackingContext is null").aggregateSystemMemoryContext(),
                        minOutputPageSize,
                        minOutputPageRowCount,
                        avoidPageMaterialization));
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return () -> {
            if (pageSource instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) pageSource);
            }
            return Optional.empty();
        };
    }

    @Override
    public DataSize getPhysicalInputDataSize()
    {
        return new DataSize(physicalBytes, BYTE);
    }

    @Override
    public long getPhysicalInputPositions()
    {
        return processedPositions;
    }

    @Override
    public DataSize getInputDataSize()
    {
        return new DataSize(processedBytes, BYTE);
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
        final Supplier<TupleDomain<ColumnHandle>> dynamicFilter;
        final List<Type> types;
        final LocalMemoryContext memoryContext;
        final AggregatedMemoryContext localAggregatedMemoryContext;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;
        final DataSize minOutputPageSize;
        final int minOutputPageRowCount;
        final boolean avoidPageMaterialization;

        SplitToPages(
                Session session,
                DriverYieldSignal yieldSignal,
                PageSourceProvider pageSourceProvider,
                CursorProcessor cursorProcessor,
                PageProcessor pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                Supplier<TupleDomain<ColumnHandle>> dynamicFilter,
                Iterable<Type> types,
                AggregatedMemoryContext aggregatedMemoryContext,
                DataSize minOutputPageSize,
                int minOutputPageRowCount,
                boolean avoidPageMaterialization)
        {
            this.session = requireNonNull(session, "session is null");
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = dynamicFilter;
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.pageSourceMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.avoidPageMaterialization = avoidPageMaterialization;
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return finished();
            }

            checkState(cursor == null && pageSource == null, "Table scan split already set");

            ConnectorPageSource source;
            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptySplitPageSource();
            }
            else {
                source = pageSourceProvider.createPageSource(session, split, table, columns, dynamicFilter);
            }

            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
                return ofResult(processColumnSource());
            }
            else {
                pageSource = source;
                return ofResult(processPageSource());
            }
        }

        WorkProcessor<Page> processColumnSource()
        {
            return WorkProcessor
                    .create(new RecordCursorToPages(session, yieldSignal, cursorProcessor, types, pageSourceMemoryContext, outputMemoryContext))
                    .yielding(yieldSignal::isSet)
                    .withProcessStateMonitor(state -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }

        WorkProcessor<Page> processPageSource()
        {
            return WorkProcessor
                    .create(new ConnectorPageSourceToPages(pageSourceMemoryContext))
                    .yielding(yieldSignal::isSet)
                    .flatMap(page -> pageProcessor.createWorkProcessor(
                            session.toConnectorSession(),
                            yieldSignal,
                            outputMemoryContext,
                            page,
                            avoidPageMaterialization))
                    .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, localAggregatedMemoryContext))
                    .withProcessStateMonitor(state -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }
    }

    private class RecordCursorToPages
            implements WorkProcessor.Process<Page>
    {
        final Session session;
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
            this.session = session;
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
                CursorProcessorOutput output = cursorProcessor.process(session.toConnectorSession(), yieldSignal, cursor, pageBuilder);
                pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

                processedPositions += output.getProcessedRows();
                // TODO: derive better values for cursors
                processedBytes = cursor.getCompletedBytes();
                physicalBytes = cursor.getCompletedBytes();
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
            else if (finished) {
                checkState(pageBuilder.isEmpty());
                return ProcessState.finished();
            }
            else {
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ProcessState.yield();
            }
        }
    }

    private class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final LocalMemoryContext pageSourceMemoryContext;

        ConnectorPageSourceToPages(LocalMemoryContext pageSourceMemoryContext)
        {
            this.pageSourceMemoryContext = pageSourceMemoryContext;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(toListenableFuture(isBlocked));
            }

            Page page = pageSource.getNextPage();
            pageSourceMemoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yield();
                }
            }

            recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);

            // update operator stats
            processedPositions += page.getPositionCount();
            physicalBytes = pageSource.getCompletedBytes();
            readTimeNanos = pageSource.getReadTimeNanos();

            return ProcessState.ofResult(page);
        }
    }

    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory, AdapterWorkProcessorSourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Supplier<PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final Supplier<TupleDomain<ColumnHandle>> dynamicFilter;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProvider pageSourceProvider,
                Supplier<CursorProcessor> cursorProcessor,
                Supplier<PageProcessor> pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                Supplier<TupleDomain<ColumnHandle>> dynamicFilter,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = dynamicFilter;
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
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
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> splits)
        {
            return create(session, memoryTrackingContext, yieldSignal, splits, true);
        }

        public WorkProcessorSourceOperator createAdapterOperator(Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> splits)
        {
            return create(session, memoryTrackingContext, yieldSignal, splits, false);
        }

        private ScanFilterAndProjectOperator create(
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> splits,
                boolean avoidPageMaterialization)
        {
            return new ScanFilterAndProjectOperator(
                    session,
                    memoryTrackingContext,
                    yieldSignal,
                    splits,
                    pageSourceProvider,
                    cursorProcessor.get(),
                    pageProcessor.get(),
                    table,
                    columns,
                    dynamicFilter,
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount,
                    avoidPageMaterialization);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
