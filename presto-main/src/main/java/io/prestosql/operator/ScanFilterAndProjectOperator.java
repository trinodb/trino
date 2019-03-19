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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.CursorProcessorOutput;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;

public class ScanFilterAndProjectOperator
        implements SourceOperator, Closeable
{
    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final SettableFuture<?> blockedOnSplits = SettableFuture.create();

    private WorkProcessor<Page> pages;
    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private Split split;
    private boolean operatorFinishing;

    private long deltaPositionsCount;
    private long deltaPhysicalBytes;
    private long deltaPhysicalReadTimeNanos;
    private long deltaProcessedBytes;

    private ScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            Iterable<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(sourceId, "sourceId is null");

        pages = WorkProcessor.create(
                new SplitToPages(
                        pageSourceProvider,
                        cursorProcessor,
                        pageProcessor,
                        table,
                        columns,
                        types,
                        operatorContext.aggregateSystemMemoryContext(),
                        minOutputPageSize,
                        minOutputPageRowCount))
                .transformProcessor(WorkProcessor::flatten);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");

        if (operatorFinishing) {
            return Optional::empty;
        }

        this.split = split;

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }
        blockedOnSplits.set(null);

        return () -> {
            if (pageSource instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) pageSource);
            }
            return Optional.empty();
        };
    }

    @Override
    public void noMoreSplits()
    {
        blockedOnSplits.set(null);
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

    @Override
    public void finish()
    {
        blockedOnSplits.set(null);
        operatorFinishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return pages.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (pages.isBlocked()) {
            return pages.getBlockedFuture();
        }

        return NOT_BLOCKED;
    }

    @Override
    public final boolean needsInput()
    {
        return false;
    }

    @Override
    public final void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return pages.getResult();
    }

    private class SplitToPages
            implements WorkProcessor.Process<WorkProcessor<Page>>
    {
        final PageSourceProvider pageSourceProvider;
        final CursorProcessor cursorProcessor;
        final PageProcessor pageProcessor;
        final TableHandle table;
        final List<ColumnHandle> columns;
        final List<Type> types;
        final LocalMemoryContext memoryContext;
        final AggregatedMemoryContext localAggregatedMemoryContext;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;
        final DataSize minOutputPageSize;
        final int minOutputPageRowCount;

        boolean finished;

        SplitToPages(
                PageSourceProvider pageSourceProvider,
                CursorProcessor cursorProcessor,
                PageProcessor pageProcessor,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                Iterable<Type> types,
                AggregatedMemoryContext aggregatedMemoryContext,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.pageSourceMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(ScanFilterAndProjectOperator.class.getSimpleName());
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public ProcessState<WorkProcessor<Page>> process()
        {
            if (operatorFinishing || finished) {
                memoryContext.close();
                return ProcessState.finished();
            }

            if (!blockedOnSplits.isDone()) {
                return ProcessState.blocked(blockedOnSplits);
            }

            finished = true;

            ConnectorPageSource source;
            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptySplitPageSource();
            }
            else {
                source = pageSourceProvider.createPageSource(operatorContext.getSession(), split, table, columns);
            }

            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
                return ProcessState.ofResult(processColumnSource());
            }
            else {
                pageSource = source;
                return ProcessState.ofResult(processPageSource());
            }
        }

        WorkProcessor<Page> processColumnSource()
        {
            return WorkProcessor
                    .create(new RecordCursorToPages(cursorProcessor, types, pageSourceMemoryContext, outputMemoryContext))
                    .yielding(() -> operatorContext.getDriverContext().getYieldSignal().isSet())
                    .withProcessStateMonitor(state -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }

        WorkProcessor<Page> processPageSource()
        {
            return WorkProcessor
                    .create(new ConnectorPageSourceToPages(pageSourceMemoryContext))
                    .yielding(() -> operatorContext.getDriverContext().getYieldSignal().isSet())
                    .flatMap(page -> pageProcessor.createWorkProcessor(
                            operatorContext.getSession().toConnectorSession(),
                            operatorContext.getDriverContext().getYieldSignal(),
                            outputMemoryContext,
                            page))
                    .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, localAggregatedMemoryContext))
                    .withProcessStateMonitor(state -> {
                        memoryContext.setBytes(localAggregatedMemoryContext.getBytes());
                        operatorContext.recordPhysicalInputWithTiming(deltaPhysicalBytes, deltaPositionsCount, deltaPhysicalReadTimeNanos);
                        operatorContext.recordProcessedInput(deltaProcessedBytes, deltaPositionsCount);
                        deltaPositionsCount = 0;
                        deltaPhysicalBytes = 0;
                        deltaPhysicalReadTimeNanos = 0;
                        deltaProcessedBytes = 0;
                    });
        }
    }

    private class RecordCursorToPages
            implements WorkProcessor.Process<Page>
    {
        final CursorProcessor cursorProcessor;
        final PageBuilder pageBuilder;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;

        long completedBytes;
        long readTimeNanos;
        boolean finished;

        RecordCursorToPages(CursorProcessor cursorProcessor, List<Type> types, LocalMemoryContext pageSourceMemoryContext, LocalMemoryContext outputMemoryContext)
        {
            this.cursorProcessor = cursorProcessor;
            this.pageBuilder = new PageBuilder(types);
            this.pageSourceMemoryContext = pageSourceMemoryContext;
            this.outputMemoryContext = outputMemoryContext;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (operatorFinishing) {
                finished = true;
            }

            if (!finished) {
                DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
                CursorProcessorOutput output = cursorProcessor.process(operatorContext.getSession().toConnectorSession(), yieldSignal, cursor, pageBuilder);
                pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

                long bytesProcessed = cursor.getCompletedBytes() - completedBytes;
                long elapsedNanos = cursor.getReadTimeNanos() - readTimeNanos;
                operatorContext.recordPhysicalInputWithTiming(bytesProcessed, output.getProcessedRows(), elapsedNanos);
                // TODO: derive better values for cursors
                operatorContext.recordProcessedInput(bytesProcessed, output.getProcessedRows());
                completedBytes = cursor.getCompletedBytes();
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

        long completedBytes;
        long readTimeNanos;

        ConnectorPageSourceToPages(LocalMemoryContext pageSourceMemoryContext)
        {
            this.pageSourceMemoryContext = pageSourceMemoryContext;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (operatorFinishing || pageSource.isFinished()) {
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

            page = recordProcessedInput(page);

            // update operator stats
            long endCompletedBytes = pageSource.getCompletedBytes();
            long endReadTimeNanos = pageSource.getReadTimeNanos();
            deltaPositionsCount += page.getPositionCount();
            deltaPhysicalBytes += endCompletedBytes - completedBytes;
            deltaPhysicalReadTimeNanos += endReadTimeNanos - readTimeNanos;
            completedBytes = endCompletedBytes;
            readTimeNanos = endReadTimeNanos;

            return ProcessState.ofResult(page);
        }
    }

    private Page recordProcessedInput(Page page)
    {
        // account processed bytes from lazy blocks only when they are loaded
        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); ++i) {
            Block block = page.getBlock(i);
            if (block instanceof LazyBlock) {
                LazyBlock delegateLazyBlock = (LazyBlock) block;
                blocks[i] = new LazyBlock(page.getPositionCount(), lazyBlock -> {
                    Block loadedBlock = delegateLazyBlock.getLoadedBlock();
                    deltaProcessedBytes += loadedBlock.getSizeInBytes();
                    lazyBlock.setBlock(loadedBlock);
                });
            }
            else {
                deltaProcessedBytes += block.getSizeInBytes();
                blocks[i] = block;
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Supplier<PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
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
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ScanFilterAndProjectOperator.class.getSimpleName());
            return new ScanFilterAndProjectOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    cursorProcessor.get(),
                    pageProcessor.get(),
                    table,
                    columns,
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
