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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.split.EmptySplit;
import io.trino.split.PageSourceProvider;
import io.trino.split.PageSourceProviderFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.cache.CacheDriverContext.getDynamicFilter;
import static java.util.Objects.requireNonNull;

public class TableScanOperator
        implements SourceOperator
{
    public static class TableScanOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final DynamicFilter dynamicFilter;
        private boolean closed;

        public TableScanOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanNodeId sourceId,
                PageSourceProviderFactory pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.pageSourceProvider = pageSourceProvider.createPageSourceProvider(table.catalogHandle());
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TableScanOperator.class.getSimpleName());
            return new TableScanOperator(
                    operatorContext,
                    sourceId,
                    pageSourceProvider,
                    table,
                    columns,
                    getDynamicFilter(operatorContext, dynamicFilter));
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final PageSourceProvider pageSourceProvider;
    private final TableHandle table;
    private final List<ColumnHandle> columns;
    private final DynamicFilter dynamicFilter;
    private final LocalMemoryContext memoryContext;
    private final SettableFuture<Void> blocked = SettableFuture.create();

    @Nullable
    private Split split;
    @Nullable
    private ConnectorPageSource source;

    private boolean finished;

    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    public TableScanOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PageSourceProvider pageSourceProvider,
            TableHandle table,
            Iterable<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "planNodeId is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.memoryContext = operatorContext.newLocalUserMemoryContext(TableScanOperator.class.getSimpleName());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public void addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.split == null, "Table scan split already set");

        if (finished) {
            return;
        }

        this.split = split;

        Map<String, String> splitInfo = split.getInfo();
        if (!splitInfo.isEmpty()) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(split.getCatalogHandle(), splitInfo)));
        }

        blocked.set(null);

        if (split.getConnectorSplit() instanceof EmptySplit) {
            source = new EmptyPageSource();
        }
    }

    @Override
    public void noMoreSplits()
    {
        if (split == null) {
            finished = true;
        }
        blocked.set(null);
    }

    @Override
    public void close()
    {
        finish();
    }

    @Override
    public void finish()
    {
        finished = true;
        blocked.set(null);

        if (source != null) {
            try {
                source.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            memoryContext.setBytes(source.getMemoryUsage());
            operatorContext.setLatestConnectorMetrics(source.getMetrics());
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!finished) {
            finished = (source != null) && source.isFinished();
            if (source != null) {
                memoryContext.setBytes(source.getMemoryUsage());
            }
        }

        return finished;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (!blocked.isDone()) {
            return blocked;
        }
        if (source != null) {
            CompletableFuture<?> pageSourceBlocked = source.isBlocked();
            return pageSourceBlocked.isDone() ? NOT_BLOCKED : asVoid(toListenableFuture(pageSourceBlocked));
        }
        return NOT_BLOCKED;
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }

    @Override
    public Page getOutput()
    {
        if (split == null) {
            return null;
        }
        if (source == null) {
            if (!dynamicFilter.getCurrentPredicate().isAll()) {
                operatorContext.recordDynamicFilterSplitProcessed(1L);
            }
            source = pageSourceProvider.createPageSource(operatorContext.getSession(), split, table, columns, dynamicFilter);
        }

        Page page = source.getNextPage();
        if (page != null) {
            // assure the page is in memory before handing to another operator
            page = page.getLoadedPage();

            // update operator stats
            long endCompletedBytes = source.getCompletedBytes();
            long endReadTimeNanos = source.getReadTimeNanos();
            long positionCount = page.getPositionCount();
            long endCompletedPositions = source.getCompletedPositions().orElse(completedPositions + positionCount);
            operatorContext.recordPhysicalInputWithTiming(
                    endCompletedBytes - completedBytes,
                    endCompletedPositions - completedPositions,
                    endReadTimeNanos - readTimeNanos);
            operatorContext.recordProcessedInput(page.getSizeInBytes(), positionCount);
            completedBytes = endCompletedBytes;
            completedPositions = endCompletedPositions;
            readTimeNanos = endReadTimeNanos;
        }

        // updating memory usage should happen after page is loaded.
        memoryContext.setBytes(source.getMemoryUsage());
        operatorContext.setLatestConnectorMetrics(source.getMetrics());
        return page;
    }
}
