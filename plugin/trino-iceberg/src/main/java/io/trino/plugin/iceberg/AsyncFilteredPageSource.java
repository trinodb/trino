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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.iceberg.delete.PageFilter;
import io.trino.plugin.iceberg.delete.ThreadlessExecutor;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.util.Objects.requireNonNull;

/**
 * Takes the load of the delete page filter, started when the split is opened, then applies the filter to every non-empty page.
 * The first non-empty page is held back and the driver waits in a blocked state until the delete page filter is loaded.
 * Delete files are decoded on the thread reading the split, via {@code decodeExecutor}, as they are opened rather than all at once.
 * If the split finishes without producing a row or is closed early, the load is canceled.
 */
public final class AsyncFilteredPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final ThreadlessExecutor decodeExecutor;
    private final LocalMemoryContext bufferedPageMemoryContext;

    private State state;

    public AsyncFilteredPageSource(
            ConnectorPageSource delegate,
            ThreadlessExecutor decodeExecutor,
            LocalMemoryContext bufferedPageMemoryContext,
            ListenableFuture<Optional<PageFilter>> pendingLoad)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.decodeExecutor = requireNonNull(decodeExecutor, "decodeExecutor is null");
        this.bufferedPageMemoryContext = requireNonNull(bufferedPageMemoryContext, "bufferedPageMemoryContext is null");
        this.state = new State.Loading(requireNonNull(pendingLoad, "pendingLoad is null"));
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !(state instanceof State.WaitingForFilter) && delegate.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        return switch (state) {
            case State.Loading(ListenableFuture<Optional<PageFilter>> pendingLoad) -> {
                runPendingDecodes();
                SourcePage page = delegate.getNextSourcePage();
                if (page == null || page.getPositionCount() == 0) {
                    if (delegate.isFinished()) {
                        cancel();
                    }
                    yield page;
                }
                bufferedPageMemoryContext.setBytes(page.getRetainedSizeInBytes());
                state = new State.WaitingForFilter(pendingLoad, page);
                yield getNextSourcePage();
            }
            case State.WaitingForFilter(ListenableFuture<Optional<PageFilter>> pendingLoad, SourcePage bufferedPage) -> {
                runPendingDecodes();
                if (!pendingLoad.isDone()) {
                    yield null;
                }
                Optional<PageFilter> pageFilter = getFutureValue(pendingLoad);
                state = new State.Filtering(pageFilter);
                bufferedPageMemoryContext.setBytes(0);
                applyFilter(pageFilter, bufferedPage);
                yield bufferedPage;
            }
            case State.Filtering(Optional<PageFilter> pageFilter) -> {
                SourcePage page = delegate.getNextSourcePage();
                if (page != null) {
                    applyFilter(pageFilter, page);
                }
                yield page;
            }
            case State.Canceled _ -> null;
        };
    }

    /**
     * Decode the delete files opened so far instead of waiting until all deletes are loaded to do decoding.
     * Helps release buffers early to avoid excessive memory usage.
     */
    private void runPendingDecodes()
    {
        try {
            decodeExecutor.runPending();
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_BAD_DATA, e);
        }
    }

    private void cancel()
    {
        switch (state) {
            case State.Loading(ListenableFuture<Optional<PageFilter>> pendingLoad) -> pendingLoad.cancel(true);
            case State.WaitingForFilter(ListenableFuture<Optional<PageFilter>> pendingLoad, _) -> pendingLoad.cancel(true);
            case State.Filtering _, State.Canceled _ -> {}
        }
        state = State.CANCELED;
        decodeExecutor.close();
    }

    private static void applyFilter(Optional<PageFilter> pageFilter, SourcePage page)
    {
        try {
            pageFilter.ifPresent(filter -> filter.applyFilter(page));
        }
        catch (RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ICEBERG_BAD_DATA, e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        cancel();
        bufferedPageMemoryContext.close();
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (state instanceof State.WaitingForFilter(ListenableFuture<Optional<PageFilter>> pendingLoad, _) && !pendingLoad.isDone()) {
            return toCompletableFuture(whenAnyComplete(ImmutableList.of(decodeExecutor.taskAvailable(), pendingLoad)));
        }
        return delegate.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    private sealed interface State
    {
        State CANCELED = new Canceled();

        // The load is started and no rows have been seen, so empty pages pass through
        record Loading(ListenableFuture<Optional<PageFilter>> pendingLoad)
                implements State
        {
            public Loading
            {
                requireNonNull(pendingLoad, "pendingLoad is null");
            }
        }

        // bufferedPage holds the first non-empty page until pendingLoad completes
        record WaitingForFilter(ListenableFuture<Optional<PageFilter>> pendingLoad, SourcePage bufferedPage)
                implements State
        {
            public WaitingForFilter
            {
                requireNonNull(pendingLoad, "pendingLoad is null");
                requireNonNull(bufferedPage, "bufferedPage is null");
            }
        }

        // The filter is loaded and applied to every page
        record Filtering(Optional<PageFilter> pageFilter)
                implements State
        {
            public Filtering
            {
                requireNonNull(pageFilter, "pageFilter is null");
            }
        }

        // The split finished without producing rows or was closed and the load was canceled
        record Canceled()
                implements State {}
    }
}
