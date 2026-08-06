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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.plugin.iceberg.delete.PageFilter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.util.Objects.requireNonNull;

/**
 * Starts loading the delete page filter when the split is opened then applies the filter to every non-empty page.
 * The first non-empty page is held back and the driver waits in a blocked state until the delete page filter is loaded.
 * <p>
 * If the split finishes without producing a row, or is closed early, the load is canceled.
 */
public final class AsyncFilteredPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;

    private State state = State.LOADING;
    private ListenableFuture<Optional<PageFilter>> pendingLoad;
    private SourcePage bufferedPage;
    private Optional<PageFilter> pageFilter;

    public AsyncFilteredPageSource(
            ConnectorPageSource delegate,
            Supplier<ListenableFuture<Optional<PageFilter>>> pageFilterLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.pendingLoad = startLoad(requireNonNull(pageFilterLoader, "pageFilterLoader is null"));
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
        return bufferedPage == null && delegate.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        return switch (state) {
            case LOADING -> {
                SourcePage page = delegate.getNextSourcePage();
                if (page == null || page.getPositionCount() == 0) {
                    if (delegate.isFinished()) {
                        cancelPendingLoad();
                        state = State.CANCELED;
                    }
                    yield page;
                }
                bufferedPage = page;
                state = State.WAITING_FOR_FILTER;
                yield getNextSourcePage();
            }
            case WAITING_FOR_FILTER -> {
                requireNonNull(pendingLoad, "pendingLoad must be present in WAITING_FOR_FILTER state");
                if (!pendingLoad.isDone()) {
                    yield null;
                }
                pageFilter = getFutureValue(pendingLoad);
                pendingLoad = null;
                state = State.FILTERING;
                yield getNextSourcePage();
            }
            case FILTERING -> {
                SourcePage page;
                if (bufferedPage != null) {
                    page = bufferedPage;
                    bufferedPage = null;
                }
                else {
                    page = delegate.getNextSourcePage();
                }
                if (page == null) {
                    yield null;
                }
                applyFilter(page);
                yield page;
            }
            case CANCELED -> null;
        };
    }

    private static ListenableFuture<Optional<PageFilter>> startLoad(Supplier<ListenableFuture<Optional<PageFilter>>> pageFilterLoader)
    {
        try {
            return requireNonNull(pageFilterLoader.get(), "pageFilterLoader returned null");
        }
        catch (TrinoException e) {
            return immediateFailedFuture(e);
        }
        catch (RuntimeException e) {
            return immediateFailedFuture(new TrinoException(ICEBERG_BAD_DATA, e));
        }
    }

    private void cancelPendingLoad()
    {
        if (pendingLoad != null) {
            pendingLoad.cancel(true);
            pendingLoad = null;
        }
    }

    private void applyFilter(SourcePage page)
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
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage() + (bufferedPage == null ? 0 : bufferedPage.getRetainedSizeInBytes());
    }

    @Override
    public void close()
            throws IOException
    {
        cancelPendingLoad();
        bufferedPage = null;
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (state == State.WAITING_FOR_FILTER && !pendingLoad.isDone()) {
            return toCompletableFuture(pendingLoad);
        }
        return delegate.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    private enum State
    {
        // The load is started and no rows have been seen, so empty pages pass through
        LOADING,
        // bufferedPage holds the first non-empty page until pendingLoad completes
        WAITING_FOR_FILTER,
        // pageFilter is set and bufferedPage holds the first page until it is returned
        FILTERING,
        // The split finished without producing rows and the load was canceled
        CANCELED,
    }
}
