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
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.util.Objects.requireNonNull;

/**
 * Defers loading the delete page filter until the split produces its first non-empty page, then applies it to
 * every subsequent page. The driver waits in blocked state while the filter loads rather than blocking a thread.
 * <p>
 * Deferring to the first non-empty page keeps the load behind both dynamic-filter split pruning and footer
 * row-group pruning, so splits that produce no rows never trigger it.
 */
public final class AsyncFilteredPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final Supplier<ListenableFuture<Optional<PageFilter>>> pageFilterLoader;

    private State state = State.INITIAL;
    private ListenableFuture<Optional<PageFilter>> pendingLoad;
    private SourcePage bufferedPage;
    private Optional<PageFilter> pageFilter;

    public AsyncFilteredPageSource(
            ConnectorPageSource delegate,
            Supplier<ListenableFuture<Optional<PageFilter>>> pageFilterLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.pageFilterLoader = requireNonNull(pageFilterLoader, "pageFilterLoader is null");
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
            case INITIAL -> {
                SourcePage page = delegate.getNextSourcePage();
                if (page == null || page.getPositionCount() == 0) {
                    yield page;
                }
                pendingLoad = startLoad();
                bufferedPage = page;
                state = State.LOADING;
                yield getNextSourcePage();
            }
            case LOADING -> {
                requireNonNull(pendingLoad, "pendingLoad must be present in LOADING state");
                if (!pendingLoad.isDone()) {
                    yield null;
                }
                pageFilter = getFutureValue(pendingLoad);
                pendingLoad = null;
                state = State.LOADED;
                yield getNextSourcePage();
            }
            case LOADED -> {
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
        };
    }

    /**
     * The loader runs outside the failed-future handling of the load itself, so classify what it throws here.
     */
    private ListenableFuture<Optional<PageFilter>> startLoad()
    {
        try {
            return requireNonNull(pageFilterLoader.get(), "pageFilterLoader returned null");
        }
        catch (RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ICEBERG_BAD_DATA, e);
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
        if (pendingLoad != null) {
            pendingLoad.cancel(true);
        }
        bufferedPage = null;
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (pendingLoad != null && !pendingLoad.isDone()) {
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
        // No non-empty page seen yet and no load started
        INITIAL,
        // BufferedPage and pendingLoad are both set
        LOADING,
        // pageFilter is set and bufferedPage holds the first page until it is returned
        LOADED,
    }
}
