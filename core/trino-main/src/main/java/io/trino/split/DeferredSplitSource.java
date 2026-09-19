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
package io.trino.split;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogHandle;
import io.trino.spi.metrics.Metrics;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

/**
 * Defers connector split-source creation until operator-factory initialization has discovered
 * the runtime constraints accepted by a scan.
 */
public final class DeferredSplitSource
        implements SplitSource
{
    private final CatalogHandle catalogHandle;
    private final CompletableFuture<SplitSource> delegateFuture;

    private boolean closed;

    public DeferredSplitSource(CatalogHandle catalogHandle, CompletableFuture<SplitSource> delegateFuture)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.delegateFuture = requireNonNull(delegateFuture, "delegateFuture is null");
        delegateFuture.thenAccept(delegate -> {
            synchronized (this) {
                if (!closed) {
                    return;
                }
            }
            delegate.close();
        });
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        synchronized (this) {
            checkState(!closed, "Split source is closed");
        }
        ListenableFuture<SplitSource> delegate = Futures.catchingAsync(
                toListenableFuture(delegateFuture),
                CompletionException.class,
                failure -> Futures.immediateFailedFuture(failure.getCause()),
                directExecutor());
        return Futures.transformAsync(delegate, source -> source.getNextBatch(maxSize), directExecutor());
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (delegateFuture.isDone() && !delegateFuture.isCompletedExceptionally()) {
            delegateFuture.join().close();
        }
    }

    @Override
    public boolean isFinished()
    {
        return delegateFuture.isDone() && !delegateFuture.isCompletedExceptionally() && delegateFuture.join().isFinished();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        if (!delegateFuture.isDone() || delegateFuture.isCompletedExceptionally()) {
            return Optional.empty();
        }
        return delegateFuture.join().getTableExecuteSplitsInfo();
    }

    @Override
    public Metrics getMetrics()
    {
        if (!delegateFuture.isDone() || delegateFuture.isCompletedExceptionally()) {
            return Metrics.EMPTY;
        }
        return delegateFuture.join().getMetrics();
    }

    @Override
    public boolean isSplitSourceCreationDeferred()
    {
        return !delegateFuture.isDone();
    }
}
