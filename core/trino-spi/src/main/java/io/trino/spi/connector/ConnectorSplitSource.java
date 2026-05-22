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
package io.trino.spi.connector;

import io.trino.spi.metrics.Metrics;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Source of splits to be processed.
 * <p>
 * Thread-safety: the implementations are not required to be thread-safe.
 */
public interface ConnectorSplitSource
        extends Closeable
{
    /**
     * Returns the next batch of splits.
     * <p>
     * Callers must not invoke this method again until the returned future is done.
     * In other words, each split source supports at most one outstanding batch request.
     * Implementations are not required to support concurrent invocations of this method.
     * <p>
     * The returned future may complete with an empty list when no splits are currently
     * available. This does not mean the split source is finished; check {@link #isFinished()}.
     * <p>
     * The {@link #close()} method may be called concurrently while a batch request is
     * outstanding.
     * If this method observes that the split source has already been closed, it should
     * preferably fail the request, such as by returning an exceptionally completed
     * future. Since close may race with a batch request, it is also acceptable to return
     * a normal batch result.
     * {@code dynamicFilterSnapshot} contains a snapshot of the engine's dynamic filter state captured
     * immediately before this call. The engine waits up to
     * {@link #getRequestedDynamicFilterWaitTimeoutMillis()} before the <em>first</em> call;
     * subsequent calls receive the current state without additional waiting.
     * <p>
     * When the returned list is empty and {@link #isFinished()} returns {@code false}, the caller
     * should retry. No more batches will be produced once {@link #isFinished()} returns {@code true}.
     */
    CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot);

    @Override
    void close();

    /**
     * Returns whether any more {@link ConnectorSplit} may be produced.
     * <p>
     * This method should only be called when there has been no invocation of getNextBatch,
     * or result Future of previous getNextBatch is done.
     * Calling this method at other time is not useful because the contract of such an invocation
     * will be inherently racy.
     */
    boolean isFinished();

    default Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }

    /**
     * Hints to the engine the maximum time in milliseconds to wait for dynamic filters to be
     * collected before the first call to {@link #getNextBatch(int, DynamicFilterSnapshot)}.
     * The engine may wait up to this timeout, or until the dynamic filter is fully resolved,
     * whichever comes first.
     * <p>
     * The engine reads this value once when the split source is constructed; it is treated as
     * fixed for the lifetime of the source (per table scan).
     */
    default long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        return 0;
    }

    /**
     * Returns the split source's metrics, mapping a metric id to its latest value.
     * Each call must return an immutable snapshot of available metrics.
     * The metrics for each split source are collected independently and exposed via StageStats and OperatorStats.
     * This method can be called after the split source is closed, and in that case the final metrics should be returned.
     */
    default Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }
}
