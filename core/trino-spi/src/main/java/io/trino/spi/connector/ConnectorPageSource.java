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

import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.connector.Preconditions.checkArgument;

public interface ConnectorPageSource
        extends Closeable
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * Gets the number of input bytes processed by this page source so far.
     * If size is not available, this method should return zero.
     */
    long getCompletedBytes();

    /**
     * Gets the number of input rows processed by this page source so far.
     * By default, the positions count of the page returned from getNextPage
     * is used to calculate the number of input rows.
     */
    default OptionalLong getCompletedPositions()
    {
        return OptionalLong.empty();
    }

    /**
     * Gets the wall time this page source spent reading data from the input.
     * If read time is not available, this method should return zero.
     */
    long getReadTimeNanos();

    /**
     * Will this page source product more pages?
     */
    boolean isFinished();

    /**
     * Gets the next page of data.  This method is allowed to return null.
     */
    Page getNextPage();

    /**
     * Get the total memory that needs to be reserved in the memory pool.
     * This memory should include any buffers, etc. that are used for reading data.
     *
     * @return the memory used so far in table read
     */
    long getMemoryUsage();

    /**
     * Immediately finishes this page source.  Trino will always call this method.
     */
    @Override
    void close()
            throws IOException;

    /**
     * Returns a future that will be completed when the page source becomes
     * unblocked.  If the page source is not blocked, this method should return
     * {@code NOT_BLOCKED}.
     */
    default CompletableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    /**
     * Returns the connector's metrics, mapping a metric ID to its latest value.
     * Each call must return an immutable snapshot of available metrics.
     * Same ID metrics are merged across all tasks and exposed via OperatorStats.
     * This method can be called after the page source is closed.
     */
    default Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }

    /**
     * Returns row number ranges that will be produced by this ConnectorPageSource.
     * For example, suppose the row numbers returned for a split are `[0, 1, 2, 4, 5, 7, 8, 9]`,
     * then this method should return 3 row ranges: `[0-2], [4-5], [7-9]`.
     * <p>
     * If Optional.empty() is returned, this ConnectorPageSource is expected to return all rows corresponding to the split.
     * <p>
     * If {@link RowRanges#EMPTY} is returned, this ConnectorPageSource is expected to return no rows.
     * <p>
     * RowRanges may be produced in batches, {@link RowRanges#isNoMoreRowRanges()} will specify whether any more row ranges may be produced.
     * When RowRanges are produced in incremental batches, an empty RowRanges with {@link RowRanges#isNoMoreRowRanges()} set to false may be returned
     * when the next range of rows for the split is not known in advance. The subsequent row ranges will become available from getNextFilteredRowRanges
     * after {@link ConnectorPageSource#getNextPage()} has advanced beyond the {@link RowRanges#getRowCount()} returned so far.
     */
    default Optional<RowRanges> getNextFilteredRowRanges()
    {
        return Optional.empty();
    }

    final class RowRanges
    {
        public static final RowRanges EMPTY = new RowRanges(new long[0], new long[0], true);

        private final long[] lowerInclusive;
        private final long[] upperExclusive;
        private final long rowCount;
        private final boolean noMoreRowRanges;

        public RowRanges(long[] lowerInclusive, long[] upperExclusive, boolean noMoreRowRanges)
        {
            checkArgument(
                    lowerInclusive.length == upperExclusive.length,
                    "lowerInclusive size %s should match upperExclusive size %s",
                    lowerInclusive.length,
                    upperExclusive.length);
            this.lowerInclusive = lowerInclusive;
            this.upperExclusive = upperExclusive;
            this.noMoreRowRanges = noMoreRowRanges;
            long rangesRowCount = 0;
            for (int rangeIndex = 0; rangeIndex < lowerInclusive.length; rangeIndex++) {
                checkArgument(lowerInclusive[rangeIndex] >= 0, "lowerInclusive %s must not be negative", lowerInclusive[rangeIndex]);
                checkArgument(
                        upperExclusive[rangeIndex] > lowerInclusive[rangeIndex],
                        "upperExclusive %s must be higher than lowerInclusive %s",
                        upperExclusive[rangeIndex],
                        lowerInclusive[rangeIndex]);
                if (rangeIndex > 0) {
                    checkArgument(
                            lowerInclusive[rangeIndex] > upperExclusive[rangeIndex - 1],
                            "lowerInclusive %s must be greater than previous upperExclusive %s",
                            lowerInclusive[rangeIndex],
                            upperExclusive[rangeIndex - 1]);
                }
                rangesRowCount += upperExclusive[rangeIndex] - lowerInclusive[rangeIndex];
            }
            this.rowCount = rangesRowCount;
        }

        public long getLowerInclusive(int rangeIndex)
        {
            return lowerInclusive[rangeIndex];
        }

        public long getUpperExclusive(int rangeIndex)
        {
            return upperExclusive[rangeIndex];
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public int getRangesCount()
        {
            return lowerInclusive.length;
        }

        public boolean isNoMoreRowRanges()
        {
            return noMoreRowRanges;
        }
    }
}
