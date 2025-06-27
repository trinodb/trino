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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.metrics.Metrics;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface SplitSource
        extends Closeable
{
    CatalogHandle getCatalogHandle();

    ListenableFuture<SplitBatch> getNextBatch(int maxSize);

    @Override
    void close();

    boolean isFinished();

    Optional<List<Object>> getTableExecuteSplitsInfo();

    /**
     * Returns the split source's metrics, mapping a metric id to its latest value.
     * Each call must return an immutable snapshot of available metrics.
     * The metrics for each split source are collected independently and exposed via StageStats and OperatorStats.
     * This method can be called after the split source is closed, and in that case the final metrics should be returned.
     */
    Metrics getMetrics();

    class SplitBatch
    {
        private final List<Split> splits;
        private final boolean lastBatch;

        public SplitBatch(List<Split> splits, boolean lastBatch)
        {
            this.splits = requireNonNull(splits, "splits is null");
            this.lastBatch = lastBatch;
        }

        public List<Split> getSplits()
        {
            return splits;
        }

        /**
         * Returns {@code true<} if all splits for the requested driver group have been returned.
         * In other hands, splits returned from this and all previous invocations of {@link #getNextBatch}
         * form the complete set of splits in the requested driver group.
         */
        public boolean isLastBatch()
        {
            return lastBatch;
        }
    }
}
