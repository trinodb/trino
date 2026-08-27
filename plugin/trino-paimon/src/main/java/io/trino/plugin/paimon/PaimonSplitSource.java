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
package io.trino.plugin.paimon;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.util.Objects.requireNonNull;

public class PaimonSplitSource
        implements ConnectorSplitSource
{
    private final Queue<PaimonSplit> splits;
    private final OptionalLong limit;

    private long count;
    private boolean closed;

    public PaimonSplitSource(List<PaimonSplit> splits, OptionalLong limit)
    {
        requireNonNull(splits, "splits is null").forEach(split -> requireNonNull(split, "splits contains null split"));
        this.splits = new ArrayDeque<>(splits);
        this.limit = requireNonNull(limit, "limit is null");
        checkArgument(this.limit.isEmpty() || this.limit.orElseThrow() >= 0, "limit must be non-negative");
    }

    protected CompletableFuture<List<ConnectorSplit>> innerGetNextBatch(int maxSize)
    {
        if (closed) {
            return CompletableFuture.completedFuture(List.of());
        }

        List<ConnectorSplit> batch = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            if (limitReached()) {
                close();
                break;
            }
            PaimonSplit split = splits.poll();
            if (split == null) {
                break;
            }
            countRowsForLimit(split);
            batch.add(split);
            if (limitReached()) {
                close();
                break;
            }
        }
        return CompletableFuture.completedFuture(batch);
    }

    private void countRowsForLimit(PaimonSplit split)
    {
        if (limit.isEmpty()) {
            return;
        }
        try {
            long rowCount = split.rowCount() == null
                    ? PaimonSplitManager.splitWeightRowCount(split.decodeSplit())
                    : split.rowCount();
            count = saturatedAdd(count, rowCount, "split row count");
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(
                    PAIMON_CANNOT_OPEN_SPLIT,
                    "Failed to decode Paimon split while applying LIMIT pushdown",
                    e);
        }
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        requireNonNull(dynamicFilterSnapshot, "dynamicFilterSnapshot is null");
        if (dynamicFilterSnapshot.currentPredicate().isNone()) {
            close();
            return CompletableFuture.completedFuture(List.of());
        }
        return innerGetNextBatch(maxSize);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        splits.clear();
    }

    @Override
    public boolean isFinished()
    {
        return closed || splits.isEmpty() || limitReached();
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        return 0;
    }

    private boolean limitReached()
    {
        return limit.isPresent() && count >= limit.orElseThrow();
    }
}
