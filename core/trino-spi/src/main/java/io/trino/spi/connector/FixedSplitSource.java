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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.StreamSupport.stream;

public class FixedSplitSource
        implements ConnectorSplitSource
{
    private final List<ConnectorSplit> splits;
    private final Optional<List<Object>> tableExecuteSplitsInfo;
    private int offset;

    public static FixedSplitSource emptySplitSource()
    {
        return new FixedSplitSource(List.of());
    }

    public FixedSplitSource(ConnectorSplit split)
    {
        this(List.of(split));
    }

    public FixedSplitSource(Iterable<? extends ConnectorSplit> splits)
    {
        this(splits, Optional.empty());
    }

    public FixedSplitSource(Iterable<? extends ConnectorSplit> splits, List<Object> tableExecuteSplitsInfo)
    {
        this(splits, Optional.of(tableExecuteSplitsInfo));
    }

    private FixedSplitSource(Iterable<? extends ConnectorSplit> splits, Optional<List<Object>> tableExecuteSplitsInfo)
    {
        requireNonNull(splits, "splits is null");
        this.splits = stream(splits.spliterator(), false).collect(toUnmodifiableList());
        this.tableExecuteSplitsInfo = tableExecuteSplitsInfo.map(List::copyOf);
    }

    @SuppressWarnings("ObjectEquality")
    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<ConnectorSplit> results = splits.subList(offset, offset + size);
        offset += size;

        return completedFuture(new ConnectorSplitBatch(results, isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return offset >= splits.size();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return tableExecuteSplitsInfo;
    }

    @Override
    public void close()
    {
    }
}
