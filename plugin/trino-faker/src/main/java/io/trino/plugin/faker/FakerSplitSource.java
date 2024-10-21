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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;

import static java.lang.Math.ceilDiv;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FakerSplitSource
        implements ConnectorSplitSource
{
    // this is equal to 10k pages generated in FakerPageSource
    static final long MAX_ROWS_PER_SPLIT = 10_000L * 4096;

    private final long splitLimit;
    private final long lastSplitLimit;
    private final Iterator<HostAddress> splits;

    public FakerSplitSource(List<HostAddress> addresses, long minSplitsPerNode, long tableLimit)
    {
        requireNonNull(addresses, "addresses is null");
        long splitsPerNode = addresses.size() * minSplitsPerNode;
        this.splitLimit = min(ceilDiv(tableLimit, splitsPerNode), MAX_ROWS_PER_SPLIT);
        splitsPerNode = ceilDiv(tableLimit, splitLimit * addresses.size());
        this.lastSplitLimit = tableLimit - splitLimit * (addresses.size() * splitsPerNode - 1);
        this.splits = LongStream.range(0, splitsPerNode)
                .boxed()
                .flatMap(_ -> addresses.stream())
                .iterator();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        for (int i = 0; i < maxSize && splits.hasNext(); i++) {
            builder.add(new FakerSplit(ImmutableList.of(splits.next()), splits.hasNext() ? splitLimit : lastSplitLimit));
        }

        return completedFuture(new ConnectorSplitBatch(builder.build(), isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return !splits.hasNext();
    }

    @Override
    public void close()
    {
    }
}
