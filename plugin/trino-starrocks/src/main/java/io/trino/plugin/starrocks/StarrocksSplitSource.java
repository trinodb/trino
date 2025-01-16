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

package io.trino.plugin.starrocks;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.starrocks.StarrocksSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;

public class StarrocksSplitSource
        implements ConnectorSplitSource
{
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);

    private final StarrocksClient client;
    private final StarrocksTableHandle tableHandle;
    private final CompletableFuture<?> dynamicFilter;
    private final CompletableFuture<ConnectorSplitSource> splitSourceFuture;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final long dynamicFilteringWaitTimeoutMillis;

    public StarrocksSplitSource(StarrocksClient client,
            StarrocksTableHandle tableHandle,
            CompletableFuture<?> dynamicFilter,
            CompletableFuture<ConnectorSplitSource> splitSourceFuture,
            ConnectorSession session)
    {
        this.client = client;
        this.tableHandle = tableHandle;
        this.dynamicFilter = dynamicFilter;
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.splitSourceFuture = requireNonNull(splitSourceFuture, "splitSourceFuture is null");
        this.dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return splitSourceFuture.thenCompose(splitSource -> splitSource.getNextBatch(maxSize));
    }

    @Override
    public void close()
    {
        if (!dynamicFilter.cancel(true)) {
            splitSourceFuture.thenAccept(ConnectorSplitSource::close);
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!splitSourceFuture.isDone()) {
            return false;
        }
        if (splitSourceFuture.isCompletedExceptionally()) {
            return false;
        }
        try {
            return splitSourceFuture.get().isFinished();
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }
}
