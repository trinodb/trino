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
package io.trino.plugin.hive;

import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static java.util.Objects.requireNonNull;

/**
 * Tracks the evolving dynamic filter state for a single Hive table scan.
 * <p>
 * {@link #update} is called by {@link HiveSplitSource#getNextBatch} on every engine batch call.
 * {@link #isBlocked()} blocks until the first snapshot has arrived, acting as a gate for
 * {@link BackgroundHiveSplitLoader} to avoid producing un-pruned splits before the engine's
 * dynamic-filter wait window has closed.
 */
public class DynamicFilterState
{
    private final CompletableFuture<Void> firstArrival = new CompletableFuture<>();
    private volatile DynamicFilterSnapshot latest = new DynamicFilterSnapshot(TupleDomain.all(), false);

    void update(DynamicFilterSnapshot filter)
    {
        latest = requireNonNull(filter, "filter is null");
        firstArrival.complete(null);
    }

    public DynamicFilterSnapshot get()
    {
        return latest;
    }

    public DynamicFilterSnapshot get(long timeout, TimeUnit unit)
    {
        try {
            firstArrival.get(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return latest;
    }

    public CompletableFuture<Void> isBlocked()
    {
        return unmodifiableFuture(firstArrival);
    }

    public boolean isReady()
    {
        return firstArrival.isDone();
    }

    void cancel()
    {
        firstArrival.complete(null);
    }

    static DynamicFilterState completedState()
    {
        DynamicFilterState state = new DynamicFilterState();
        state.update(new DynamicFilterSnapshot(TupleDomain.all(), true));
        return state;
    }
}
