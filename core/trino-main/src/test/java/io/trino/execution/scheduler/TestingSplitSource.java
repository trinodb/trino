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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.Split;
import io.trino.split.SplitSource;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class TestingSplitSource
        implements SplitSource
{
    private final CatalogHandle catalogHandle;
    private final ListenableFuture<List<Split>> splitsFuture;
    private int finishDelayRemainingIterations;
    private Iterator<Split> splits;

    public TestingSplitSource(CatalogHandle catalogHandle, List<Split> splits)
    {
        this(catalogHandle, splits, 0);
    }

    public TestingSplitSource(CatalogHandle catalogHandle, List<Split> splits, int finishDelayIterations)
    {
        this(
                catalogHandle,
                immediateFuture(ImmutableList.copyOf(requireNonNull(splits, "splits is null"))),
                finishDelayIterations);
    }

    public TestingSplitSource(CatalogHandle catalogHandle, ListenableFuture<List<Split>> splitsFuture, int finishDelayIterations)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.splitsFuture = requireNonNull(splitsFuture, "splitsFuture is null");
        this.finishDelayRemainingIterations = finishDelayIterations;
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        if (isFinished()) {
            return immediateFuture(new SplitBatch(ImmutableList.of(), true));
        }

        if (splits == null) {
            return Futures.transform(
                    splitsFuture,
                    splits -> {
                        checkState(this.splits == null, "splits should be null");
                        this.splits = splits.iterator();
                        return populateSplitBatch(maxSize);
                    },
                    directExecutor());
        }
        checkState(splitsFuture.isDone(), "splitsFuture should be completed");
        return immediateFuture(populateSplitBatch(maxSize));
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return (splits != null && !splits.hasNext())
                && finishDelayRemainingIterations-- <= 0;
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }

    private SplitBatch populateSplitBatch(int maxSize)
    {
        ImmutableList.Builder<Split> result = ImmutableList.builder();
        for (int i = 0; i < maxSize; i++) {
            if (!splits.hasNext()) {
                break;
            }
            result.add(splits.next());
        }
        return new SplitBatch(result.build(), isFinished());
    }
}
