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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.metadata.Split;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.split.SplitSource;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class TestingSplitSource
        implements SplitSource
{
    private final CatalogName catalogName;
    private final Iterator<Split> splits;

    public TestingSplitSource(CatalogName catalogName, List<Split> splits)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null")).iterator();
    }

    @Override
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        if (isFinished()) {
            return immediateFuture(new SplitBatch(ImmutableList.of(), true));
        }
        ImmutableList.Builder<Split> result = ImmutableList.builder();
        for (int i = 0; i < maxSize; i++) {
            if (!splits.hasNext()) {
                break;
            }
            result.add(splits.next());
        }
        return immediateFuture(new SplitBatch(result.build(), isFinished()));
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return !splits.hasNext();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }
}
