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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.UpdatablePageSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Concatenates the Pages for all PageSources which were grouped together an Iceberg CombinedScanTask.
 */
public class IcebergCombinedPageSource
        implements UpdatablePageSource
{
    private final Map<String, UpdatablePageSource> delegatePageSources;
    private final Iterator<UpdatablePageSource> pageSourceIterator;

    private ConnectorPageSource currentPageSource;

    public IcebergCombinedPageSource(Map<String, UpdatablePageSource> delegatePageSourcesByPath)
    {
        if (delegatePageSourcesByPath.isEmpty()) {
            this.delegatePageSources = ImmutableMap.of();
            this.pageSourceIterator = emptyIterator();
            this.currentPageSource = new EmptyPageSource();
        }
        else {
            this.delegatePageSources = ImmutableMap.copyOf(requireNonNull(delegatePageSourcesByPath, "delegatePageSources is null"));
            this.pageSourceIterator = delegatePageSourcesByPath.values().iterator();
            this.currentPageSource = pageSourceIterator.next();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return delegatePageSources.values().stream()
                .mapToLong(ConnectorPageSource::getCompletedBytes)
                .sum();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegatePageSources.values().stream()
                .map(ConnectorPageSource::getCompletedPositions)
                .reduce(OptionalLong.of(0), (accumulatedPositions, newPositions) -> {
                    if (accumulatedPositions.isPresent() && newPositions.isPresent()) {
                        return OptionalLong.of(accumulatedPositions.getAsLong() + newPositions.getAsLong());
                    }
                    return OptionalLong.empty();
                });
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegatePageSources.values().stream()
                .mapToLong(ConnectorPageSource::getReadTimeNanos)
                .sum();
    }

    @Override
    public boolean isFinished()
    {
        return !pageSourceIterator.hasNext() && currentPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        if (currentPageSource.isFinished()) {
            currentPageSource = pageSourceIterator.next();
        }

        return currentPageSource.getNextPage();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegatePageSources.values().stream()
                .mapToLong(ConnectorPageSource::getMemoryUsage)
                .sum();
    }

    @Override
    public void close()
            throws IOException
    {
        IOException closeException = null;
        for (ConnectorPageSource delegatePageSource : delegatePageSources.values()) {
            // Try to close all Sources before throwing any exceptions
            try {
                delegatePageSource.close();
            }
            catch (IOException e) {
                if (closeException == null) {
                    closeException = e;
                }
                else if (closeException != e) {
                    closeException.addSuppressed(e);
                }
            }
        }

        if (closeException != null) {
            throw closeException;
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return currentPageSource.isBlocked();
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        if (rowIds.getPositionCount() == 0) {
            return;
        }

        ColumnarRow rowIdColumns = ColumnarRow.toColumnarRow(rowIds);
        Block filePathBlock = rowIdColumns.getField(0);
        Block rowPositionBlock = rowIdColumns.getField(1);

        Map<String, List<Integer>> pathToPositionList = new HashMap<>();
        for (int position = 0; position < rowIds.getPositionCount(); position++) {
            String path = VARCHAR.getSlice(filePathBlock, position).toStringUtf8();
            pathToPositionList.putIfAbsent(path, new ArrayList<>());
            pathToPositionList.get(path).add(position);
        }

        for (Map.Entry<String, List<Integer>> pathAndPositionList : pathToPositionList.entrySet()) {
            UpdatablePageSource delegatePageSource = delegatePageSources.get(pathAndPositionList.getKey());
            if (delegatePageSource == null) {
                throw new IllegalStateException("ConcatenatingPageSource received a block for a file it did not read");
            }
            int[] positions = pathAndPositionList.getValue().stream().mapToInt(Integer::intValue).toArray();
            delegatePageSource.deleteRows(rowPositionBlock.getPositions(positions, 0, positions.length));
        }
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        int rowIdChannel = columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1);
        Block filePathBlock = ColumnarRow.toColumnarRow(page.getBlock(rowIdChannel)).getField(0);

        Map<String, List<Integer>> pathToPositionList = new HashMap<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            String path = VARCHAR.getSlice(filePathBlock, position).toStringUtf8();
            pathToPositionList.putIfAbsent(path, new ArrayList<>());
            pathToPositionList.get(path).add(position);
        }

        for (Map.Entry<String, List<Integer>> pathAndPositionList : pathToPositionList.entrySet()) {
            UpdatablePageSource delegatePageSource = delegatePageSources.get(pathAndPositionList.getKey());
            if (delegatePageSource == null) {
                throw new IllegalStateException("ConcatenatingPageSource received a block for a file it did not read");
            }
            int[] positions = pathAndPositionList.getValue().stream().mapToInt(Integer::intValue).toArray();
            delegatePageSource.updateRows(page.getPositions(positions, 0, positions.length), columnValueAndRowIdChannels);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> finishFuture = CompletableFuture.completedFuture(ImmutableList.of());
        for (UpdatablePageSource pageSource : delegatePageSources.values()) {
            finishFuture = finishFuture.thenCombine(
                    pageSource.finish(),
                    (collection1, collection2) -> ImmutableList.<Slice>builder()
                            .addAll(collection1)
                            .addAll(collection2)
                            .build());
        }

        return finishFuture;
    }

    @Override
    public void abort()
    {
        delegatePageSources.values().forEach(UpdatablePageSource::abort);
    }
}
