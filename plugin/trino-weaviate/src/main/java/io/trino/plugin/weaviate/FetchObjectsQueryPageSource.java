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
package io.trino.plugin.weaviate;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.weaviate.Decoder.decode;
import static java.util.Objects.requireNonNull;

public class FetchObjectsQueryPageSource
        implements ConnectorPageSource
{
    private final List<WeaviateColumnHandle> columns;
    private final Iterator<Map<String, Object>> iterator;
    private final BlockBuilder[] columnBuilders;

    private long readTimeNanos;
    private long totalBytes;

    public FetchObjectsQueryPageSource(
            WeaviateService weaviateService,
            WeaviateTableHandle tableHandle,
            List<WeaviateColumnHandle> columns)
    {
        requireNonNull(weaviateService, "weaviateService is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");

        this.columns = columns;
        this.columnBuilders = columns.stream()
                .map(WeaviateColumnHandle::trinoType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        long start = System.nanoTime();
        this.iterator = weaviateService.getTableIterator(tableHandle);
        this.readTimeNanos += System.nanoTime() - start;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        long pageSize = 0;
        while (pageSize < PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES && iterator.hasNext()) {
            Map<String, Object> row = iterator.next();

            for (int i = 0; i < columnBuilders.length; i++) {
                WeaviateColumnHandle columnHandle = columns.get(i);
                Object rawValue = row.get(columnHandle.name());
                decode(columnBuilders[i], rawValue, columnHandle.trinoType());
                pageSize += columnBuilders[i].getSizeInBytes();
            }
        }

        totalBytes += pageSize;
        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }
        return SourcePage.create(new Page(blocks));
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
