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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.iceberg.IcebergColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class IcebergPageSourceUpdate
        implements UpdatablePageSource
{
    private final Slice filePathSlice;
    private final List<IcebergColumnHandle> queriedColumns;
    private final List<IcebergColumnHandle> allTableColumns;
    private final List<IcebergColumnHandle> updateColumns;
    private final ConnectorPageSource source;
    private final ConnectorPageSink posDeleteSink;
    private final ConnectorPageSink updateRowSink;

    public IcebergPageSourceUpdate(
            String filePath,
            List<IcebergColumnHandle> queriedColumns,
            List<IcebergColumnHandle> allTableColumns,
            List<IcebergColumnHandle> updateColumns,
            ConnectorPageSource source,
            ConnectorPageSink posDeleteSink,
            ConnectorPageSink updateRowSink)
    {
        this.filePathSlice = Slices.utf8Slice(requireNonNull(filePath, "filePath is null"));
        this.queriedColumns = requireNonNull(queriedColumns, "queriedColumns is null");
        this.allTableColumns = requireNonNull(allTableColumns, "allTableColumns is null");
        this.updateColumns = requireNonNull(updateColumns, "updateColumns is null");
        this.source = requireNonNull(source, "source is null");
        this.posDeleteSink = requireNonNull(posDeleteSink, "posDeleteSink is null");
        this.updateRowSink = requireNonNull(updateRowSink, "updateRowSink is null");
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        RowBlock rows = (RowBlock) rowIds;
        // TODO: need to use proper way to get blocks inside the row block instead of using getRawFieldBlocks(), this is just for POC purpose
        posDeleteSink.appendPage(new Page(rows.getPositionCount(), rows.getRawFieldBlocks()));
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        RowBlock rowIdBlock = (RowBlock) page.getBlock(columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1));
        deleteRows(rowIdBlock);
        Map<Integer, Block> updatedColumnBlocks = new HashMap<>();
        for (int i = 0; i < columnValueAndRowIdChannels.size() - 1; i++) {
            IcebergColumnHandle updatedColumn = updateColumns.get(i);
            Block updatedValues = page.getBlock(columnValueAndRowIdChannels.get(i));
            updatedColumnBlocks.put(updatedColumn.getId(), updatedValues);
        }

        Block[] updatedRows = new Block[allTableColumns.size()];
        Block[] oldRows = ((RowBlock) rowIdBlock.getRawFieldBlocks()[2]).getRawFieldBlocks();
        for (int i = 0; i < allTableColumns.size(); i++) {
            int columnId = allTableColumns.get(i).getId();
            if (updatedColumnBlocks.containsKey(columnId)) {
                updatedRows[i] = updatedColumnBlocks.get(columnId);
            }
            else {
                updatedRows[i] = oldRows[i];
            }
        }

        updateRowSink.appendPage(new Page(page.getPositionCount(), updatedRows));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            Collection<Slice> slices = posDeleteSink.finish().get();
            slices.addAll(updateRowSink.finish().get());
            return CompletableFuture.completedFuture(slices);
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Page getNextPage()
    {
        Page sourcePage = source.getNextPage();
        if (sourcePage == null) {
            return null;
        }
        Block[] rowIdComponentBlocks = new Block[3];
        int channelCount = sourcePage.getChannelCount();
        int pageSize = sourcePage.getPositionCount();
        // file_path
        rowIdComponentBlocks[0] = RunLengthEncodedBlock.create(VARCHAR, filePathSlice, pageSize);
        rowIdComponentBlocks[1] = sourcePage.getBlock(channelCount - 2);
        Block[] fieldBlocks = new Block[allTableColumns.size()];
        for (int i = 0; i < allTableColumns.size(); i++) {
            fieldBlocks[i] = sourcePage.getBlock(i);
        }
        rowIdComponentBlocks[2] = RowBlock.fromFieldBlocks(pageSize, Optional.empty(), fieldBlocks);

        Block[] resultBlocks = new Block[queriedColumns.size()];
        for (int i = 0; i < queriedColumns.size(); i++) {
            IcebergColumnHandle columnHandle = queriedColumns.get(i);
            if (ROW_ID_COLUMN_NAME.equals(columnHandle.getName())) {
                resultBlocks[i] = RowBlock.fromFieldBlocks(pageSize, Optional.empty(), rowIdComponentBlocks);
            }
            else {
                resultBlocks[i] = sourcePage.getBlock(allTableColumns.indexOf(columnHandle));
            }
        }
        return new Page(sourcePage.getPositionCount(), resultBlocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return source.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }

    @Override
    public long getCompletedBytes()
    {
        return source.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return source.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }
}
