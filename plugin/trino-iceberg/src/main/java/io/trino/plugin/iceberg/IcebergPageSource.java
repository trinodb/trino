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
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.iceberg.delete.IcebergPositionDeletePageSink;
import io.trino.plugin.iceberg.delete.TrinoDeleteFilter;
import io.trino.plugin.iceberg.delete.TrinoRow;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class IcebergPageSource
        implements UpdatablePageSource
{
    private final Block filePathBlock;
    private final TrinoDeleteFilter deleteFilter;
    private final ConnectorPageSource delegate;
    private final Block[] queriedColumnPrefillValues;
    private final int[] queriedColumnFileReadChannels;
    private final Block[] deleteColumnPrefillValues;
    private final Type[] deleteColumnTypes;
    private final int[] deleteColumnFileReadChannels;
    private final Block[] nonUpdateColumnPrefillValues;
    private final int[] nonUpdateColumnFileReadChannels;
    private final int rowPositionChannel;
    private final int[] allTableColumnChannels;
    private final IcebergPositionDeletePageSink posDeleteSink;
    private final IcebergPageSink updateRowSink;
    private final Optional<ReaderProjectionsAdapter> projectionsAdapter;

    public IcebergPageSource(
            String filePath,
            TrinoDeleteFilter deleteFilter,
            ConnectorPageSource delegate,
            Block[] queriedColumnPrefillValues,
            int[] queriedColumnFileReadChannels,
            Block[] deleteColumnPrefillValues,
            int[] deleteColumnFileReadChannels,
            Type[] deleteColumnTypes,
            Block[] nonUpdateColumnPrefillValues,
            int[] nonUpdateColumnFileReadChannels,
            int rowPositionChannel,
            int[] allTableColumnChannels,
            IcebergPositionDeletePageSink posDeleteSink,
            IcebergPageSink updateRowSink,
            Optional<ReaderProjectionsAdapter> projectionsAdapter,
            boolean isDeleteOrUpdateQuery,
            boolean isUpdateQuery)
    {
        this.filePathBlock = nativeValueToBlock(VARCHAR, Slices.utf8Slice(requireNonNull(filePath, "filePath is null")));
        this.deleteFilter = requireNonNull(deleteFilter, "deleteFilter is null");
        this.deleteColumnTypes = requireNonNull(deleteColumnTypes, "deleteColumnTypes is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.queriedColumnPrefillValues = requireNonNull(queriedColumnPrefillValues, "queriedColumnPrefillValues is null");
        this.queriedColumnFileReadChannels = requireNonNull(queriedColumnFileReadChannels, "queriedColumnFileReadChannels is null");
        this.deleteColumnPrefillValues = requireNonNull(deleteColumnPrefillValues, "deleteColumnPrefillValues is null");
        this.deleteColumnFileReadChannels = requireNonNull(deleteColumnFileReadChannels, "deleteColumnFileReadChannels is null");
        this.nonUpdateColumnPrefillValues = requireNonNull(nonUpdateColumnPrefillValues, "nonUpdateColumnPrefillValues is null");
        this.nonUpdateColumnFileReadChannels = requireNonNull(nonUpdateColumnFileReadChannels, "nonUpdateColumnFileReadChannels is null");
        this.allTableColumnChannels = requireNonNull(allTableColumnChannels, "allTableColumnChannels is null");
        this.rowPositionChannel = rowPositionChannel;
        if (isDeleteOrUpdateQuery) {
            requireNonNull(posDeleteSink, "posDeleteSink is null");
        }
        this.posDeleteSink = posDeleteSink;
        if (isUpdateQuery) {
            requireNonNull(updateRowSink, "updateRowSink is null");
        }
        this.updateRowSink = updateRowSink;
        this.projectionsAdapter = requireNonNull(projectionsAdapter, "projectionsAdapter is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    private Block[] buildBlocksFromPage(Page page, Block[] prefillValues, int[] fileReadChannels)
    {
        int batchSize = page.getPositionCount();
        Block[] blocks = new Block[prefillValues.length];
        for (int i = 0; i < prefillValues.length; i++) {
            int fileReadChannel = fileReadChannels[i];
            if (fileReadChannel == -1) {
                blocks[i] = new RunLengthEncodedBlock(prefillValues[i], batchSize);
            }
            else if (fileReadChannel == -2) {
                blocks[i] = buildRowIdBlock(page);
            }
            else {
                blocks[i] = page.getBlock(fileReadChannel);
            }
        }
        return blocks;
    }

    private Block buildRowIdBlock(Page page)
    {
        int batchSize = page.getPositionCount();
        Block[] blocks = new Block[nonUpdateColumnPrefillValues.length + 2];
        blocks[0] = new RunLengthEncodedBlock(filePathBlock, batchSize);
        blocks[1] = page.getBlock(rowPositionChannel);
        Block[] nonUpdateColumnBlocks = buildBlocksFromPage(page, nonUpdateColumnPrefillValues, nonUpdateColumnFileReadChannels);
        for (int i = 0; i < nonUpdateColumnBlocks.length; i++) {
            blocks[i + 2] = nonUpdateColumnBlocks[i];
        }
        return fromFieldBlocks(batchSize, Optional.empty(), blocks);
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (projectionsAdapter.isPresent()) {
                dataPage = projectionsAdapter.get().adaptPage(dataPage);
            }
            if (dataPage == null) {
                return null;
            }

            int batchSize = dataPage.getPositionCount();
            Block[] outputBlocks = buildBlocksFromPage(dataPage, queriedColumnPrefillValues, queriedColumnFileReadChannels);
            Page outputPage = new Page(batchSize, outputBlocks);

            // short-circuit if no deletes
            if (deleteColumnFileReadChannels.length == 0) {
                return outputPage;
            }

            Block[] deleteBlocks = buildBlocksFromPage(dataPage, deleteColumnPrefillValues, deleteColumnFileReadChannels);
            CloseableIterable<TrinoRow> filteringRows = CloseableIterable.withNoopClose(TrinoRow.fromBlocks(deleteColumnTypes, deleteBlocks, batchSize));
            CloseableIterable<TrinoRow> filteredRows = deleteFilter.filter(filteringRows);
            int[] positionsToKeep = StreamSupport.stream(filteredRows.spliterator(), false).mapToInt(TrinoRow::getPosition).toArray();
            return outputPage.getPositions(positionsToKeep, 0, positionsToKeep.length);
        }

        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ICEBERG_BAD_DATA, e);
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        RowBlock rows = resolveRowIdBlock(rowIds);
        posDeleteSink.appendPage(new Page(rows.getPositionCount(), rows.getChildren().toArray(new Block[0])));
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        int batchSize = page.getPositionCount();
        Block rowIdRawBlock = page.getBlock(columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1));
        RowBlock rowIdBlock = resolveRowIdBlock(rowIdRawBlock);
        Block[] rowIdFieldBlocks = rowIdBlock.getChildren().toArray(new Block[0]);
        Block[] posDeleteBlocks = new Block[2];
        posDeleteBlocks[0] = rowIdFieldBlocks[0];
        posDeleteBlocks[1] = rowIdFieldBlocks[1];
        posDeleteSink.appendPage(new Page(batchSize, posDeleteBlocks));

        Block[] allTableColumnBlocks = new Block[allTableColumnChannels.length];
        for (int i = 0; i < allTableColumnChannels.length; i++) {
            int allTableColumnChannel = allTableColumnChannels[i];
            if (allTableColumnChannel > -1) {
                allTableColumnBlocks[i] = rowIdFieldBlocks[allTableColumnChannel + 2];
            }
            else {
                int updateValueChannelIndex = (-1) - allTableColumnChannel;
                allTableColumnBlocks[i] = page.getBlock(columnValueAndRowIdChannels.get(updateValueChannelIndex));
            }
        }

        updateRowSink.appendPage(new Page(batchSize, allTableColumnBlocks));
    }

    private RowBlock resolveRowIdBlock(Block rowIds)
    {
        RowBlock rows;
        if (rowIds instanceof RowBlock) {
            rows = (RowBlock) rowIds;
        }
        else if (rowIds instanceof DictionaryBlock) {
            Block dictionary = ((DictionaryBlock) rowIds).getDictionary();
            if (dictionary instanceof RowBlock) {
                rows = (RowBlock) dictionary;
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected rowId dictionary block type " + dictionary);
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected rowId block type " + rowIds);
        }
        return rows;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            Collection<Slice> slices = new ArrayList<>();
            if (posDeleteSink != null) {
                slices.addAll(posDeleteSink.finish().get());
            }
            if (updateRowSink != null) {
                slices.addAll(updateRowSink.finish().get());
            }
            return CompletableFuture.completedFuture(slices);
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort()
    {
        posDeleteSink.abort();
        updateRowSink.abort();
        close();
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        closeAllSuppress(throwable, this);
    }
}
