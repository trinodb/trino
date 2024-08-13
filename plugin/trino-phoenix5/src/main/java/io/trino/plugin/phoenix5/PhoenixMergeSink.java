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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeSink;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY_COLUMN_HANDLE;
import static io.trino.spi.type.TinyintType.TINYINT;

public class PhoenixMergeSink
        implements ConnectorMergeSink
{
    private final JdbcMergeSink delegate;
    private final ConnectorPageSink updateSink;
    private final boolean hasRowKey;
    private final int columnCount;
    private final List<Integer> reservedChannelsForUpdate;

    public PhoenixMergeSink(
            ConnectorSession session,
            PhoenixClient phoenixClient,
            RemoteQueryModifier remoteQueryModifier,
            ConnectorMergeTableHandle mergeHandle,
            ConnectorPageSinkId pageSinkId)
    {
        this.delegate = new JdbcMergeSink(session, phoenixClient, remoteQueryModifier, mergeHandle, pageSinkId);
        PhoenixMergeTableHandle mergeTableHandle = (PhoenixMergeTableHandle) mergeHandle;
        this.updateSink = createUpdateSink(session, mergeTableHandle.getOutputTableHandle(), phoenixClient, pageSinkId, remoteQueryModifier);
        this.hasRowKey = mergeTableHandle.isHasRowKey();
        this.columnCount = mergeTableHandle.getOutputTableHandle().getColumnNames().size();

        this.reservedChannelsForUpdate = delegate.getReservedChannelsForUpdate(mergeTableHandle.getOutputTableHandle());
        verify(!reservedChannelsForUpdate.isEmpty(), "Update primary key itself is not supported");
    }

    protected ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            JdbcOutputTableHandle jdbcOutputTableHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        PhoenixOutputTableHandle outputTableHandle = (PhoenixOutputTableHandle) jdbcOutputTableHandle;
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        columnNamesBuilder.addAll(outputTableHandle.getColumnNames());
        columnTypesBuilder.addAll(outputTableHandle.getColumnTypes());
        if (outputTableHandle.rowkeyColumn().isPresent()) {
            columnNamesBuilder.add(ROWKEY);
            columnTypesBuilder.add(ROWKEY_COLUMN_HANDLE.getColumnType());
        }

        PhoenixOutputTableHandle updateOutputTableHandle = new PhoenixOutputTableHandle(
                outputTableHandle.getSchemaName(),
                outputTableHandle.getTableName(),
                columnNamesBuilder.build(),
                columnTypesBuilder.build(),
                Optional.empty(),
                Optional.empty());
        return new JdbcPageSink(session, updateOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
    }

    private void appendUpdatePage(Page dataPage, List<Block> rowIdFields, int[] updatePositions, int updatePositionCount)
    {
        if (updatePositionCount == 0) {
            return;
        }

        if (!hasRowKey) {
            // Upsert directly
            delegate.appendInsertPage(dataPage, updatePositions, updatePositionCount);
            return;
        }

        dataPage = dataPage.getColumns(reservedChannelsForUpdate.stream().mapToInt(Integer::intValue).toArray());
        int columnCount = dataPage.getChannelCount();
        Block[] updateBlocks = new Block[columnCount + rowIdFields.size()];
        for (int channel = 0; channel < columnCount; channel++) {
            updateBlocks[channel] = dataPage.getBlock(channel).getPositions(updatePositions, 0, updatePositionCount);
        }
        for (int field = 0; field < rowIdFields.size(); field++) {
            updateBlocks[field + columnCount] = rowIdFields.get(field).getPositions(updatePositions, 0, updatePositionCount);
        }

        updateSink.appendPage(new Page(updatePositionCount, updateBlocks));
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkArgument(page.getChannelCount() == 2 + columnCount, "The page size should be 2 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        int positionCount = page.getPositionCount();
        Block operationBlock = page.getBlock(columnCount);

        int[] dataChannel = IntStream.range(0, columnCount).toArray();
        Page dataPage = page.getColumns(dataChannel);

        int[] insertPositions = new int[positionCount];
        int insertPositionCount = 0;
        int[] deletePositions = new int[positionCount];
        int deletePositionCount = 0;
        int[] updatePositions = new int[positionCount];
        int updatePositionCount = 0;

        for (int position = 0; position < positionCount; position++) {
            int operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> {
                    insertPositions[insertPositionCount] = position;
                    insertPositionCount++;
                }
                case DELETE_OPERATION_NUMBER -> {
                    deletePositions[deletePositionCount] = position;
                    deletePositionCount++;
                }
                case UPDATE_OPERATION_NUMBER -> {
                    updatePositions[updatePositionCount] = position;
                    updatePositionCount++;
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
            }
        }

        List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columnCount + 1));
        delegate.appendInsertPage(dataPage, insertPositions, insertPositionCount);
        delegate.appendDeletePage(rowIdFields, deletePositions, deletePositionCount);
        appendUpdatePage(dataPage, rowIdFields, updatePositions, updatePositionCount);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return delegate.finish();
    }
}
