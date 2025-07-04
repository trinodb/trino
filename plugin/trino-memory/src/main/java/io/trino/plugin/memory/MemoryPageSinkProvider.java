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
package io.trino.plugin.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.memory.MemoryInsertTableHandle.InsertMode;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryPagesStore pagesStore;
    private final HostAddress currentHostAddress;

    @Inject
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, Node currentNode)
    {
        this(pagesStore, currentNode.getHostAndPort());
    }

    @VisibleForTesting
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, HostAddress currentHostAddress)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        MemoryOutputTableHandle memoryOutputTableHandle = (MemoryOutputTableHandle) outputTableHandle;
        long tableId = memoryOutputTableHandle.table();
        checkState(memoryOutputTableHandle.activeTableIds().contains(tableId));

        pagesStore.cleanUp(memoryOutputTableHandle.activeTableIds());
        pagesStore.initialize(tableId, memoryOutputTableHandle.rowIdIndex());
        return new MemoryPageSink(pagesStore, currentHostAddress, tableId, memoryOutputTableHandle.rowIdIndex());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        MemoryInsertTableHandle memoryInsertTableHandle = (MemoryInsertTableHandle) insertTableHandle;
        long tableId = memoryInsertTableHandle.table();
        checkState(memoryInsertTableHandle.activeTableIds().contains(tableId));

        if (memoryInsertTableHandle.mode() == InsertMode.OVERWRITE) {
            pagesStore.purge(tableId);
        }

        pagesStore.cleanUp(memoryInsertTableHandle.activeTableIds());
        pagesStore.initialize(tableId, memoryInsertTableHandle.rowIdIndex());
        return new MemoryPageSink(pagesStore, currentHostAddress, tableId, memoryInsertTableHandle.rowIdIndex());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) mergeHandle;
        pagesStore.initialize(memoryTableHandle.id(), memoryTableHandle.rowIdIndex());
        return new MemoryMergeSink(pagesStore, currentHostAddress, memoryTableHandle.id(), memoryTableHandle.rowIdIndex());
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final MemoryPagesStore pagesStore;
        private final HostAddress currentHostAddress;
        private final long tableId;
        private final int rowIdIndex;
        private final List<Page> appendedPages = new ArrayList<>();

        public MemoryPageSink(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId, int rowIdIndex)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
            this.rowIdIndex = rowIdIndex;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            appendedPages.add(addRowId(page));
            return NOT_BLOCKED;
        }

        private Page addRowId(Page page)
        {
            return addBlockAt(page, rowIdIndex, generateRowIdBlock(page.getPositionCount(), currentHostAddress.toString()));
        }

        private static Block generateRowIdBlock(int positionCount, String address)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
            for (int i = 0; i < positionCount; i++) {
                VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(UUID.randomUUID().toString()));
            }

            return RowBlock.fromFieldBlocks(positionCount, new Block[] {blockBuilder.build(), RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice(address), positionCount)});
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            // add pages to pagesStore
            long addedRows = 0;
            for (Page page : appendedPages) {
                pagesStore.add(tableId, page);
                addedRows += page.getPositionCount();
            }

            return completedFuture(ImmutableList.of(new MemoryDataFragment(currentHostAddress, addedRows).toSlice()));
        }

        @Override
        public void abort() {}
    }

    private static Page addBlockAt(Page page, int index, Block block)
    {
        checkState(page.getPositionCount() == block.getPositionCount(), "Error block: %s for page: %s", block.getPositionCount(), page.getPositionCount());
        int channelCount = page.getChannelCount();
        int positionCount = page.getPositionCount();
        if (index == channelCount) {
            return page.appendColumn(block);
        }

        Block[] blocks = new Block[channelCount + 1];
        checkState(index < channelCount, "rowIdIndex out of bounds");
        for (int channel = 0; channel < channelCount; channel++) {
            if (channel < index) {
                blocks[channel] = page.getBlock(channel);
            }
            else if (channel == index) {
                blocks[channel] = block;
                blocks[channel + 1] = page.getBlock(channel);
            }
            else {
                blocks[channel + 1] = page.getBlock(channel);
            }
        }
        return new Page(positionCount, blocks);
    }

    private static class MemoryMergeSink
            implements ConnectorMergeSink
    {
        private final ConnectorPageSink insertSink;
        private final MemoryPagesStore pagesStore;
        private final HostAddress currentHostAddress;
        private final long tableId;
        private final int rowIdIndex;

        private final List<Block> deleteRowIds = new ArrayList<>();
        private final List<Page> updatePages = new ArrayList<>();

        private MemoryMergeSink(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId, int rowIdIndex)
        {
            this.insertSink = new MemoryPageSink(pagesStore, currentHostAddress, tableId, rowIdIndex);
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
            this.rowIdIndex = rowIdIndex;
        }

        @Override
        public void storeMergedRows(Page page)
        {
            int channelCount = page.getChannelCount();
            Block operationBlock = page.getBlock(channelCount - 3);

            int[] dataChannel = IntStream.range(0, channelCount - 3).toArray();
            Page dataPage = page.getColumns(dataChannel);

            int positionCount = page.getPositionCount();
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

            Block rowIdBlock = page.getBlock(channelCount - 1);
            // insert should follow the original columns order
            if (insertPositionCount > 0) {
                insertSink.appendPage(dataPage.getPositions(insertPositions, 0, insertPositionCount));
            }

            // delete first than update since we want to reduce the memory usage first
            if (deletePositionCount > 0) {
                deleteRowIds.add(rowIdBlock.getPositions(deletePositions, 0, deletePositionCount));
            }

            if (updatePositionCount > 0) {
                updatePages.add(addBlockAt(dataPage.getPositions(updatePositions, 0, updatePositionCount), rowIdIndex, rowIdBlock.getPositions(updatePositions, 0, updatePositionCount)));
            }
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            ImmutableList.Builder<Slice> builder = ImmutableList.builder();
            insertSink.finish().thenAccept(builder::addAll);

            long removedCount = 0;
            for (Block block : deleteRowIds) {
                pagesStore.delete(tableId, block);
                removedCount -= block.getPositionCount();
            }
            builder.add(new MemoryDataFragment(currentHostAddress, removedCount).toSlice());

            for (Page page : updatePages) {
                pagesStore.update(tableId, page);
            }

            return completedFuture(builder.build());
        }
    }
}
