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
package io.trino.operator.hash.fixed;

import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.HashTableRowFormat;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.BigintType.BIGINT;

public class FixedOffsetHashTableRowFormat
        implements HashTableRowFormat
{
    private final int maxVarWidthBufferSize;
    private final int hashChannelsCount;
    private final ColumnValueExtractor[] columnValueExtractors;
    private final int[] mainBufferOffsets;
    private final int mainBufferValuesLength;

    public FixedOffsetHashTableRowFormat(int maxVarWidthBufferSize, ColumnValueExtractor[] columnValueExtractors)
    {
        this.maxVarWidthBufferSize = maxVarWidthBufferSize;
        this.hashChannelsCount = columnValueExtractors.length;
        this.columnValueExtractors = columnValueExtractors;

        mainBufferOffsets = new int[hashChannelsCount];
        int mainBufferOffset = 0;
        for (int i = 0; i < columnValueExtractors.length; i++) {
            mainBufferOffsets[i] = mainBufferOffset;
            mainBufferOffset += calculateMainBufferSize(columnValueExtractors[i]);
        }
        this.mainBufferValuesLength = mainBufferOffset;
    }

    public int calculateMainBufferSize(ColumnValueExtractor columnValueExtractor)
    {
        return calculateMainBufferSize(columnValueExtractor, maxVarWidthBufferSize);
    }

    public static int calculateMainBufferSize(ColumnValueExtractor columnValueExtractor, int maxVarWidthBufferSize)
    {
        int bufferSize = columnValueExtractor.getSize();
        if (columnValueExtractor.isFixedSize()) {
            return bufferSize;
        }

        if (bufferSize > maxVarWidthBufferSize) {
            bufferSize = maxVarWidthBufferSize;
        }

        return bufferSize;
    }

    private void copyToMainBuffer(Page page, int position, FixedOffsetGroupByHashTableEntries row, int offset)
    {
        row.markNoOverflow(offset);
        int valuesOffset = row.getValuesOffset(offset);
        FastByteBuffer mainBuffer = row.getMainBuffer();
        for (int i = 0; i < hashChannelsCount; i++) {
            Block block = page.getBlock(i);

            columnValueExtractors[i].putValue(mainBuffer, valuesOffset + mainBufferOffsets[i], block, position);
        }
    }

    @Override
    public void copyToTable(Page page, int position, GroupByHashTableEntries table, int entriesPosition)
    {
        FixedOffsetGroupByHashTableEntries entries = (FixedOffsetGroupByHashTableEntries) table;
        putEntryValue(page, position, entries, entriesPosition);

        long hash = entries.calculateValuesHash(entriesPosition);
        entries.putHash(entriesPosition, hash);
    }

    @Override
    public void putEntry(GroupByHashTableEntries entries, int entriesPosition, int groupId, Page page, int position, long rawHash)
    {
        entries.putGroupId(entriesPosition, groupId);
        putEntryValue(page, position, (FixedOffsetGroupByHashTableEntries) entries, entriesPosition);
        entries.putHash(entriesPosition, rawHash);
    }

    private void putEntryValue(Page page, int position, FixedOffsetGroupByHashTableEntries entries, int entriesOffset)
    {
//        byte[] isNull = new byte[hashChannelsCount];
        boolean overflow = false;
        int offset = 0;
        for (int i = 0; i < hashChannelsCount; i++) {
            Block block = page.getBlock(i);
            boolean valueIsNull = block.isNull(position);
//            isNull[i] = (byte) (valueIsNull ? 1 : 0);
            entries.putIsNull(entriesOffset, i, (byte) (valueIsNull ? 1 : 0));

            int valueLength = valueIsNull ? 0 : columnValueExtractors[i].getSerializedValueLength(block, position);
//            valueOffsets[i] = offset;
            offset += valueLength;
            if (valueLength > maxVarWidthBufferSize) {
                overflow = true;
            }
        }

//        entries.putIsNull(entriesOffset, isNull);
        if (!overflow) {
            copyToMainBuffer(page, position, entries, entriesOffset);
        }
        else {
            /// put in overflow
//            entries.reserveOverflowLength(0, offset);
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean keyEquals(GroupByHashTableEntries entries, int entriesPosition, Page page, int position, long rawHash)
    {
        if (rawHash != entries.getHash(entriesPosition)) {
            return false;
        }

        boolean overflow = entries.isOverflow(entriesPosition);
        FixedOffsetGroupByHashTableEntries table = (FixedOffsetGroupByHashTableEntries) entries;
        if (!overflow) {
            int valuesOffset = table.getValuesOffset(entriesPosition);
            FastByteBuffer mainBuffer = table.getMainBuffer();
            return valuesEquals(table, entriesPosition, page, position, mainBuffer, valuesOffset);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    private boolean valuesEquals(FixedOffsetGroupByHashTableEntries table, int hashPosition,
            Page page, int position, FastByteBuffer buffer, int valuesOffset)
    {
        for (int i = 0; i < hashChannelsCount; i++) {
            Block block = page.getBlock(i);

            boolean blockValueNull = block.isNull(position);
            byte tableValueIsNull = table.isNull(hashPosition, i);
            if (blockValueNull) {
                return tableValueIsNull == 1;
            }
            if (tableValueIsNull == 1) {
                return false;
            }

            if (!columnValueExtractors[i].valueEquals(buffer, valuesOffset + mainBufferOffsets[i], block, position)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void appendValuesTo(GroupByHashTableEntries entries, int position, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
    {
        FixedOffsetGroupByHashTableEntries hashTable = (FixedOffsetGroupByHashTableEntries) entries;
        boolean overflow = hashTable.isOverflow(position);
        if (!overflow) {
            FastByteBuffer mainBuffer = hashTable.getMainBuffer();
            int valuesOffset = hashTable.getValuesOffset(position);

            for (int i = 0; i < hashChannelsCount; i++, outputChannelOffset++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
                if (hashTable.isNull(position, i) == 1) {
                    blockBuilder.appendNull();
                }
                else {
                    columnValueExtractors[i].appendValue(mainBuffer, valuesOffset + mainBufferOffsets[i], blockBuilder);
                }
            }
        }
        else {
            throw new UnsupportedOperationException();
        }

        if (outputHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(hashBlockBuilder, hashTable.getHash(position));
        }
    }

    @Override
    public GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount)
    {
        return FixedOffsetGroupByHashTableEntries.allocate(1, FastByteBuffer.allocate(1024), hashChannelsCount, false, mainBufferValuesLength);
    }

    @Override
    public GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow)
    {
        return FixedOffsetGroupByHashTableEntries.allocate(hashCapacity, overflow, hashChannelsCount, true, mainBufferValuesLength);
    }
}
