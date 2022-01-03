package io.trino.operator.hash.fixed;

import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.FastByteBuffer;
import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.RowExtractor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;

public class FixedOffsetRowExtractor
        implements RowExtractor
{
    private static final int MAX_VAR_WIDTH_BUFFER_SIZE = 16;
    private final int[] hashChannels;
    private final ColumnValueExtractor[] columnValueExtractors;
    private final int[] mainBufferOffsets;
    private final int[] valueOffsets;
    private final byte[] isNull;
    private final int mainBufferValuesLength;

    public FixedOffsetRowExtractor(int[] hashChannels, ColumnValueExtractor[] columnValueExtractors)
    {
        checkArgument(hashChannels.length == columnValueExtractors.length);
        this.hashChannels = hashChannels;
        this.columnValueExtractors = columnValueExtractors;

        valueOffsets = new int[hashChannels.length];
        isNull = new byte[hashChannels.length];
        mainBufferOffsets = new int[hashChannels.length];
        int mainBufferOffset = 0;
        for (int i = 0; i < columnValueExtractors.length; i++) {
            mainBufferOffsets[i] = mainBufferOffset;
            mainBufferOffset += calculateMainBufferSize(columnValueExtractors[i]);
        }
        this.mainBufferValuesLength = mainBufferOffset;
    }

    public static int calculateMainBufferSize(ColumnValueExtractor columnValueExtractor)
    {
        int bufferSize = columnValueExtractor.getSize();
        if (columnValueExtractor.isFixedSize()) {
            return bufferSize;
        }

        if (bufferSize > MAX_VAR_WIDTH_BUFFER_SIZE) {
            bufferSize = MAX_VAR_WIDTH_BUFFER_SIZE;
        }

        return bufferSize;
    }

    public void copyToRow(Page page, int position, FixedOffsetsGroupByHashTableEntries row)
    {
//        row.clear();
        boolean overflow = false;
        int offset = 0;
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);
            boolean valueIsNull = block.isNull(position);
            isNull[i] = (byte) (valueIsNull ? 1 : 0);

//            int valueLength = valueIsNull ? 0 : columnValueExtractors[i].getSerializedValueLength(block, position);
//            valueOffsets[i] = offset;
//            offset += valueLength;
//            if (valueLength > MAX_VAR_WIDTH_BUFFER_SIZE) {
//                overflow = true;
//            }
        }

        row.putIsNull(0, isNull);
        if (!overflow) {
            copyToMainBuffer(page, position, row);
        }
        else {
            /// put in overflow
//            row.reserveOverflowLength(0, offset);
            throw new UnsupportedOperationException();
        }

        long hash = row.calculateValuesHash(0);
        row.putHash(0, hash);
    }

    private void copyToMainBuffer(Page page, int position, FixedOffsetsGroupByHashTableEntries row)
    {
        row.markNoOverflow(0);
        int valuesOffset = row.getValuesOffset(0);
        FastByteBuffer mainBuffer = row.getMainBuffer();
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);

            columnValueExtractors[i].putValue(mainBuffer, valuesOffset + mainBufferOffsets[i], block, position);
        }
    }

    @Override
    public void copyToRow(Page page, int position, GroupByHashTableEntries entry)
    {
        copyToRow(page, position, (FixedOffsetsGroupByHashTableEntries) entry);
    }

    @Override
    public void appendValuesTo(GroupByHashTableEntries entries, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
    {
        FixedOffsetsGroupByHashTableEntries hashTable = (FixedOffsetsGroupByHashTableEntries) entries;
        boolean overflow = hashTable.isOverflow(hashPosition);
        if (!overflow) {
            FastByteBuffer mainBuffer = hashTable.getMainBuffer();
            int valuesOffset = hashTable.getValuesOffset(hashPosition);

            for (int i = 0; i < hashChannels.length; i++, outputChannelOffset++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
                if (hashTable.isNull(hashPosition, i) == 1) {
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
            BIGINT.writeLong(hashBlockBuilder, hashTable.getHash(hashPosition));
        }
    }

    @Override
    public GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount, int dataValuesLength)
    {
        return new FixedOffsetsGroupByHashTableEntries(1, FastByteBuffer.allocate(1024), hashChannelsCount, false, dataValuesLength);
    }

    @Override
    public GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow, int dataValuesLength)
    {
        return new FixedOffsetsGroupByHashTableEntries(hashCapacity, overflow, hashChannelsCount, true, dataValuesLength);
    }

    @Override
    public int mainBufferValuesLength()
    {
        return mainBufferValuesLength;
    }
}
