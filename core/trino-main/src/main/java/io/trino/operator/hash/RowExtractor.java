package io.trino.operator.hash;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

public class RowExtractor
{
    private final int[] hashChannels;
    private final ColumnValueExtractor[] columnValueExtractors;

    public RowExtractor(int[] hashChannels, ColumnValueExtractor[] columnValueExtractors)
    {
        this.hashChannels = hashChannels;
        this.columnValueExtractors = columnValueExtractors;
    }

    public void copyToRow(Page page, int position, GroupByHashTableAccess row)
    {
//        row.clear();
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);
            byte isNull = (byte) (block.isNull(position) ? 1 : 0);
            row.putIsNull(0, i, isNull);
            if (isNull == 0) {
                columnValueExtractors[i].putValue(row, 0, i, block, position);
            }
            else {
                row.putNullValue(0, i);
            }
        }

        long hash = row.calculateValuesHash(0);
        row.putHash(0, hash);
    }

    public void writeValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
    {
        columnValueExtractors[valueIndex].appendValue(hashTable, position, valueIndex, blockBuilder);
    }
}
