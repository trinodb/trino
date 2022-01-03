package io.trino.operator.hash;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

public interface RowExtractor
{
    void copyToRow(Page page, int position, GroupByHashTableEntries entry);

    void appendValuesTo(GroupByHashTableEntries entries, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash);

    GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount, int dataValuesLength);

    GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow, int dataValuesLength);

    int mainBufferValuesLength();
}
