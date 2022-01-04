package io.trino.operator.hash;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

public interface RowExtractor
{
    void copyToEntriesTable(Page page, int position, GroupByHashTableEntries entries, int entriesPosition);

    void appendValuesTo(GroupByHashTableEntries entries, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash);

    GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount, int dataValuesLength);

    GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow, int dataValuesLength);

    int mainBufferValuesLength();

    void putEntry(GroupByHashTableEntries entries, int hashPosition, int groupId, Page page, int position, long rawHash);

    boolean keyEquals(GroupByHashTableEntries entries, int hashPosition, Page page, int position, long rawHash);
}
