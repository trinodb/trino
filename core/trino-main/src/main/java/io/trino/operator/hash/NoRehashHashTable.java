package io.trino.operator.hash;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

public interface NoRehashHashTable
        extends AutoCloseable
{
    int putIfAbsent(int position, Page page);

    int getSize();

    int getCapacity();

    long getEstimatedSize();

    @Override
    void close();

    void copyFrom(NoRehashHashTable other);

    void appendValuesTo(int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash);

    long getHash(int startPosition);

    long getHashCollisions();

    int isNull(int hashPosition, int index);

    GroupByHashTableEntries entries();

    GroupByHashTableEntries valuesTable();

    int hashTableSize();

    int groupToHashPosition(int groupId);

    int[] groupToHashPosition();

    boolean needRehash();

    int maxFill();
}
