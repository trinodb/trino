package io.trino.operator.hash;

public interface GroupByHashTableEntries
{
    int getEntrySize();

    long getHash(int position);

    boolean keyEquals(int position, GroupByHashTableEntries other, int otherPosition);

    void close();

    FastByteBuffer takeOverflow();

    byte isNull(int position, int i);

    void putEntry(int hashPosition, int groupId, GroupByHashTableEntries key);

    int capacity();

    String toString(int position);

    int getGroupId(int position);

    void copyEntryFrom(GroupByHashTableEntries src, int srcPosition, int toPosition);

    long getEstimatedSize();
}
