package io.trino.operator.hash;

import io.airlift.slice.Slice;

public interface FastByteBuffer
{
    static FastByteBuffer allocate(int capacity)
    {
        return OffHeapByteBuffer.USE_OFF_HEAP ? new OffHeapByteBuffer(capacity) : new ArrayFastByteBuffer(capacity);
    }

    void close();

    void copyFrom(FastByteBuffer src, int srcPosition, int destPosition, int length);

    void putInt(int position, int value);

    int getInt(int position);

    int capacity();

    void putLong(int position, long value);

    byte get(int position);

    void put(int position, byte value);

    long getLong(int position);

    boolean subArrayEquals(FastByteBuffer other, int thisOffset, int otherOffset, int length);

    default void clear()
    {
        clear(capacity());
    }

    boolean subArrayEquals(Slice other, int thisOffset, int otherOffset, int length);

    void clear(int upToPosition);

    default void putByteUnsigned(int position, int value)
    {
        put(position, (byte) value);
    }

    default int getByteUnsigned(int position)
    {
        return Byte.toUnsignedInt(get(position));
    }

    short getShort(int position);

    void putShort(int position, short value);

    void putSlice(int position, Slice value, int valueStartIndex, int valueLength);

    void getSlice(int position, int length, Slice out, int slicePosition);

    void put(int position, byte[] value, int valueOffset, int valueLength);

    default String toString(int position, int length)
    {
        int iMax = position + length - 1;
        if (iMax == -1) {
            return "[]";
        }

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = position; ; i++) {
            b.append(get(i));
            if (i == iMax) {
                return b.append(']').toString();
            }
            b.append(", ");
        }
    }
}
