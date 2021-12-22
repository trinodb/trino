package io.trino.operator.hash;

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

    void clear(int upToPosition);

    default void putByteUnsigned(int position, int value)
    {
        put(position, (byte) value);
    }

    default int getByteUnsigned(int position)
    {
        return Byte.toUnsignedInt(get(position));
    }
}
