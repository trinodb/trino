package io.trino.operator.hash;

import io.airlift.slice.Slice;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ArrayFastByteBuffer
        implements FastByteBuffer
{
    private static final Unsafe UNSAFE = getUnsafe();
    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static Unsafe getUnsafe()
    {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private final byte[] array;
    private final int capacity;

    public ArrayFastByteBuffer(int capacity)
    {
        this.capacity = capacity;
        this.array = new byte[capacity];
    }

    @Override
    public void close()
    {
    }

    @Override
    public void copyFrom(FastByteBuffer src, int srcPosition, int destPosition, int length)
    {
        ArrayFastByteBuffer srcArray = (ArrayFastByteBuffer) src;
        System.arraycopy(srcArray.array, srcPosition, array, destPosition, length);
    }

    @Override
    public void putInt(int position, int value)
    {
        UNSAFE.putInt(array, BYTE_ARRAY_BASE_OFFSET + position, value);
    }

    @Override
    public int getInt(int position)
    {
        return UNSAFE.getInt(array, BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public int capacity()
    {
        return capacity;
    }

    @Override
    public void putLong(int position, long value)
    {
        UNSAFE.putLong(array, BYTE_ARRAY_BASE_OFFSET + position, value);
    }

    @Override
    public byte get(int position)
    {
//        return array[position];
        return UNSAFE.getByte(array, BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public void put(int position, byte value)
    {
        UNSAFE.putByte(array, BYTE_ARRAY_BASE_OFFSET + position, value);
//        array[position] = value;
    }

    @Override
    public short getShort(int position)
    {
        return UNSAFE.getShort(array, BYTE_ARRAY_BASE_OFFSET + position);
    }

    @Override
    public void putShort(int position, short value)
    {
        UNSAFE.putShort(array, BYTE_ARRAY_BASE_OFFSET + position, value);
    }

    @Override
    public void putSlice(int position, Slice value, int valueStartIndex, int valueLength)
    {
        Object base = value.getBase();
        long from = value.getAddress() + valueStartIndex;
        int to = position;
        long endFromIndex = value.getAddress() + valueStartIndex + valueLength;
        for (; from <= endFromIndex - 8; from += 8, to += 8) {
            UNSAFE.putLong(array, BYTE_ARRAY_BASE_OFFSET + to, UNSAFE.getLong(base, from));
        }
        if (from <= endFromIndex - 4) {
            UNSAFE.putInt(array, BYTE_ARRAY_BASE_OFFSET + to, UNSAFE.getInt(base, from));
            from += 4;
            to += 4;
        }
        for (; from < endFromIndex; from++, to++) {
            array[to] = UNSAFE.getByte(base, from);
        }
//        UNSAFE.copyMemory(base, valueOffset, array, BYTE_ARRAY_BASE_OFFSET + position, valueLength);
//        value.getBytes(valueStartIndex, array, position, valueLength);
    }

    @Override
    public void getSlice(int position, int length, Slice out, int slicePosition)
    {
        out.setBytes(slicePosition, array, position, length);
    }

    @Override
    public void put(int position, byte[] value, int valueOffset, int valueLength)
    {
        System.arraycopy(value, valueOffset, array, position, valueLength);
    }

    @Override
    public long getLong(int position)
    {
        return UNSAFE.getLong(array, BYTE_ARRAY_BASE_OFFSET + position);
    }

//    @Override
//    public boolean subArrayEquals(FastByteBuffer other, int thisOffset, int otherOffset, int length)
//    {
//        byte[] otherArray = ((ArrayFastByteBuffer) other).array;
//
//        return Arrays.equals(array, thisOffset, thisOffset + length, otherArray, otherOffset, otherOffset + length);

    //    }
    @Override
    public boolean subArrayEquals(FastByteBuffer other, int thisOffset, int otherOffset, int length)
    {
        long thisPosition = BYTE_ARRAY_BASE_OFFSET + thisOffset;
        long otherPosition = BYTE_ARRAY_BASE_OFFSET + otherOffset;
        byte[] otherArray = ((ArrayFastByteBuffer) other).array;

        int i = 0;
        for (; i <= length - 8; i += 8) {
            if (UNSAFE.getLong(this.array, thisPosition) != UNSAFE.getLong(otherArray, otherPosition)) {
                return false;
            }
            thisPosition += 8;
            otherPosition += 8;
        }

        if (i <= length - 4) {
            if (UNSAFE.getInt(this.array, thisPosition) != UNSAFE.getInt(otherArray, otherPosition)) {
                return false;
            }
            thisPosition += 4;
            otherPosition += 4;
            i += 4;
        }

        for (; i < length; i++) {
            if (UNSAFE.getByte(this.array, thisPosition) != UNSAFE.getByte(otherArray, otherPosition)) {
                return false;
            }
            thisPosition++;
            otherPosition++;
        }
        return true;
    }

    @Override
    public void clear(int upToPosition)
    {
        UNSAFE.setMemory(array, BYTE_ARRAY_BASE_OFFSET, upToPosition, (byte) 0);
    }
}
