package io.trino.operator;

import jdk.internal.util.ArraysSupport;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class OffHeapByteBuffer
{
    static final Unsafe UNSAFE = getUnsafe();

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

    long address;
    private final int capacity;

    public OffHeapByteBuffer(int capacity)
    {
        this.capacity = capacity;
        this.address = UNSAFE.allocateMemory(capacity);
    }

    public static OffHeapByteBuffer allocate(int capacity)
    {
        return new OffHeapByteBuffer(capacity);
    }

    public void close()
    {
        UNSAFE.freeMemory(address);
    }

    public void copyFrom(OffHeapByteBuffer src, int srcPosition, int destPosition, int length)
    {
        UNSAFE.copyMemory(src.address + srcPosition, address + destPosition, length);
    }

    public void putInt(int position, int value)
    {
        UNSAFE.putInt(address + position, value);
    }

    public int getInt(int position)
    {
        return UNSAFE.getInt(address + position);
    }

    public int capacity()
    {
        return capacity;
    }

    public void putLong(int position, long value)
    {
        UNSAFE.putLong(address + position, value);
    }

    public byte get(int position)
    {
        return UNSAFE.getByte(address + position);
    }

    public void put(int position, byte value)
    {
        UNSAFE.putByte(address + position, value);
    }

    public long getLong(int position)
    {
        return UNSAFE.getLong(address + position);
    }

    public boolean subArrayEquals(OffHeapByteBuffer other, int thisOffset, int otherOffset, int length)
    {
        long thisPosition = address + thisOffset;
        long otherPosition = other.address + otherOffset;

        int i = 0;
        for (; i <= length - 8; i += 8) {
            if (UNSAFE.getLong(thisPosition) != UNSAFE.getLong(otherPosition)) {
                return false;
            }
            thisPosition += 8;
            otherPosition += 8;
        }

        if (i <= length - 4) {
            if (UNSAFE.getInt(thisPosition) != UNSAFE.getInt(otherPosition)) {
                return false;
            }
            thisPosition += 4;
            otherPosition += 4;
            i += 4;
        }

        for (; i < length; i++) {
            if (UNSAFE.getByte(thisPosition) != UNSAFE.getByte(otherPosition)) {
                return false;
            }
            thisPosition++;
            otherPosition++;
        }
        return true;
    }
}
