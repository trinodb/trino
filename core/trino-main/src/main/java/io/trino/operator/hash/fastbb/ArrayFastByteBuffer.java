/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.hash.fastbb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

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
    private final Slice slice; // Slice that wraps the array for operations that require Slice
    private final int capacity;

    public ArrayFastByteBuffer(int capacity)
    {
        this.capacity = capacity;
        this.array = new byte[capacity];
        this.slice = Slices.wrappedBuffer(array);
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
        // alternative
//        return UNSAFE.getByte(array, BYTE_ARRAY_BASE_OFFSET + position);
        return array[position];
    }

    @Override
    public void put(int position, byte value)
    {
        array[position] = value;
        // alternative
//        UNSAFE.putByte(array, BYTE_ARRAY_BASE_OFFSET + position, value);
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
        // alternatives
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
        for (int i = 0; i < valueLength; i++) {
            array[position + i] = value[valueOffset + i];
        }
        // alternative
//        System.arraycopy(value, valueOffset, array, position, valueLength);
    }

    @Override
    public Slice asSlice()
    {
        return slice;
    }

    @Override
    public long getLong(int position)
    {
        return UNSAFE.getLong(array, BYTE_ARRAY_BASE_OFFSET + position);
    }

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

    // alternative
//    @Override
//    public boolean subArrayEquals(FastByteBuffer other, int thisOffset, int otherOffset, int length)
//    {
//        byte[] otherArray = ((ArrayFastByteBuffer) other).array;
//
//        return Arrays.equals(array, thisOffset, thisOffset + length, otherArray, otherOffset, otherOffset + length);

    //    }

    @Override
    public boolean subArrayEquals(Slice other, int thisOffset, int otherOffset, int length)
    {
        Object otherBase = other.getBase();
        long thisPosition = BYTE_ARRAY_BASE_OFFSET + thisOffset;
        long otherPosition = other.getAddress() + otherOffset;

        int i = 0;
        for (; i <= length - 8; i += 8) {
            if (UNSAFE.getLong(this.array, thisPosition) != UNSAFE.getLong(otherBase, otherPosition)) {
                return false;
            }
            thisPosition += 8;
            otherPosition += 8;
        }

        if (i <= length - 4) {
            if (UNSAFE.getInt(this.array, thisPosition) != UNSAFE.getInt(otherBase, otherPosition)) {
                return false;
            }
            thisPosition += 4;
            otherPosition += 4;
            i += 4;
        }

        for (; i < length; i++) {
            if (UNSAFE.getByte(this.array, thisPosition) != UNSAFE.getByte(otherBase, otherPosition)) {
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
