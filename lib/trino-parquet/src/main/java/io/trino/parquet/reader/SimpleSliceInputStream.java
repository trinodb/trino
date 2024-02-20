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
package io.trino.parquet.reader;

import com.google.common.primitives.Shorts;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.util.Objects.requireNonNull;

/**
 * Basic input stream based on a given Slice object.
 * This is a simpler version of BasicSliceInput with a few additional methods.
 * <p>
 * Note that methods starting with 'read' modify the underlying offset, while 'get' methods return
 * value without modifying the state
 */
public final class SimpleSliceInputStream
{
    private final Slice slice;
    private int offset;

    public SimpleSliceInputStream(Slice slice)
    {
        this(slice, 0);
    }

    public SimpleSliceInputStream(Slice slice, int offset)
    {
        this.slice = requireNonNull(slice, "slice is null");
        this.offset = offset;
    }

    public byte readByte()
    {
        return slice.getByte(offset++);
    }

    public short readShort()
    {
        short value = slice.getShort(offset);
        offset += Short.BYTES;
        return value;
    }

    public int readInt()
    {
        int value = slice.getInt(offset);
        offset += Integer.BYTES;
        return value;
    }

    public long readLong()
    {
        long value = slice.getLong(offset);
        offset += Long.BYTES;
        return value;
    }

    public byte[] readBytes()
    {
        byte[] bytes = slice.getBytes();
        offset = slice.length();
        return bytes;
    }

    public void readBytes(byte[] output, int outputOffset, int length)
    {
        slice.getBytes(offset, output, outputOffset, length);
        offset += length;
    }

    public void readShorts(short[] output, int outputOffset, int length)
    {
        slice.getShorts(offset, output, outputOffset, length);
        offset += length * Shorts.BYTES;
    }

    public void readInts(int[] output, int outputOffset, int length)
    {
        slice.getInts(offset, output, outputOffset, length);
        offset += length * Integer.BYTES;
    }

    public void readLongs(long[] output, int outputOffset, int length)
    {
        slice.getLongs(offset, output, outputOffset, length);
        offset += length * Long.BYTES;
    }

    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(offset, destination, destinationIndex, length);
        offset += length;
    }

    public Slice readSlice(int length)
    {
        Slice result = slice.slice(offset, length);
        offset += length;
        return result;
    }

    public void skip(int n)
    {
        offset += n;
    }

    public Slice asSlice()
    {
        return slice.slice(offset, slice.length() - offset);
    }

    /**
     * Returns the byte array wrapped by this Slice.
     * Callers should take care to use {@link SimpleSliceInputStream#getByteArrayOffset()}
     * since the contents of this Slice may not start at array index 0.
     */
    public byte[] getByteArray()
    {
        return slice.byteArray();
    }

    /**
     * Returns the start index the content of this slice within the byte array wrapped by this slice.
     */
    public int getByteArrayOffset()
    {
        return offset + slice.byteArrayOffset();
    }

    public void ensureBytesAvailable(int bytes)
    {
        checkPositionIndexes(offset, offset + bytes, slice.length());
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public int readIntUnsafe()
    {
        int value = slice.getIntUnchecked(offset);
        offset += Integer.BYTES;
        return value;
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public long readLongUnsafe()
    {
        long value = slice.getLongUnchecked(offset);
        offset += Long.BYTES;
        return value;
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public byte getByteUnsafe(int index)
    {
        return slice.getByteUnchecked(offset + index);
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public int getIntUnsafe(int index)
    {
        return slice.getIntUnchecked(offset + index);
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public long getLongUnsafe(int index)
    {
        return slice.getLongUnchecked(offset + index);
    }
}
