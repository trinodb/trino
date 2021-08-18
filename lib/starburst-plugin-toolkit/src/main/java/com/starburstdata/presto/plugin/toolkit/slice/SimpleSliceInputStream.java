/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.toolkit.slice;

import io.airlift.slice.Slice;
import io.airlift.slice.UnsafeSlice;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.util.Objects.requireNonNull;

/**
 * Basic input stream based on a given Slice object.
 * It is a final class with no inheritance so it performs better than BasicSliceInput
 * The class is not considered fully safe as methods suffixed with 'unsafe' keyword used without
 * manual bound check may result in a JVM crash
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

    public void ensureBytesAvailable(int bytes)
    {
        checkPositionIndexes(offset, offset + bytes, slice.length());
    }

    /**
     * Use this method only if there is a significant performance boost validated in benchmarks.
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public long readLongUnsafe()
    {
        long value = UnsafeSlice.getLongUnchecked(slice, offset);
        offset += Long.BYTES;
        return value;
    }

    /**
     * Use this method only if there is a significant performance boost validated in benchmarks.
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public int readIntUnsafe()
    {
        int value = UnsafeSlice.getIntUnchecked(slice, offset);
        offset += Integer.BYTES;
        return value;
    }

    public byte[] readBytes(int length)
    {
        byte[] bytes = slice.getBytes(offset, length);
        offset += length;
        return bytes;
    }

    public void readBytes(byte[] output, int outputOffset, int length)
    {
        slice.getBytes(offset, output, outputOffset, length);
        offset += length;
    }

    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(offset, destination, destinationIndex, length);
        offset += length;
    }

    public byte[] readBytes()
    {
        byte[] bytes = slice.getBytes();
        offset = slice.length();
        return bytes;
    }

    /**
     * Use this method only if there is a significant performance boost validated in benchmarks.
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public byte getByteUnsafe(int index)
    {
        return UnsafeSlice.getByteUnchecked(slice, offset + index);
    }

    /**
     * Use this method only if there is a significant performance boost validated in benchmarks.
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public int getIntUnsafe(int index)
    {
        return UnsafeSlice.getIntUnchecked(slice, offset + index);
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
}
