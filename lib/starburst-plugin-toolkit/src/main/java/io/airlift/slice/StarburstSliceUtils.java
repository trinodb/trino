/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.airlift.slice;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class uses internal unsafe Slice methods for improved performance.
 */
public class StarburstSliceUtils
{
    private static final int SHORTS_IN_LONG = Long.BYTES / Short.BYTES;

    private StarburstSliceUtils() {}

    /**
     * Fill byte array with a value. Fills 8 values at a time
     *
     * @param length Number of LONG values to write i.e. number of bytes / 8
     */
    public static void fillArray8(byte[] output, int outputOffset, int length, byte baseValue)
    {
        int lengthInBytes = length * Long.BYTES;
        Slice tmp = Slices.wrappedBuffer(output, outputOffset, lengthInBytes);
        checkArgument(output.length - outputOffset >= lengthInBytes, "Trying to write values out of array bounds");
        long value = fillLong(baseValue);
        for (int i = 0; i < lengthInBytes; i += Long.BYTES) {
            tmp.setLongUnchecked(i, value);
        }
    }

    /**
     * Fill short array with a value. Fills 4 values at a time
     *
     * @param length Number of LONG values to write i.e. number of shorts / 4
     */
    public static void fillArray4(short[] output, int outputOffset, int length, short baseValue)
    {
        Slice tmp = Slices.wrappedShortArray(output, outputOffset, length * SHORTS_IN_LONG);
        checkArgument(output.length - outputOffset >= length * SHORTS_IN_LONG, "Trying to write values out of array bounds");
        long value = fillLong(baseValue);
        for (int i = 0; i < length * Long.BYTES; i += Long.BYTES) {
            tmp.setLongUnchecked(i, value);
        }
    }

    /**
     * @return long value made out of the argument concatenated 8 times
     */
    private static long fillLong(byte baseValue)
    {
        long value = ((baseValue & 0xFF) << 8) | (baseValue & 0xFF);
        value = (value << 16) | value;
        value = (value << 32) | value;
        return value;
    }

    /**
     * @return long value made out of the argument concatenated 4 times
     */
    private static long fillLong(short baseValue)
    {
        long value = ((long) (baseValue & 0xFFFF) << 16) | (baseValue & 0xFFFF);
        value = (value << 32) | value;
        return value;
    }
}
