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
package io.trino.plugin.varada.storage.read.predicates;

import io.airlift.slice.Slice;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.plugin.varada.util.StringPredicateData;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.function.Function;

class RangesConverter
{
    static final byte INCLUSIVE = (byte) 1;
    static final byte EXCLUSIVE = (byte) 0;
    // Unbounded markers
    static final int INT_LOWER_UNBOUNDED = Integer.MIN_VALUE;
    static final int INT_UPPER_UNBOUNDED = Integer.MAX_VALUE;
    static final long LONG_LOWER_UNBOUNDED = Long.MIN_VALUE;
    static final long LONG_UPPER_UNBOUNDED = Long.MAX_VALUE;
    static final short SHORT_LOWER_UNBOUNDED = Short.MIN_VALUE;
    static final short SHORT_UPPER_UNBOUNDED = Short.MAX_VALUE;
    static final byte BYTE_LOWER_UNBOUNDED = Byte.MIN_VALUE;
    static final byte BYTE_UPPER_UNBOUNDED = Byte.MAX_VALUE;
    static final float FLOAT_LOWER_UNBOUNDED = Float.NEGATIVE_INFINITY;
    static final float FLOAT_UPPER_UNBOUNDED = Float.POSITIVE_INFINITY;
    static final double DOUBLE_LOWER_UNBOUNDED = Double.NEGATIVE_INFINITY;
    static final double DOUBLE_UPPER_UNBOUNDED = Double.POSITIVE_INFINITY;
    static final long LONG_DECIMAL_LOWER_UNBOUNDED_MSB = Long.MIN_VALUE;
    static final long LONG_DECIMAL_LOWER_UNBOUNDED_LSB = 0L;
    static final long LONG_DECIMAL_UPPER_UNBOUNDED_MSB = Long.MAX_VALUE;
    static final long LONG_DECIMAL_UPPER_UNBOUNDED_LSB = 0xffffffffffffffffL;

    RangesConverter()
    {
    }

    void setBooleanRanges(Ranges ranges, ByteBuffer lowBuf, ByteBuffer highBuf)
    {
        for (Range r : ranges.getOrderedRanges()) {
            // low
            if (r.isLowUnbounded()) {
                lowBuf.put((byte) 0);
                lowBuf.put(INCLUSIVE);
            }
            else {
                Boolean low = (Boolean) r.getLowBoundedValue();
                lowBuf.put(low ? (byte) 1 : (byte) 0);
                lowBuf.put(r.isLowInclusive()
                        ? INCLUSIVE
                        : EXCLUSIVE);
            }

            // high
            if (r.isHighUnbounded()) {
                highBuf.put((byte) 1);
                highBuf.put(INCLUSIVE);
            }
            else {
                Boolean high = (Boolean) r.getHighBoundedValue();
                highBuf.put(high ? (byte) 1 : (byte) 0);
                highBuf.put(r.isHighInclusive()
                        ? INCLUSIVE
                        : EXCLUSIVE);
            }
        }
    }

    void setIntRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putInt(INT_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putInt(IntegerType.INTEGER.getInt(sortedRangesBlock, 1));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putInt(IntegerType.INTEGER.getInt(sortedRangesBlock, i));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putInt(IntegerType.INTEGER.getInt(sortedRangesBlock, i + 1));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putInt(IntegerType.INTEGER.getInt(sortedRangesBlock, positionCount - 2));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putInt(INT_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setRealRanges(Ranges ranges, ByteBuffer lowBuf, ByteBuffer highBuf)
    {
        for (Range r : ranges.getOrderedRanges()) {
            // low
            if (r.isLowUnbounded()) {
                lowBuf.putFloat(FLOAT_LOWER_UNBOUNDED);
                lowBuf.put(INCLUSIVE); // inclusive
            }
            else {
                lowBuf.putInt(((Long) r.getLowBoundedValue()).intValue());
                lowBuf.put(r.isLowInclusive()
                        ? INCLUSIVE
                        : EXCLUSIVE); // ABOVE is the '0' case
            }

            // high
            if (r.isHighUnbounded()) {
                highBuf.putFloat(FLOAT_UPPER_UNBOUNDED);
                highBuf.put(INCLUSIVE); // inclusive
            }
            else {
                highBuf.putInt(((Long) r.getHighBoundedValue()).intValue());
                highBuf.put(r.isHighInclusive()
                        ? INCLUSIVE
                        : EXCLUSIVE); // BELOW is the '0' case
            }
        }
    }

    void setLongRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        Type type = sortedRangeSet.getType();
        if (lowUnbounded) {
            lowBuf.putLong(LONG_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putLong(type.getLong(sortedRangesBlock, 1));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putLong(type.getLong(sortedRangesBlock, i));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putLong(type.getLong(sortedRangesBlock, i + 1));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putLong(type.getLong(sortedRangesBlock, positionCount - 2));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putLong(LONG_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setLongDecimalRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        int positionCount = sortedRangesBlock.getPositionCount();
        Type type = sortedRangeSet.getType();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putLong(LONG_DECIMAL_LOWER_UNBOUNDED_MSB);
            lowBuf.putLong(LONG_DECIMAL_LOWER_UNBOUNDED_LSB);
            lowBuf.put(INCLUSIVE); // inclusive

            Int128 value = (Int128) type.getObject(sortedRangesBlock, 1);
            highBuf.putLong(value.getHigh());
            highBuf.putLong(value.getLow());
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            Int128 value = (Int128) type.getObject(sortedRangesBlock, i);
            lowBuf.putLong(value.getHigh());
            lowBuf.putLong(value.getLow());
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            value = (Int128) type.getObject(sortedRangesBlock, i + 1);
            highBuf.putLong(value.getHigh());
            highBuf.putLong(value.getLow());
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            Int128 value = (Int128) type.getObject(sortedRangesBlock, positionCount - 2);
            lowBuf.putLong(value.getHigh());
            lowBuf.putLong(value.getLow());
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putLong(LONG_DECIMAL_UPPER_UNBOUNDED_MSB);
            highBuf.putLong(LONG_DECIMAL_UPPER_UNBOUNDED_LSB);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setDoubleRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Type type = sortedRangeSet.getType();
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putDouble(DOUBLE_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putDouble(type.getDouble(sortedRangesBlock, 1));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putDouble(type.getDouble(sortedRangesBlock, i));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putDouble(type.getDouble(sortedRangesBlock, i + 1));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putDouble(type.getDouble(sortedRangesBlock, positionCount - 2));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putDouble(DOUBLE_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setTinyintRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.put(BYTE_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.put(TinyintType.TINYINT.getByte(sortedRangesBlock, 1));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.put(TinyintType.TINYINT.getByte(sortedRangesBlock, i));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.put(TinyintType.TINYINT.getByte(sortedRangesBlock, i + 1));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.put(TinyintType.TINYINT.getByte(sortedRangesBlock, positionCount - 2));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.put(BYTE_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setSmallIntRanges(ByteBuffer lowBuf, ByteBuffer highBuf, SortedRangeSet sortedRangeSet)
    {
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        int positionCount = sortedRangesBlock.getPositionCount();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        boolean lowUnbounded = isUnbounded(sortedRangesBlock, 0);
        boolean highUnbounded = isUnbounded(sortedRangesBlock, positionCount - 1);
        if (lowUnbounded) {
            lowBuf.putShort(SHORT_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            highBuf.putShort(SmallintType.SMALLINT.getShort(sortedRangesBlock, 1));
            highBuf.put(inclusive[1] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
        }
        int endRange = highUnbounded ? positionCount - 2 : positionCount;
        int startRange = lowUnbounded ? 2 : 0;
        for (int i = startRange; i < endRange; i += 2) {
            lowBuf.putShort(SmallintType.SMALLINT.getShort(sortedRangesBlock, i));
            lowBuf.put(inclusive[i] ? INCLUSIVE : EXCLUSIVE);
            highBuf.putShort(SmallintType.SMALLINT.getShort(sortedRangesBlock, i + 1));
            highBuf.put(inclusive[i + 1] ? INCLUSIVE : EXCLUSIVE);
        }
        if (highUnbounded) {
            lowBuf.putShort(SmallintType.SMALLINT.getShort(sortedRangesBlock, positionCount - 2));
            lowBuf.put(inclusive[positionCount - 2] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            highBuf.putShort(SHORT_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
        }
    }

    void setStringRanges(SortedRangeSet sortedRangeSet, ByteBuffer lowBuf, ByteBuffer highBuf, int recLength,
            Function<Slice, Slice> sliceConverter)
    {
        Type type = sortedRangeSet.getType();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        final int positionCount = sortedRangesBlock.getPositionCount();
        SliceUtils.StringPredicateDataFactory stringPredicateDataFactory = new SliceUtils.StringPredicateDataFactory();
        StringPredicateData stringPredicateData;
        int posIx = 0;

        if (isUnbounded(sortedRangesBlock, posIx)) {
            lowBuf.putLong(LONG_LOWER_UNBOUNDED);
            lowBuf.put(INCLUSIVE); // inclusive
            posIx++;

            Slice highSlice = sliceConverter.apply(type.getSlice(sortedRangesBlock, posIx));
            stringPredicateData = stringPredicateDataFactory.create(highSlice, recLength, false);
            highBuf.putLong(stringPredicateData.comperationValue());
            highBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE); // BELOW is the '0' case
            posIx++;
        }

        final int endPosIx = isUnbounded(sortedRangesBlock, positionCount - 1) ? positionCount - 2 : positionCount;
        while (posIx < endPosIx) {
            Slice lowSlice = sliceConverter.apply(type.getSlice(sortedRangesBlock, posIx));
            stringPredicateData = stringPredicateDataFactory.create(lowSlice, recLength, false);
            lowBuf.putLong(stringPredicateData.comperationValue());
            lowBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE);
            posIx++;

            Slice highSlice = sliceConverter.apply(type.getSlice(sortedRangesBlock, posIx));
            stringPredicateData = stringPredicateDataFactory.create(highSlice, recLength, false);
            highBuf.putLong(stringPredicateData.comperationValue());
            highBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE);
            posIx++;
        }

        if (posIx < positionCount) {
            Slice lowSlice = sliceConverter.apply(type.getSlice(sortedRangesBlock, posIx));
            stringPredicateData = stringPredicateDataFactory.create(lowSlice, recLength, false);
            lowBuf.putLong(stringPredicateData.comperationValue());
            lowBuf.put(inclusive[posIx] ? INCLUSIVE : EXCLUSIVE); // ABOVE is the '0' case
            posIx++;
            highBuf.putLong(LONG_UPPER_UNBOUNDED);
            highBuf.put(INCLUSIVE); // inclusive
            posIx++;
        }
    }

    private boolean isUnbounded(Block sortedRangesBlock, int position)
    {
        return sortedRangesBlock.isNull(position);
    }
}
