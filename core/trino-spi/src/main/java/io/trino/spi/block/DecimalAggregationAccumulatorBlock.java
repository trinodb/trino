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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.countUsedPositions;

public class DecimalAggregationAccumulatorBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalAggregationAccumulatorBlock.class).instanceSize();

    private final int positionOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final long[] values;
    @Nullable
    private final long[] overflow;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;
    private final int numberOfNoOverflowValuesPerEntry;
    private final int maxOffset;

    public DecimalAggregationAccumulatorBlock(int positionCount, Optional<boolean[]> valueIsNull, long[] values, Optional<long[]> overflow, int numberOfNoOverflowValuesPerEntry)
    {
        this(0, positionCount, valueIsNull.orElse(null), values, overflow.orElse(null), numberOfNoOverflowValuesPerEntry);
    }

    DecimalAggregationAccumulatorBlock(int positionOffset, int positionCount, boolean[] valueIsNull, long[] values, long[] overflow, int numberOfNoOverflowValuesPerEntry)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        this.positionOffset = positionOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.numberOfNoOverflowValuesPerEntry = numberOfNoOverflowValuesPerEntry;

        // offset used for overflow
        this.maxOffset = 8 * numberOfNoOverflowValuesPerEntry;

        if (values.length - (positionOffset * numberOfNoOverflowValuesPerEntry) < positionCount * numberOfNoOverflowValuesPerEntry) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        if (overflow != null && overflow.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("overflow length is less than positionCount");
        }
        this.overflow = overflow;

        sizeInBytes = (numberOfNoOverflowValuesPerEntry + Byte.BYTES + Long.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values) + sizeOf(overflow);
    }

    @Override
    public int getSliceLength(int position)
    {
        return getEntrySizeInBytes();
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return getEntrySizeInBytes() * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return getEntrySizeInBytes() * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : getEntrySizeInBytes();
    }

    private int getEntrySizeInBytes()
    {
        return numberOfNoOverflowValuesPerEntry * Long.BYTES + Long.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, sizeOf(values));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        if (overflow != null) {
            consumer.accept(overflow, sizeOf(overflow));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset == maxOffset) {
            // overflow
            return getOverflow(position);
        }
        if (offset < 0 || !isMultiplicationOf8(offset) || offset > maxOffset) {
            throw new IllegalArgumentException("invalid offset " + offset);
        }
        int valuesOffset = offset / 8;

        return values[((position + positionOffset) * numberOfNoOverflowValuesPerEntry) + valuesOffset];
    }

    static boolean isMultiplicationOf8(int number)
    {
        return (number & 7) == 0;
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull != null && valueIsNull[position + positionOffset];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int startIndex = (position + positionOffset) * numberOfNoOverflowValuesPerEntry;
        for (int i = 0; i < numberOfNoOverflowValuesPerEntry; i++) {
            blockBuilder.writeLong(values[startIndex + i]);
        }
        blockBuilder.writeLong(getOverflow(position));
        blockBuilder.closeEntry();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        long overflowValue = getOverflow(position);
        return new DecimalAggregationAccumulatorBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                compactArray(values, (position + positionOffset) * numberOfNoOverflowValuesPerEntry, numberOfNoOverflowValuesPerEntry),
                overflowValue != 0 ? new long[] {overflowValue} : null,
                numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length * numberOfNoOverflowValuesPerEntry];
        long[] newOverflow = null;
        if (overflow != null) {
            newOverflow = new long[length];
        }
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + positionOffset];
            }
            for (int j = 0; j < numberOfNoOverflowValuesPerEntry; j++) {
                newValues[(i * numberOfNoOverflowValuesPerEntry) + j] = values[((position + positionOffset) * numberOfNoOverflowValuesPerEntry) + j];
            }
            if (overflow != null) {
                newOverflow[i] = overflow[position + positionOffset];
            }
        }
        return new DecimalAggregationAccumulatorBlock(0, length, newValueIsNull, newValues, newOverflow, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new DecimalAggregationAccumulatorBlock(positionOffset + this.positionOffset, length, valueIsNull, values, overflow, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += this.positionOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);
        long[] newValues = compactArray(values, positionOffset * numberOfNoOverflowValuesPerEntry, length * numberOfNoOverflowValuesPerEntry);
        long[] newOverflow = overflow == null ? null : compactArray(overflow, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values && newOverflow == overflow) {
            return this;
        }
        return new DecimalAggregationAccumulatorBlock(0, length, newValueIsNull, newValues, newOverflow, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        if (length != getEntrySizeInBytes()) {
            throw new IllegalArgumentException("length different than entrySizeInBytes: " + length);
        }
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero but got: " + offset);
        }
        int startIndex = (positionOffset + position) * numberOfNoOverflowValuesPerEntry;
        long[] array = new long[numberOfNoOverflowValuesPerEntry + 1];
        for (int i = 0; i < numberOfNoOverflowValuesPerEntry; i++) {
            array[i] = values[startIndex + i];
        }
        array[numberOfNoOverflowValuesPerEntry] = getOverflow(position);
        return Slices.wrappedLongArray(array);
    }

    @Override
    public String getEncodingName()
    {
        return DecimalAggregationAccumulatorBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DecimalAggregationAccumulatorBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    public Slice getValuesSlice()
    {
        return Slices.wrappedLongArray(
                values,
                positionOffset * numberOfNoOverflowValuesPerEntry,
                positionCount * numberOfNoOverflowValuesPerEntry);
    }

    public Slice getOverflowSlice()
    {
        return overflow == null ?
                null :
                Slices.wrappedLongArray(
                        overflow,
                        positionOffset,
                        positionCount);
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    public int getNumberOfNoOverflowValuesPerEntry()
    {
        return numberOfNoOverflowValuesPerEntry;
    }

    private long getOverflow(int position)
    {
        return overflow != null ? overflow[position + positionOffset] : 0;
    }
}
