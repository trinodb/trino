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

import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.countUsedPositions;
import static io.trino.spi.block.DecimalAggregationAccumulatorBlock.isMultiplicationOf8;
import static java.lang.Math.max;

public class DecimalAggregationAccumulatorBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalAggregationAccumulatorBlockBuilder.class).instanceSize();

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private final int entrySize;
    private final int numberOfNoOverflowValuesPerEntry;
    private final int maxOffset;

    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;
    private boolean hasNonZeroOverflowValue;

    // it is assumed that valueIsNull and overflow arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private long[] overflow = new long[0];

    private long[] values = new long[0];

    private long retainedSizeInBytes;

    private int entryPositionCount;

    public DecimalAggregationAccumulatorBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int numberOfNoOverflowValuesPerEntry)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);
        this.numberOfNoOverflowValuesPerEntry = numberOfNoOverflowValuesPerEntry;
        this.entrySize = numberOfNoOverflowValuesPerEntry * Long.BYTES + Long.BYTES;
        // offset used for overflow
        this.maxOffset = 8 * numberOfNoOverflowValuesPerEntry;

        updateDataSize();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }
        if (entryPositionCount == numberOfNoOverflowValuesPerEntry) {
            overflow[positionCount] = value;
            hasNonZeroOverflowValue |= value != 0;
        }
        else {
            values[(positionCount * numberOfNoOverflowValuesPerEntry) + entryPositionCount] = value;
        }
        entryPositionCount++;

        hasNonNullValue = true;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (entryPositionCount != numberOfNoOverflowValuesPerEntry + 1) {
            throw new IllegalStateException("Expected entry size to be exactly " + entrySize + " bytes but was " + (entryPositionCount * SIZE_OF_LONG));
        }

        positionCount++;
        entryPositionCount = 0;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + entrySize);
        }
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }
        if (entryPositionCount != 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + entrySize);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(nullValueBLock(entrySize), positionCount);
        }
        return new DecimalAggregationAccumulatorBlock(0, positionCount, hasNullValue ? valueIsNull : null, values, hasNonZeroOverflowValue ? overflow : null, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new DecimalAggregationAccumulatorBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount), numberOfNoOverflowValuesPerEntry);
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize * numberOfNoOverflowValuesPerEntry);
        overflow = Arrays.copyOf(overflow, newSize);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values) + sizeOf(overflow);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public long getSizeInBytes()
    {
        return (entrySize + Byte.BYTES) * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (entrySize + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (entrySize + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : entrySize;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, sizeOf(values));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(overflow, sizeOf(overflow));
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
            return overflow[position];
        }
        if (offset < 0 || !isMultiplicationOf8(offset) || offset > maxOffset) {
            throw new IllegalArgumentException("invalid offset " + offset);
        }
        int valuesOffset = offset / 8;

        return values[(position * numberOfNoOverflowValuesPerEntry) + valuesOffset];
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNullValue;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull[position];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int startIndex = position * numberOfNoOverflowValuesPerEntry;
        for (int i = 0; i < numberOfNoOverflowValuesPerEntry; i++) {
            blockBuilder.writeLong(values[startIndex + i]);
        }
        blockBuilder.writeLong(overflow[position]);
        blockBuilder.closeEntry();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new DecimalAggregationAccumulatorBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                compactArray(values, position * numberOfNoOverflowValuesPerEntry, numberOfNoOverflowValuesPerEntry),
                overflow[position] != 0 ? new long[] {overflow[position]} : null,
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
                newValueIsNull[i] = valueIsNull[position];
            }
            for (int j = 0; j < numberOfNoOverflowValuesPerEntry; j++) {
                newValues[(i * numberOfNoOverflowValuesPerEntry) + j] = values[(position * numberOfNoOverflowValuesPerEntry) + j];
            }
            if (overflow != null) {
                newOverflow[i] = overflow[position];
            }
        }
        return new DecimalAggregationAccumulatorBlock(0, length, newValueIsNull, newValues, newOverflow, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(nullValueBLock(entrySize), length);
        }
        return new DecimalAggregationAccumulatorBlock(positionOffset, length, hasNullValue ? valueIsNull : null, values, hasNonZeroOverflowValue ? overflow : null, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(nullValueBLock(entrySize), length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        }
        long[] newOverflow = hasNonZeroOverflowValue ? compactArray(overflow, positionOffset, length) : null;
        long[] newValues = compactArray(values, positionOffset * numberOfNoOverflowValuesPerEntry, length * numberOfNoOverflowValuesPerEntry);
        return new DecimalAggregationAccumulatorBlock(0, length, newValueIsNull, newValues, newOverflow, numberOfNoOverflowValuesPerEntry);
    }

    @Override
    public String getEncodingName()
    {
        return DecimalAggregationAccumulatorBlockEncoding.NAME;
    }

    @Override
    public int getSliceLength(int position)
    {
        return getEntrySizeInBytes();
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
        int startIndex = position * numberOfNoOverflowValuesPerEntry;
        long[] array = new long[numberOfNoOverflowValuesPerEntry + 1];
        for (int i = 0; i < numberOfNoOverflowValuesPerEntry; i++) {
            array[i] = values[startIndex + i];
        }
        array[numberOfNoOverflowValuesPerEntry] = overflow[position];
        return Slices.wrappedLongArray(array);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DecimalAggregationAccumulatorBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    public Slice getValuesSlice()
    {
        return Slices.wrappedLongArray(values, 0, positionCount * numberOfNoOverflowValuesPerEntry);
    }

    public Slice getOverflowSlice()
    {
        return hasNonZeroOverflowValue ? Slices.wrappedLongArray(overflow, 0, positionCount) : null;
    }

    public int getNumberOfNoOverflowValuesPerEntry()
    {
        return numberOfNoOverflowValuesPerEntry;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    private static DecimalAggregationAccumulatorBlock nullValueBLock(int numberOfNoOverflowValuesPerEntry)
    {
        return new DecimalAggregationAccumulatorBlock(
                0,
                1,
                new boolean[] {true},
                new long[numberOfNoOverflowValuesPerEntry],
                new long[1],
                numberOfNoOverflowValuesPerEntry);
    }

    private int getEntrySizeInBytes()
    {
        return numberOfNoOverflowValuesPerEntry * Long.BYTES + Long.BYTES;
    }
}
