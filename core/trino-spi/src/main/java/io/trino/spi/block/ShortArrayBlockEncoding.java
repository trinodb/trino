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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import jakarta.annotation.Nullable;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import static io.trino.spi.block.EncoderUtil.decodeNullBitsScalar;
import static io.trino.spi.block.EncoderUtil.decodeNullBitsVectorized;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBitsScalar;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBitsVectorized;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.lang.System.arraycopy;
import static java.util.Objects.checkFromIndexSize;

public class ShortArrayBlockEncoding
        implements BlockEncoding
{
    private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_PREFERRED;
    public static final String NAME = "SHORT_ARRAY";

    private final boolean vectorizeNullBitPacking;
    private final boolean vectorizeNullCompress;
    private final boolean vectorizeNullExpand;

    public ShortArrayBlockEncoding(boolean vectorizeNullBitPacking, boolean vectorizeNullCompress, boolean vectorizeNullExpand)
    {
        this.vectorizeNullBitPacking = vectorizeNullBitPacking;
        this.vectorizeNullCompress = vectorizeNullCompress;
        this.vectorizeNullExpand = vectorizeNullExpand;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return ShortArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        ShortArrayBlock shortArrayBlock = (ShortArrayBlock) block;
        int positionCount = shortArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = shortArrayBlock.getRawValuesOffset();
        @Nullable
        boolean[] isNull = shortArrayBlock.getRawValueIsNull();
        short[] rawValues = shortArrayBlock.getRawValues();

        if (vectorizeNullBitPacking) {
            encodeNullsAsBitsVectorized(sliceOutput, isNull, rawOffset, positionCount);
        }
        else {
            encodeNullsAsBitsScalar(sliceOutput, isNull, rawOffset, positionCount);
        }

        if (isNull == null) {
            sliceOutput.writeShorts(rawValues, rawOffset, positionCount);
        }
        else {
            if (vectorizeNullCompress) {
                compactShortsWithNullsVectorized(sliceOutput, rawValues, isNull, rawOffset, positionCount);
            }
            else {
                compactShortsWithNullsScalar(sliceOutput, rawValues, isNull, rawOffset, positionCount);
            }
        }
    }

    @Override
    public ShortArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);

        if (valueIsNullPacked == null) {
            short[] values = new short[positionCount];
            sliceInput.readShorts(values);
            return new ShortArrayBlock(0, positionCount, null, values);
        }

        boolean[] valueIsNull;
        if (vectorizeNullBitPacking) {
            valueIsNull = decodeNullBitsVectorized(valueIsNullPacked, positionCount);
        }
        else {
            valueIsNull = decodeNullBitsScalar(valueIsNullPacked, positionCount);
        }
        if (vectorizeNullExpand) {
            return expandShortsWithNullsVectorized(sliceInput, positionCount, valueIsNull);
        }
        return expandShortsWithNullsScalar(sliceInput, positionCount, valueIsNullPacked, valueIsNull);
    }

    static void compactShortsWithNullsVectorized(SliceOutput sliceOutput, short[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        short[] compacted = new short[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < SHORT_SPECIES.loopBound(length); valuesIndex += SHORT_SPECIES.length()) {
            VectorMask<Short> mask = SHORT_SPECIES.loadMask(isNull, valuesIndex + offset).not();
            ShortVector.fromArray(SHORT_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += mask.trueCount();
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isNull[valuesIndex + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeShorts(compacted, 0, compactedIndex);
    }

    static void compactShortsWithNullsScalar(SliceOutput sliceOutput, short[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        short[] compacted = new short[length];
        int compactedIndex = 0;
        for (int i = 0; i < length; i++) {
            compacted[compactedIndex] = values[i + offset];
            compactedIndex += isNull[i + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeShorts(compacted, 0, compactedIndex);
    }

    static ShortArrayBlock expandShortsWithNullsVectorized(SliceInput sliceInput, int positionCount, boolean[] valueIsNull)
    {
        if (valueIsNull.length != positionCount) {
            throw new IllegalArgumentException("valueIsNull length must match positionCount");
        }
        int nonNullPositionsCount = sliceInput.readInt();
        int nonNullIndex = positionCount - nonNullPositionsCount;
        short[] values = new short[positionCount];
        sliceInput.readShorts(values, nonNullIndex, nonNullPositionsCount);

        int position = 0;
        // Vectorized loop while the current position is still before the compacted starting offset,
        // and we can load a full vector of non-null values before the end of the array
        for (; position < nonNullIndex && nonNullIndex + SHORT_SPECIES.length() < values.length; position += SHORT_SPECIES.length()) {
            ShortVector nonNullValues = ShortVector.fromArray(SHORT_SPECIES, values, nonNullIndex);
            VectorMask<Short> nonNullMask = SHORT_SPECIES.loadMask(valueIsNull, position).not();
            nonNullIndex += nonNullMask.trueCount();
            nonNullValues
                    .expand(nonNullMask)
                    .intoArray(values, position);
        }
        for (; position < nonNullIndex; position++) {
            values[position] = valueIsNull[position] ? 0 : values[nonNullIndex++];
        }

        return new ShortArrayBlock(0, positionCount, valueIsNull, values);
    }

    static ShortArrayBlock expandShortsWithNullsScalar(SliceInput sliceInput, int positionCount, byte[] valueIsNullPacked, boolean[] valueIsNull)
    {
        if (valueIsNull.length != positionCount) {
            throw new IllegalArgumentException("valueIsNull length must match positionCount");
        }
        short[] values = new short[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        sliceInput.readShorts(values, 0, nonNullPositionCount);
        int position = nonNullPositionCount - 1;

        // Handle Last (positionCount % 8) values
        for (int i = positionCount - 1; i >= (positionCount & ~0b111) && position >= 0; i--) {
            values[i] = values[position];
            if (!valueIsNull[i]) {
                position--;
            }
        }

        // Handle the remaining positions.
        for (int i = (positionCount & ~0b111) - 8; i >= 0 && position >= 0; i -= 8) {
            byte packed = valueIsNullPacked[i >>> 3];
            if (packed == 0) { // Only values
                arraycopy(values, position - 7, values, i, 8);
                position -= 8;
            }
            else if (packed != -1) { // At least one non-null
                for (int j = i + 7; j >= i && position >= 0; j--) {
                    values[j] = values[position];
                    if (!valueIsNull[j]) {
                        position--;
                    }
                }
            }
            // Do nothing if there are only nulls
        }
        return new ShortArrayBlock(0, positionCount, valueIsNull, values);
    }
}
