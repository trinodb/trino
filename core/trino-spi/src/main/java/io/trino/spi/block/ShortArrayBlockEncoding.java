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

import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static java.lang.Long.bitCount;
import static java.util.Objects.checkFromIndexSize;

public class ShortArrayBlockEncoding
        implements BlockEncoding
{
    private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_PREFERRED;
    public static final String NAME = "SHORT_ARRAY";

    private final boolean vectorizeNullCompress;
    private final boolean vectorizeNullExpand;

    public ShortArrayBlockEncoding(boolean vectorizeNullCompress, boolean vectorizeNullExpand)
    {
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
        long[] valueIsValid = shortArrayBlock.getRawValueIsValid();
        short[] rawValues = shortArrayBlock.getRawValues();

        encodeValidityAsLongs(sliceOutput, valueIsValid, rawOffset, positionCount);

        if (valueIsValid == null) {
            sliceOutput.writeShorts(rawValues, rawOffset, positionCount);
        }
        else if (vectorizeNullCompress) {
            compactShortsWithNullsVectorized(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
        else {
            compactShortsWithNulls(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
    }

    @Override
    public ShortArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        if (valueIsValid == null) {
            short[] values = new short[positionCount];
            sliceInput.readShorts(values);
            return new ShortArrayBlock(0, positionCount, null, values);
        }

        if (vectorizeNullExpand) {
            return expandShortsWithNullsVectorized(sliceInput, positionCount, valueIsValid);
        }
        return expandShortsWithNulls(sliceInput, positionCount, valueIsValid);
    }

    static void compactShortsWithNullsVectorized(SliceOutput sliceOutput, short[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        short[] compacted = new short[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < SHORT_SPECIES.loopBound(length); valuesIndex += SHORT_SPECIES.length()) {
            long validBits = getBits(valueIsValid, offset, valuesIndex, SHORT_SPECIES.length());
            VectorMask<Short> mask = VectorMask.fromLong(SHORT_SPECIES, validBits);
            ShortVector.fromArray(SHORT_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += bitCount(validBits);
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isSet(valueIsValid, offset, valuesIndex) ? 1 : 0;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeShorts(compacted, 0, compactedIndex);
    }

    static void compactShortsWithNulls(SliceOutput sliceOutput, short[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        short[] compacted = new short[length];
        int compactedIndex = 0;
        for (int position = 0; position < length; position++) {
            if (isSet(valueIsValid, offset, position)) {
                compacted[compactedIndex++] = values[position + offset];
            }
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeShorts(compacted, 0, compactedIndex);
    }

    static ShortArrayBlock expandShortsWithNulls(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        short[] values = new short[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        short[] compacted = new short[nonNullPositionCount];
        sliceInput.readShorts(compacted);

        int compactedIndex = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isSet(valueIsValid, 0, position)) {
                values[position] = compacted[compactedIndex++];
            }
        }
        return new ShortArrayBlock(0, positionCount, valueIsValid, values);
    }

    static ShortArrayBlock expandShortsWithNullsVectorized(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        short[] values = new short[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        int nonNullIndex = positionCount - nonNullPositionCount;
        sliceInput.readShorts(values, nonNullIndex, nonNullPositionCount);

        int position = 0;
        // Vectorized loop while the current position is still before the compacted starting offset,
        // and we can load a full vector of non-null values before the end of the array
        for (; position < nonNullIndex && nonNullIndex + SHORT_SPECIES.length() < values.length; position += SHORT_SPECIES.length()) {
            long validBits = getBits(valueIsValid, 0, position, SHORT_SPECIES.length());
            ShortVector nonNullValues = ShortVector.fromArray(SHORT_SPECIES, values, nonNullIndex);
            VectorMask<Short> nonNullMask = VectorMask.fromLong(SHORT_SPECIES, validBits);
            nonNullIndex += bitCount(validBits);
            nonNullValues
                    .expand(nonNullMask)
                    .intoArray(values, position);
        }
        for (; position < nonNullIndex; position++) {
            if (isSet(valueIsValid, 0, position)) {
                values[position] = values[nonNullIndex++];
            }
        }

        return new ShortArrayBlock(0, positionCount, valueIsValid, values);
    }
}
