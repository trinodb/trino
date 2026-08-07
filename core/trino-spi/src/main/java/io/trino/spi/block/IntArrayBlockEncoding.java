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
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static java.lang.Long.bitCount;
import static java.util.Objects.checkFromIndexSize;

public class IntArrayBlockEncoding
        implements BlockEncoding
{
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    public static final String NAME = "INT_ARRAY";

    private final boolean vectorizeNullCompress;
    private final boolean vectorizeNullExpand;

    public IntArrayBlockEncoding(boolean vectorizeNullCompress, boolean vectorizeNullExpand)
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
        return IntArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        int positionCount = intArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = intArrayBlock.getRawValuesOffset();
        @Nullable
        long[] valueIsValid = intArrayBlock.getRawValueIsValid();
        int[] rawValues = intArrayBlock.getRawValues();

        encodeValidityAsLongs(sliceOutput, valueIsValid, rawOffset, positionCount);

        if (valueIsValid == null) {
            sliceOutput.writeInts(rawValues, rawOffset, positionCount);
        }
        else if (vectorizeNullCompress) {
            compactIntsWithNullsVectorized(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
        else {
            compactIntsWithNulls(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
    }

    @Override
    public IntArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        if (valueIsValid == null) {
            int[] values = new int[positionCount];
            sliceInput.readInts(values);
            return new IntArrayBlock(0, positionCount, null, values);
        }

        if (vectorizeNullExpand) {
            return expandIntsWithNullsVectorized(sliceInput, positionCount, valueIsValid);
        }
        return expandIntsWithNulls(sliceInput, positionCount, valueIsValid);
    }

    static void compactIntsWithNullsVectorized(SliceOutput sliceOutput, int[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        int[] compacted = new int[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < INT_SPECIES.loopBound(length); valuesIndex += INT_SPECIES.length()) {
            long validBits = getBits(valueIsValid, offset, valuesIndex, INT_SPECIES.length());
            VectorMask<Integer> mask = VectorMask.fromLong(INT_SPECIES, validBits);
            IntVector.fromArray(INT_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += bitCount(validBits);
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isSet(valueIsValid, offset, valuesIndex) ? 1 : 0;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static void compactIntsWithNulls(SliceOutput sliceOutput, int[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        int[] compacted = new int[length];
        int compactedIndex = 0;
        for (int position = 0; position < length; position++) {
            if (isSet(valueIsValid, offset, position)) {
                compacted[compactedIndex++] = values[position + offset];
            }
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static IntArrayBlock expandIntsWithNulls(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        int[] values = new int[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        int[] compacted = new int[nonNullPositionCount];
        sliceInput.readInts(compacted);

        int compactedIndex = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isSet(valueIsValid, 0, position)) {
                values[position] = compacted[compactedIndex++];
            }
        }
        return new IntArrayBlock(0, positionCount, valueIsValid, values);
    }

    static IntArrayBlock expandIntsWithNullsVectorized(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        int[] values = new int[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        int nonNullIndex = positionCount - nonNullPositionCount;
        sliceInput.readInts(values, nonNullIndex, nonNullPositionCount);

        int position = 0;
        if ((nonNullPositionCount * (INT_SPECIES.length() * 4L)) <= positionCount) {
            for (; position < nonNullIndex && nonNullIndex + INT_SPECIES.length() < values.length; position += INT_SPECIES.length()) {
                long validBits = getBits(valueIsValid, 0, position, INT_SPECIES.length());
                if (validBits != 0) {
                    IntVector nonNullValues = IntVector.fromArray(INT_SPECIES, values, nonNullIndex);
                    VectorMask<Integer> nonNullMask = VectorMask.fromLong(INT_SPECIES, validBits);
                    nonNullIndex += bitCount(validBits);
                    nonNullValues
                            .expand(nonNullMask)
                            .intoArray(values, position);
                }
            }
        }
        else {
            for (; position < nonNullIndex && nonNullIndex + INT_SPECIES.length() < values.length; position += INT_SPECIES.length()) {
                long validBits = getBits(valueIsValid, 0, position, INT_SPECIES.length());
                IntVector nonNullValues = IntVector.fromArray(INT_SPECIES, values, nonNullIndex);
                VectorMask<Integer> nonNullMask = VectorMask.fromLong(INT_SPECIES, validBits);
                nonNullIndex += bitCount(validBits);
                nonNullValues
                        .expand(nonNullMask)
                        .intoArray(values, position);
            }
        }
        for (; position < nonNullIndex; position++) {
            if (isSet(valueIsValid, 0, position)) {
                values[position] = values[nonNullIndex++];
            }
        }
        return new IntArrayBlock(0, positionCount, valueIsValid, values);
    }
}
