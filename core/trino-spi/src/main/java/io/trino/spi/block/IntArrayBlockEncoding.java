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

import static io.trino.spi.block.EncoderUtil.decodeNullBitsScalar;
import static io.trino.spi.block.EncoderUtil.decodeNullBitsVectorized;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBitsScalar;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBitsVectorized;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.lang.System.arraycopy;
import static java.util.Objects.checkFromIndexSize;

public class IntArrayBlockEncoding
        implements BlockEncoding
{
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    public static final String NAME = "INT_ARRAY";

    private final boolean vectorizeNullBitPacking;
    private final boolean vectorizeNullCompress;
    private final boolean vectorizeNullExpand;

    public IntArrayBlockEncoding(boolean vectorizeNullBitPacking, boolean vectorizeNullCompress, boolean vectorizeNullExpand)
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
        boolean[] isNull = intArrayBlock.getRawValueIsNull();
        int[] rawValues = intArrayBlock.getRawValues();

        if (vectorizeNullBitPacking) {
            encodeNullsAsBitsVectorized(sliceOutput, isNull, rawOffset, positionCount);
        }
        else {
            encodeNullsAsBitsScalar(sliceOutput, isNull, rawOffset, positionCount);
        }

        if (isNull == null) {
            sliceOutput.writeInts(rawValues, rawOffset, positionCount);
        }
        else {
            if (vectorizeNullCompress) {
                compactIntsWithNullsVectorized(sliceOutput, rawValues, isNull, rawOffset, positionCount);
            }
            else {
                compactIntsWithNullsScalar(sliceOutput, rawValues, isNull, rawOffset, positionCount);
            }
        }
    }

    @Override
    public IntArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
        if (valueIsNullPacked == null) {
            int[] values = new int[positionCount];
            sliceInput.readInts(values);
            return new IntArrayBlock(0, positionCount, null, values);
        }

        boolean[] valueIsNull;
        if (vectorizeNullBitPacking) {
            valueIsNull = decodeNullBitsVectorized(valueIsNullPacked, positionCount);
        }
        else {
            valueIsNull = decodeNullBitsScalar(valueIsNullPacked, positionCount);
        }
        if (vectorizeNullExpand) {
            return expandIntsWithNullsVectorized(sliceInput, positionCount, valueIsNull);
        }
        return expandIntsWithNullsScalar(sliceInput, positionCount, valueIsNullPacked, valueIsNull);
    }

    static void compactIntsWithNullsVectorized(SliceOutput sliceOutput, int[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        int[] compacted = new int[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < INT_SPECIES.loopBound(length); valuesIndex += INT_SPECIES.length()) {
            VectorMask<Integer> mask = INT_SPECIES.loadMask(isNull, valuesIndex + offset).not();
            IntVector.fromArray(INT_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += mask.trueCount();
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isNull[valuesIndex + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static void compactIntsWithNullsScalar(SliceOutput sliceOutput, int[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        int[] compacted = new int[length];
        int compactedIndex = 0;
        for (int i = 0; i < length; i++) {
            compacted[compactedIndex] = values[i + offset];
            compactedIndex += isNull[i + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static IntArrayBlock expandIntsWithNullsVectorized(SliceInput sliceInput, int positionCount, boolean[] valueIsNull)
    {
        if (valueIsNull.length != positionCount) {
            throw new IllegalArgumentException("valueIsNull length must match positionCount");
        }
        int nonNullPositionsCount = sliceInput.readInt();
        int nonNullIndex = positionCount - nonNullPositionsCount;
        int[] values = new int[positionCount];
        sliceInput.readInts(values, nonNullIndex, nonNullPositionsCount);

        int position = 0;
        if ((nonNullPositionsCount * (INT_SPECIES.length() * 4L)) <= positionCount) {
            // Selective loop with many nulls, specialized handling with a branching approach
            for (; position < nonNullIndex && nonNullIndex + INT_SPECIES.length() < values.length; position += INT_SPECIES.length()) {
                VectorMask<Integer> nullMask = INT_SPECIES.loadMask(valueIsNull, position);
                if (!nullMask.allTrue()) {
                    VectorMask<Integer> nonNullMask = nullMask.not();
                    IntVector nonNullValues = IntVector.fromArray(INT_SPECIES, values, nonNullIndex);
                    nonNullIndex += nonNullMask.trueCount();
                    nonNullValues
                            .expand(nonNullMask)
                            .intoArray(values, position);
                }
            }
        }
        else {
            // Branchless vectorized loop while the current position is still before the compacted starting offset,
            // and we can load a full vector of non-null values before the end of the array
            for (; position < nonNullIndex && nonNullIndex + INT_SPECIES.length() < values.length; position += INT_SPECIES.length()) {
                IntVector nonNullValues = IntVector.fromArray(INT_SPECIES, values, nonNullIndex);
                VectorMask<Integer> nonNullMask = INT_SPECIES.loadMask(valueIsNull, position).not();
                nonNullIndex += nonNullMask.trueCount();
                nonNullValues
                        .expand(nonNullMask)
                        .intoArray(values, position);
            }
        }
        for (; position < nonNullIndex; position++) {
            values[position] = valueIsNull[position] ? 0 : values[nonNullIndex++];
        }
        return new IntArrayBlock(0, positionCount, valueIsNull, values);
    }

    static IntArrayBlock expandIntsWithNullsScalar(SliceInput sliceInput, int positionCount, byte[] valueIsNullPacked, boolean[] valueIsNull)
    {
        if (valueIsNull.length != positionCount) {
            throw new IllegalArgumentException("valueIsNull length must match positionCount");
        }
        int[] values = new int[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        sliceInput.readInts(values, 0, nonNullPositionCount);
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
        return new IntArrayBlock(0, positionCount, valueIsNull, values);
    }
}
