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
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static java.lang.Long.bitCount;
import static java.util.Objects.checkFromIndexSize;

public class LongArrayBlockEncoding
        implements BlockEncoding
{
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    public static final String NAME = "LONG_ARRAY";

    private final boolean vectorizeNullCompress;
    private final boolean vectorizeNullExpand;

    public LongArrayBlockEncoding(boolean vectorizeNullCompress, boolean vectorizeNullExpand)
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
        return LongArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        int positionCount = longArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = longArrayBlock.getRawValuesOffset();
        @Nullable
        long[] valueIsValid = longArrayBlock.getRawValueIsValid();
        long[] rawValues = longArrayBlock.getRawValues();

        encodeValidityAsLongs(sliceOutput, valueIsValid, rawOffset, positionCount);

        if (valueIsValid == null) {
            sliceOutput.writeLongs(rawValues, rawOffset, positionCount);
        }
        else if (vectorizeNullCompress) {
            compactLongsWithNullsVectorized(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
        else {
            compactLongsWithNulls(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
    }

    @Override
    public LongArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        if (valueIsValid == null) {
            long[] values = new long[positionCount];
            sliceInput.readLongs(values);
            return new LongArrayBlock(0, positionCount, null, values);
        }

        if (vectorizeNullExpand) {
            return expandLongsWithNullsVectorized(sliceInput, positionCount, valueIsValid);
        }
        return expandLongsWithNulls(sliceInput, positionCount, valueIsValid);
    }

    static void compactLongsWithNullsVectorized(SliceOutput sliceOutput, long[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        long[] compacted = new long[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < LONG_SPECIES.loopBound(length); valuesIndex += LONG_SPECIES.length()) {
            long validBits = getBits(valueIsValid, offset, valuesIndex, LONG_SPECIES.length());
            VectorMask<Long> mask = VectorMask.fromLong(LONG_SPECIES, validBits);
            LongVector.fromArray(LONG_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += bitCount(validBits);
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isSet(valueIsValid, offset, valuesIndex) ? 1 : 0;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeLongs(compacted, 0, compactedIndex);
    }

    static void compactLongsWithNulls(SliceOutput sliceOutput, long[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        long[] compacted = new long[length];
        int compactedIndex = 0;
        for (int position = 0; position < length; position++) {
            if (isSet(valueIsValid, offset, position)) {
                compacted[compactedIndex++] = values[position + offset];
            }
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeLongs(compacted, 0, compactedIndex);
    }

    static LongArrayBlock expandLongsWithNulls(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        long[] values = new long[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        long[] compacted = new long[nonNullPositionCount];
        sliceInput.readLongs(compacted);

        int compactedIndex = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isSet(valueIsValid, 0, position)) {
                values[position] = compacted[compactedIndex++];
            }
        }
        return new LongArrayBlock(0, positionCount, valueIsValid, values);
    }

    static LongArrayBlock expandLongsWithNullsVectorized(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        long[] values = new long[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        int nonNullIndex = positionCount - nonNullPositionCount;
        sliceInput.readLongs(values, nonNullIndex, nonNullPositionCount);

        int position = 0;
        if ((nonNullPositionCount * (LONG_SPECIES.length() * 4L)) <= positionCount) {
            for (; position < nonNullIndex && nonNullIndex + LONG_SPECIES.length() < values.length; position += LONG_SPECIES.length()) {
                long validBits = getBits(valueIsValid, 0, position, LONG_SPECIES.length());
                if (validBits != 0) {
                    LongVector nonNullValues = LongVector.fromArray(LONG_SPECIES, values, nonNullIndex);
                    VectorMask<Long> nonNullMask = VectorMask.fromLong(LONG_SPECIES, validBits);
                    nonNullIndex += bitCount(validBits);
                    nonNullValues
                            .expand(nonNullMask)
                            .intoArray(values, position);
                }
            }
        }
        else {
            for (; position < nonNullIndex && nonNullIndex + LONG_SPECIES.length() < values.length; position += LONG_SPECIES.length()) {
                long validBits = getBits(valueIsValid, 0, position, LONG_SPECIES.length());
                LongVector nonNullValues = LongVector.fromArray(LONG_SPECIES, values, nonNullIndex);
                VectorMask<Long> nonNullMask = VectorMask.fromLong(LONG_SPECIES, validBits);
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

        return new LongArrayBlock(0, positionCount, valueIsValid, values);
    }
}
