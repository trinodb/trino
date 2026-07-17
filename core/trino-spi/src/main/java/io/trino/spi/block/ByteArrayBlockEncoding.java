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
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static java.lang.Long.bitCount;
import static java.util.Objects.checkFromIndexSize;

public class ByteArrayBlockEncoding
        implements BlockEncoding
{
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    public static final String NAME = "BYTE_ARRAY";

    private final boolean vectorizeNullCompress;
    private final boolean vectorizeNullExpand;

    public ByteArrayBlockEncoding(boolean vectorizeNullCompress, boolean vectorizeNullExpand)
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
        return ByteArrayBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        ByteArrayBlock byteArrayBlock = (ByteArrayBlock) block;
        int positionCount = byteArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = byteArrayBlock.getRawValuesOffset();
        @Nullable
        long[] valueIsValid = byteArrayBlock.getRawValueIsValid();
        byte[] rawValues = byteArrayBlock.getRawValues();

        encodeValidityAsLongs(sliceOutput, valueIsValid, rawOffset, positionCount);

        if (valueIsValid == null) {
            sliceOutput.writeBytes(rawValues, rawOffset, positionCount);
        }
        else if (vectorizeNullCompress) {
            compactBytesWithNullsVectorized(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
        else {
            compactBytesWithNulls(sliceOutput, rawValues, valueIsValid, rawOffset, positionCount);
        }
    }

    @Override
    public ByteArrayBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        if (valueIsValid == null) {
            byte[] values = new byte[positionCount];
            sliceInput.readBytes(values, 0, values.length);
            return new ByteArrayBlock(0, positionCount, null, values);
        }

        if (vectorizeNullExpand) {
            return expandBytesWithNullsVectorized(sliceInput, positionCount, valueIsValid);
        }
        return expandBytesWithNulls(sliceInput, positionCount, valueIsValid);
    }

    static void compactBytesWithNullsVectorized(SliceOutput sliceOutput, byte[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        byte[] compacted = new byte[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < BYTE_SPECIES.loopBound(length); valuesIndex += BYTE_SPECIES.length()) {
            long validBits = getBits(valueIsValid, offset, valuesIndex, BYTE_SPECIES.length());
            VectorMask<Byte> mask = VectorMask.fromLong(BYTE_SPECIES, validBits);
            ByteVector.fromArray(BYTE_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += bitCount(validBits);
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isSet(valueIsValid, offset, valuesIndex) ? 1 : 0;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeBytes(compacted, 0, compactedIndex);
    }

    static void compactBytesWithNulls(SliceOutput sliceOutput, byte[] values, long[] valueIsValid, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        byte[] compacted = new byte[length];
        int compactedIndex = 0;
        for (int position = 0; position < length; position++) {
            if (isSet(valueIsValid, offset, position)) {
                compacted[compactedIndex++] = values[position + offset];
            }
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeBytes(compacted, 0, compactedIndex);
    }

    static ByteArrayBlock expandBytesWithNulls(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        byte[] values = new byte[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        byte[] compacted = new byte[nonNullPositionCount];
        sliceInput.readBytes(compacted, 0, compacted.length);

        int compactedIndex = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isSet(valueIsValid, 0, position)) {
                values[position] = compacted[compactedIndex++];
            }
        }
        return new ByteArrayBlock(0, positionCount, valueIsValid, values);
    }

    static ByteArrayBlock expandBytesWithNullsVectorized(SliceInput sliceInput, int positionCount, long[] valueIsValid)
    {
        byte[] values = new byte[positionCount];
        int nonNullPositionCount = sliceInput.readInt();
        int nonNullIndex = positionCount - nonNullPositionCount;
        sliceInput.readBytes(values, nonNullIndex, nonNullPositionCount);

        int position = 0;
        // Vectorized loop while the current position is still before the compacted starting offset,
        // and we can load a full vector of non-null values before the end of the array
        for (; position < nonNullIndex && nonNullIndex + BYTE_SPECIES.length() < values.length; position += BYTE_SPECIES.length()) {
            long validBits = getBits(valueIsValid, 0, position, BYTE_SPECIES.length());
            ByteVector nonNullValues = ByteVector.fromArray(BYTE_SPECIES, values, nonNullIndex);
            VectorMask<Byte> nonNullMask = VectorMask.fromLong(BYTE_SPECIES, validBits);
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
        return new ByteArrayBlock(0, positionCount, valueIsValid, values);
    }
}
