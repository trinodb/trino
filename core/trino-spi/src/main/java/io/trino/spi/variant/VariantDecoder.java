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
package io.trino.spi.variant;

import io.airlift.slice.Slice;

import java.util.Arrays;

import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getPrimitiveType;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.Header.shortStringLength;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BOOLEAN_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BYTE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DATE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL4_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DOUBLE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_FLOAT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_INT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_LONG_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_NULL_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_SHORT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIMESTAMP_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIME_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_UUID_SIZE;
import static io.trino.spi.variant.VariantUtils.checkState;
import static io.trino.spi.variant.VariantUtils.readOffset;

public final class VariantDecoder
{
    private VariantDecoder() {}

    public static int valueSize(Slice data, int offset)
    {
        byte header = data.getByte(offset);
        return switch (Header.getBasicType(header)) {
            case PRIMITIVE -> switch (getPrimitiveType(header)) {
                case NULL -> ENCODED_NULL_SIZE;
                case BOOLEAN_TRUE, BOOLEAN_FALSE -> ENCODED_BOOLEAN_SIZE;
                case INT8 -> ENCODED_BYTE_SIZE;
                case INT16 -> ENCODED_SHORT_SIZE;
                case INT32 -> ENCODED_INT_SIZE;
                case INT64 -> ENCODED_LONG_SIZE;
                case DOUBLE -> ENCODED_DOUBLE_SIZE;
                case DECIMAL4 -> ENCODED_DECIMAL4_SIZE;
                case DECIMAL8 -> ENCODED_DECIMAL8_SIZE;
                case DECIMAL16 -> ENCODED_DECIMAL16_SIZE;
                case DATE -> ENCODED_DATE_SIZE;
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS, TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> ENCODED_TIMESTAMP_SIZE;
                case FLOAT -> ENCODED_FLOAT_SIZE;
                case BINARY, STRING -> 5 + data.getInt(offset + 1);
                case TIME_NTZ_MICROS -> ENCODED_TIME_SIZE;
                case UUID -> ENCODED_UUID_SIZE;
            };
            case SHORT_STRING -> 1 + shortStringLength(header);
            case ARRAY -> arraySize(data, offset, header);
            case OBJECT -> objectSize(data, offset, header);
        };
    }

    private static int arraySize(Slice data, int offset, byte header)
    {
        boolean large = arrayIsLarge(header);
        int offsetSize = arrayFieldOffsetSize(header);
        int count = large ? data.getInt(offset + 1) : (data.getByte(offset + 1) & 0xFF);
        int offsetsStart = offset + 1 + (large ? 4 : 1);
        int valuesStart = offsetsStart + (count + 1) * offsetSize;
        return valuesStart - offset + readOffset(data, offsetsStart + count * offsetSize, offsetSize);
    }

    private static int objectSize(Slice data, int offset, byte header)
    {
        boolean large = objectIsLarge(header);
        int offsetSize = objectFieldOffsetSize(header);
        int idSize = objectFieldIdSize(header);
        int count = large ? data.getInt(offset + 1) : (data.getByte(offset + 1) & 0xFF);
        int idsStart = offset + 1 + (large ? 4 : 1);
        int offsetsStart = idsStart + count * idSize;
        int valuesStart = offsetsStart + (count + 1) * offsetSize;
        return valuesStart - offset + readOffset(data, offsetsStart + count * offsetSize, offsetSize);
    }

    public static VariantLayout decode(Slice data, int offset)
    {
        byte header = data.getByte(offset);
        return switch (Header.getBasicType(header)) {
            case PRIMITIVE, SHORT_STRING -> PrimitiveLayout.PRIMITIVE;
            case ARRAY -> ArrayLayout.decode(data, offset, header);
            case OBJECT -> ObjectLayout.decode(data, offset, header);
        };
    }

    public sealed interface VariantLayout
            permits PrimitiveLayout, ArrayLayout, ObjectLayout {}

    public enum PrimitiveLayout
            implements VariantLayout
    {
        PRIMITIVE
    }

    public record ArrayLayout(
            Slice data,
            int offset,
            int count,
            int offsetSize,
            int offsetsStart,
            int valuesStart)
            implements VariantLayout
    {
        static ArrayLayout decode(Slice data, int offset, byte header)
        {
            boolean large = arrayIsLarge(header);
            int offsetSize = arrayFieldOffsetSize(header);

            int count = large ? data.getInt(offset + 1) : (data.getByte(offset + 1) & 0xFF);
            int offsetsStart = offset + 1 + (large ? 4 : 1);
            int valuesStart = offsetsStart + (count + 1) * offsetSize;

            return new ArrayLayout(data, offset, count, offsetSize, offsetsStart, valuesStart);
        }

        int headerSize()
        {
            return valuesStart - offset;
        }

        int elementStart(int index)
        {
            int offsetPosition = offsetsStart + index * offsetSize;
            return valuesStart + readOffset(data, offsetPosition, offsetSize);
        }

        int elementEnd(int index)
        {
            int offsetPosition = offsetsStart + (index + 1) * offsetSize;
            return valuesStart + readOffset(data, offsetPosition, offsetSize);
        }
    }

    public record ObjectLayout(
            Slice data,
            int count,
            int idSize,
            int offsetSize,
            int idsStart,
            int offsetsStart,
            int valuesStart,
            int[] valueOffsets,
            int[] valueLengths)
            implements VariantLayout
    {
        static ObjectLayout decode(Slice data, int offset, byte header)
        {
            boolean large = objectIsLarge(header);
            int idSize = objectFieldIdSize(header);
            int offsetSize = objectFieldOffsetSize(header);

            int count = large ? data.getInt(offset + 1) : (data.getByte(offset + 1) & 0xFF);
            checkState(count >= 0, () -> "Corrupt object field count: " + count);
            int idsStart = offset + 1 + (large ? 4 : 1);
            int offsetsStart = idsStart + count * idSize;
            int valuesStart = offsetsStart + (count + 1) * offsetSize;
            if (count == 0) {
                return new ObjectLayout(data, count, idSize, offsetSize, idsStart, offsetsStart, valuesStart, new int[0], new int[0]);
            }

            int[] valueOffsets = new int[count];
            int[] valueLengths = new int[count];
            for (int index = 0; index < count; index++) {
                valueOffsets[index] = readOffset(data, offsetsStart + index * offsetSize, offsetSize);
            }

            // Object encodings store field IDs in name order and store value start offsets,
            // but they do not store value lengths or guarantee that the physical value order
            // matches the field-ID order. To derive lengths for object traversal, readers must
            // find the next larger offset for each field. That makes object decoding
            // unavoidably more expensive than simple adjacent subtraction.
            long[] sortedOffsetIndexPairs = new long[count];
            for (int index = 0; index < count; index++) {
                sortedOffsetIndexPairs[index] = (((long) valueOffsets[index]) << Integer.SIZE) | index;
            }
            Arrays.sort(sortedOffsetIndexPairs);

            for (int index = count - 1, nextOffset = readOffset(data, offsetsStart + count * offsetSize, offsetSize); index >= 0; index--) {
                long pair = sortedOffsetIndexPairs[index];
                int start = (int) (pair >>> Integer.SIZE);
                int originalIndex = (int) pair;
                valueLengths[originalIndex] = nextOffset - start;
                nextOffset = start;
            }

            return new ObjectLayout(data, count, idSize, offsetSize, idsStart, offsetsStart, valuesStart, valueOffsets, valueLengths);
        }

        int fieldId(int index)
        {
            return readOffset(data, idsStart + index * idSize, idSize);
        }

        int valueStart(int index)
        {
            return valuesStart + valueOffsets[index];
        }

        int valueEnd(int index)
        {
            return valuesStart + valueOffsets[index] + valueLengths[index];
        }
    }
}
