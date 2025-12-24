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

import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.VariantUtils.readOffset;

public final class VariantDecoder
{
    private VariantDecoder() {}

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
            int valuesStart)
            implements VariantLayout
    {
        static ObjectLayout decode(Slice data, int offset, byte header)
        {
            boolean large = objectIsLarge(header);
            int idSize = objectFieldIdSize(header);
            int offsetSize = objectFieldOffsetSize(header);

            int count = large ? data.getInt(offset + 1) : (data.getByte(offset + 1) & 0xFF);
            int idsStart = offset + 1 + (large ? 4 : 1);
            int offsetsStart = idsStart + count * idSize;
            int valuesStart = offsetsStart + (count + 1) * offsetSize;

            return new ObjectLayout(data, count, idSize, offsetSize, idsStart, offsetsStart, valuesStart);
        }

        int fieldId(int index)
        {
            return readOffset(data, idsStart + index * idSize, idSize);
        }

        int valueStart(int index)
        {
            int offsetPosition = offsetsStart + index * offsetSize;
            return valuesStart + readOffset(data, offsetPosition, offsetSize);
        }

        int valueEnd(int index)
        {
            int offsetPosition = offsetsStart + (index + 1) * offsetSize;
            return valuesStart + readOffset(data, offsetPosition, offsetSize);
        }
    }
}
