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
import io.airlift.slice.XxHash64;
import io.trino.spi.type.Int128;

import java.lang.runtime.ExactConversionsSupport;

import static io.trino.spi.variant.Header.BasicType.SHORT_STRING;
import static io.trino.spi.variant.Header.PrimitiveType;
import static io.trino.spi.variant.Header.PrimitiveType.DOUBLE;
import static io.trino.spi.variant.Header.PrimitiveType.FLOAT;
import static io.trino.spi.variant.Header.PrimitiveType.INT16;
import static io.trino.spi.variant.Header.PrimitiveType.INT32;
import static io.trino.spi.variant.Header.PrimitiveType.INT64;
import static io.trino.spi.variant.Header.PrimitiveType.INT8;
import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.getPrimitiveType;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.Header.shortStringLength;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static java.lang.Double.doubleToLongBits;

final class VariantHashing
{
    private VariantHashing() {}

    public static long hashCode(Metadata metadata, Slice slice, int offset)
    {
        VariantHash variantHash = new VariantHash(0);
        variantHash.hashVariant(metadata, slice, offset);
        return variantHash.finish();
    }

    private static final class VariantHash
    {
        private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
        private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
        private static final long PRIME64_3 = 0x165667B19E3779F9L;
        private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
        private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

        private long hash;
        private long totalLength;

        VariantHash(long seed)
        {
            this.hash = seed + PRIME64_5;
        }

        public void hashVariant(Metadata metadata, Slice slice, int offset)
        {
            byte header = slice.getByte(offset);
            ValueClass valueClass = ValueClass.classify(header);
            addInt(valueClass.hashTag());
            switch (valueClass) {
                case NULL -> addLong(0);
                case BOOLEAN -> addLong(getPrimitiveType(header) == PrimitiveType.BOOLEAN_TRUE ? 1 : 2);
                case NUMERIC -> hashNumeric(getPrimitiveType(header), slice, offset);
                case DATE -> addInt(slice.getInt(offset + 1));
                case TIME_NTZ -> addLong(slice.getLong(offset + 1));
                case TIMESTAMP_UTC, TIMESTAMP_NTZ -> hashTimestamp(header, slice, offset);
                case BINARY -> addBytesHash(slice, offset + 5, slice.getInt(offset + 1));
                case STRING -> hashStringLike(slice, offset, header);
                case UUID -> addBytesHash(slice, offset + 1, 16);
                case OBJECT -> hashObject(metadata, slice, offset);
                case ARRAY -> hashArray(metadata, slice, offset);
            }
        }

        private void hashNumeric(PrimitiveType primitiveType, Slice slice, int offset)
        {
            if (VariantUtils.isExactNumeric(primitiveType)) {
                hashCanonicalDecimal(VariantUtils.decodeExactNumericCanonical(primitiveType, slice, offset));
                return;
            }

            double value = VariantUtils.floatingAsDouble(primitiveType, slice, offset);
            if (Double.isNaN(value)) {
                addInt(4);
                return;
            }
            if (Double.isInfinite(value)) {
                addInt(value > 0 ? 2 : 3);
                return;
            }
            if (value == 0.0 || ExactConversionsSupport.isDoubleToLongExact(value)) {
                hashCanonicalLongDecimal((long) value);
                return;
            }

            VariantUtils.Decimal128Canonical decimal = VariantUtils.tryToDecimal128Exact(value);
            if (decimal != null) {
                hashCanonicalDecimal(decimal);
                return;
            }

            addInt(1);
            addDouble(value);
        }

        private void hashCanonicalDecimal(VariantUtils.Decimal128Canonical decimal)
        {
            addInt(0);
            if (decimal.scale() != 0) {
                addInt(decimal.scale());
            }
            Int128 unscaled = decimal.unscaled();
            if (VariantUtils.fitsInLong(unscaled)) {
                addLong(unscaled.getLow());
                return;
            }
            addLong(unscaled.getHigh());
            addLong(unscaled.getLow());
        }

        private void hashCanonicalLongDecimal(long value)
        {
            addInt(0);
            addLong(value);
        }

        private void addInt(int value)
        {
            addLong(value);
        }

        private void addLong(long value)
        {
            totalLength += Long.BYTES;
            hash ^= round(value);
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
        }

        private void addDouble(double value)
        {
            addLong(doubleToLongBits(value));
        }

        void addBytesHash(Slice slice, int offset, int length)
        {
            long bytesXxHash = XxHash64.hash(slice, offset, length);
            addBytesHash(bytesXxHash, length);
        }

        void addBytesHash(long bytesXxHash, int length)
        {
            totalLength += length;
            hash ^= round(bytesXxHash);
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
        }

        private void hashStringLike(Slice slice, int offset, byte header)
        {
            int stringOffset;
            int stringLength;
            if (getBasicType(header) == SHORT_STRING) {
                stringLength = shortStringLength(header);
                stringOffset = offset + 1;
            }
            else {
                stringLength = slice.getInt(offset + 1);
                stringOffset = offset + 5;
            }

            addBytesHash(slice, stringOffset, stringLength);
        }

        private void hashTimestamp(byte header, Slice slice, int offset)
        {
            long value = slice.getLong(offset + 1);

            PrimitiveType primitiveType = getPrimitiveType(header);
            if (primitiveType == PrimitiveType.TIMESTAMP_UTC_MICROS || primitiveType == PrimitiveType.TIMESTAMP_NTZ_MICROS) {
                addLong(0);
                addLong(value);
                return;
            }

            if (value % 1_000L == 0) {
                addLong(0);
                addLong(value / 1_000L);
                return;
            }
            addLong(1);
            addLong(value);
        }

        private void hashObject(Metadata metadata, Slice slice, int offset)
        {
            byte header = slice.getByte(offset);
            boolean large = objectIsLarge(header);
            int count = large ? slice.getInt(offset + 1) : (slice.getByte(offset + 1) & 0xFF);

            int idSize = objectFieldIdSize(header);
            int fieldOffsetSize = objectFieldOffsetSize(header);

            int idsStart = offset + 1 + (large ? 4 : 1);
            int offsetsStart = idsStart + count * idSize;
            int valuesStart = offsetsStart + (count + 1) * fieldOffsetSize;

            addInt(count);
            for (int index = 0; index < count; index++) {
                int fieldId = readOffset(slice, idsStart + index * idSize, idSize);
                Slice key = metadata.get(fieldId);

                int valueStart = valuesStart + readOffset(slice, offsetsStart + index * fieldOffsetSize, fieldOffsetSize);
                addBytesHash(key, 0, key.length());
                hashVariant(metadata, slice, valueStart);
            }
        }

        private void hashArray(Metadata metadata, Slice slice, int offset)
        {
            byte header = slice.getByte(offset);
            boolean large = arrayIsLarge(header);
            int count = large ? slice.getInt(offset + 1) : (slice.getByte(offset + 1) & 0xFF);

            int offsetSize = arrayFieldOffsetSize(header);
            int offsetsStart = offset + 1 + (large ? 4 : 1);
            int valuesStart = offsetsStart + (count + 1) * offsetSize;

            addInt(count);
            for (int index = 0; index < count; index++) {
                int elementStart = valuesStart + readOffset(slice, offsetsStart + index * offsetSize, offsetSize);
                hashVariant(metadata, slice, elementStart);
            }
        }

        long finish()
        {
            long h = hash + totalLength;

            h ^= h >>> 33;
            h *= PRIME64_2;
            h ^= h >>> 29;
            h *= PRIME64_3;
            h ^= h >>> 32;
            return h;
        }

        private static long round(long value)
        {
            return Long.rotateLeft(value * PRIME64_2, 31) * PRIME64_1;
        }
    }

    private enum ValueClass
    {
        NULL(0),
        BOOLEAN(1),
        NUMERIC(2),
        DATE(5),
        TIME_NTZ(6),
        TIMESTAMP_UTC(7),
        TIMESTAMP_NTZ(8),
        BINARY(9),
        STRING(10),
        UUID(11),
        OBJECT(12),
        ARRAY(13);

        private final int hashTag;

        ValueClass(int hashTag)
        {
            this.hashTag = hashTag;
        }

        private int hashTag()
        {
            return hashTag;
        }

        private static ValueClass classify(byte header)
        {
            return switch (getBasicType(header)) {
                case PRIMITIVE -> switch (getPrimitiveType(header)) {
                    case NULL -> NULL;
                    case BOOLEAN_TRUE, BOOLEAN_FALSE -> BOOLEAN;
                    case INT8, INT16, INT32, INT64, DECIMAL4, DECIMAL8, DECIMAL16, FLOAT, DOUBLE -> NUMERIC;
                    case DATE -> DATE;
                    case TIME_NTZ_MICROS -> TIME_NTZ;
                    case TIMESTAMP_UTC_MICROS, TIMESTAMP_UTC_NANOS -> TIMESTAMP_UTC;
                    case TIMESTAMP_NTZ_MICROS, TIMESTAMP_NTZ_NANOS -> TIMESTAMP_NTZ;
                    case BINARY -> BINARY;
                    case STRING -> STRING;
                    case UUID -> UUID;
                };
                case SHORT_STRING -> STRING;
                case OBJECT -> OBJECT;
                case ARRAY -> ARRAY;
            };
        }
    }
}
