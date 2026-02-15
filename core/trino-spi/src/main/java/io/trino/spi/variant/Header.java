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

import static io.trino.spi.variant.Header.BasicType.PRIMITIVE;
import static io.trino.spi.variant.Header.BasicType.SHORT_STRING;
import static io.trino.spi.variant.VariantUtils.checkArgument;

public final class Header
{
    public static final int SHORT_STRING_MAX_LENGTH = 63;
    public static final byte VERSION = 0x01;

    private Header() {}

    public static BasicType getBasicType(byte header)
    {
        return BasicType.fromHeader(header);
    }

    public static PrimitiveType getPrimitiveType(byte header)
    {
        return PrimitiveType.fromHeader(header);
    }

    public static EquivalenceClass getEquivalenceClass(byte header)
    {
        return EquivalenceClass.fromHeader(header);
    }

    public static int shortStringLength(byte header)
    {
        // Bits 2-7 represent the length of the short string
        return (header & 0b1111_1100) >>> 2;
    }

    /// The number of bytes used to encode the field offsets.
    /// The value is between 1 and 4.
    @SuppressWarnings("JavaExistingMethodCanBeUsed")
    public static int objectFieldOffsetSize(byte header)
    {
        // Bits 2-3 represent the size of the object field offsets
        int sizeBits = (header & 0b0000_1100) >>> 2;
        return sizeBits + 1;
    }

    /// The number of bytes used to encode the field ids
    /// The value is between 1 and 4.
    public static int objectFieldIdSize(byte header)
    {
        // Bits 4-5 represent the size of the object field IDs
        int sizeBits = (header & 0b0011_0000) >>> 4;
        return sizeBits + 1;
    }

    // If true, 4 bytes are used to encode the field count; otherwise, 1 byte is used.
    public static boolean objectIsLarge(byte header)
    {
        // Bit 6 represents whether the object is large
        return (header & 0b0100_0000) != 0;
    }

    /// The number of bytes used to encode the array field offsets.
    public static int arrayFieldOffsetSize(byte header)
    {
        // Bits 2-3 represent the size of the array field offsets
        int sizeBits = (header & 0b0000_1100) >>> 2;
        return sizeBits + 1;
    }

    /// If true, 4 bytes are used to encode the element count; otherwise, 1 byte is used.
    public static boolean arrayIsLarge(byte header)
    {
        // Bit 4 represents whether the array is large
        return (header & 0b0001_0000) != 0;
    }

    public static byte primitiveHeader(PrimitiveType primitiveType)
    {
        return (byte) (PRIMITIVE.ordinal() | primitiveType.ordinal() << 2);
    }

    public static byte shortStringHeader(int length)
    {
        checkArgument(length >= 0 && length <= SHORT_STRING_MAX_LENGTH, () -> "Short string length must be between 0 and %s: %s".formatted(SHORT_STRING_MAX_LENGTH, length));
        return (byte) (SHORT_STRING.ordinal() | (length << 2));
    }

    public static byte objectHeader(int fieldIdSize, int fieldOffsetSize, boolean isLarge)
    {
        // Bits 0-1 represent the basic type
        int header = BasicType.OBJECT.ordinal();
        // Bits 2-3 represent the size of the field offsets
        checkArgument(fieldOffsetSize >= 1 && fieldOffsetSize <= 4, () -> "fieldOffsetSize must be between 1 and 4: %s".formatted(fieldOffsetSize));
        header |= (fieldOffsetSize - 1) << 2;
        // Bits 4-5 represent the size of the field IDs
        checkArgument(fieldIdSize >= 1 && fieldIdSize <= 4, () -> "fieldIdSize must be between 1 and 4: %s".formatted(fieldIdSize));
        header |= (fieldIdSize - 1) << 4;
        // Bit 6 represents whether the object is large
        if (isLarge) {
            header |= 0b0100_0000;
        }
        return (byte) header;
    }

    public static byte arrayHeader(int fieldOffsetSize, boolean isLarge)
    {
        // Bits 0-1 represent the basic type
        int header = BasicType.ARRAY.ordinal();
        // Bits 2-3 represent the size of the field offsets
        checkArgument(fieldOffsetSize >= 1 && fieldOffsetSize <= 4, () -> "fieldOffsetSize must be between 1 and 4: %s".formatted(fieldOffsetSize));
        header |= (fieldOffsetSize - 1) << 2;
        // Bit 4 represents whether the array is large
        if (isLarge) {
            header |= 0b0001_0000;
        }
        return (byte) header;
    }

    public static int metadataVersion(byte header)
    {
        // Bits 0-3: version
        return header & 0b0000_1111;
    }

    public static boolean metadataIsSorted(byte header)
    {
        // Bit 4: sorted
        return (header & 0b0001_0000) != 0;
    }

    public static int metadataOffsetSize(byte header)
    {
        // Bits 6-7: offset size
        int sizeBits = (header & 0b1100_0000) >>> 6;
        return sizeBits + 1;
    }

    public static byte metadataHeader(boolean sorted, int offsetSize)
    {
        // Bits 0-3: version
        int header = VERSION;
        // Bit 4: sorted
        if (sorted) {
            header |= 0b0001_0000;
        }
        // Bit 6-7: offset size
        checkArgument(offsetSize >= 1 && offsetSize <= 4, () -> "offsetSize must be between 1 and 4: %s".formatted(offsetSize));
        header |= (offsetSize - 1) << 6;
        return (byte) header;
    }

    public enum BasicType
    {
        PRIMITIVE,
        SHORT_STRING,
        OBJECT,
        ARRAY;

        public boolean isContainer()
        {
            return this == OBJECT || this == ARRAY;
        }

        private static BasicType fromHeader(byte header)
        {
            int basicTypeBits = (header & 0b0000_0011);
            return values()[basicTypeBits];
        }
    }

    public enum PrimitiveType
    {
        NULL,
        BOOLEAN_TRUE,
        BOOLEAN_FALSE,
        INT8,
        INT16,
        INT32,
        INT64,
        DOUBLE,
        DECIMAL4,
        DECIMAL8,
        DECIMAL16,
        DATE,
        TIMESTAMP_UTC_MICROS,
        TIMESTAMP_NTZ_MICROS,
        FLOAT,
        BINARY,
        STRING,
        TIME_NTZ_MICROS,
        TIMESTAMP_UTC_NANOS,
        TIMESTAMP_NTZ_NANOS,
        UUID;

        private static PrimitiveType fromHeader(byte header)
        {
            // Bits 2-7 represent the primitive type
            int primitiveTypeBits = (header & 0b1111_1100) >>> 2;
            return values()[primitiveTypeBits];
        }
    }

    public enum EquivalenceClass
    {
        NULL,
        BOOLEAN,
        EXACT_NUMERIC,
        FLOAT,
        DOUBLE,
        DATE,
        TIME_NTZ,
        TIMESTAMP_UTC,
        TIMESTAMP_NTZ,
        BINARY,
        STRING,
        UUID,
        OBJECT,
        ARRAY;

        private static EquivalenceClass fromHeader(byte header)
        {
            return switch (BasicType.fromHeader(header)) {
                case PRIMITIVE -> switch (PrimitiveType.fromHeader(header)) {
                    case NULL -> NULL;
                    case BOOLEAN_TRUE, BOOLEAN_FALSE -> BOOLEAN;
                    case INT8, INT16, INT32, INT64, DECIMAL4, DECIMAL8, DECIMAL16 -> EXACT_NUMERIC;
                    case FLOAT -> FLOAT;
                    case DOUBLE -> DOUBLE;
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
