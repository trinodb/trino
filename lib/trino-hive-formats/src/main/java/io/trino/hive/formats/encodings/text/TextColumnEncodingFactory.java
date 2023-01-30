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
package io.trino.hive.formats.encodings.text;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;

public class TextColumnEncodingFactory
        implements ColumnEncodingFactory
{
    private static final byte[] DEFAULT_SEPARATORS = new byte[] {
            1,  // Start of Heading
            2,  // Start of text
            3,  // End of Text
            4,  // End of Transmission
            5,  // Enquiry
            6,  // Acknowledge
            7,  // Bell
            8,  // Backspace
            // RESERVED 9,  // Horizontal Tab
            // RESERVED 10, // Line Feed
            11, // Vertical Tab
            // RESERVED 12, // Form Feed
            // RESERVED 13, // Carriage Return
            14, // Shift Out
            15, // Shift In
            16, // Data Link Escape
            17, // Device Control One
            18, // Device Control Two
            19, // Device Control Three
            20, // Device Control Four
            21, // Negative Acknowledge
            22, // Synchronous Idle
            23, // End of Transmission Block
            24, // Cancel
            25, // End of medium
            26, // Substitute
            // RESERVED 27, // Escape
            28, // File Separator
            29, // Group separator
            // RESERVED 30, // Record Separator
            // RESERVED 31, // Unit separator
    };
    public static final Slice DEFAULT_NULL_SEQUENCE = Slices.utf8Slice("\\N");

    private final Slice nullSequence;
    private final byte[] separators;
    private final Byte escapeByte;
    private final boolean lastColumnTakesRest;

    public TextColumnEncodingFactory()
    {
        this(
                DEFAULT_NULL_SEQUENCE,
                DEFAULT_SEPARATORS.clone(),
                null,
                false);
    }

    public TextColumnEncodingFactory(Slice nullSequence, byte[] separators, Byte escapeByte, boolean lastColumnTakesRest)
    {
        this.nullSequence = nullSequence;
        this.separators = separators;
        this.escapeByte = escapeByte;
        this.lastColumnTakesRest = lastColumnTakesRest;
    }

    public static byte[] getDefaultSeparators(int nestingLevels)
    {
        return Arrays.copyOf(DEFAULT_SEPARATORS, nestingLevels);
    }

    @Override
    public TextColumnEncoding getEncoding(Type type)
    {
        try {
            return getEncoding(type, 0);
        }
        catch (NotEnoughSeparatorsException e) {
            throw new IllegalArgumentException(format("Type %s requires %s nesting levels", type, e.getDepth()));
        }
    }

    private TextColumnEncoding getEncoding(Type type, int depth)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanEncoding(type, nullSequence);
        }
        if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
            return new LongEncoding(type, nullSequence);
        }
        if (type instanceof DecimalType) {
            return new DecimalEncoding(type, nullSequence);
        }
        if (REAL.equals(type)) {
            return new FloatEncoding(type, nullSequence);
        }
        if (DOUBLE.equals(type)) {
            return new DoubleEncoding(type, nullSequence);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return new StringEncoding(type, nullSequence, escapeByte);
        }
        if (VARBINARY.equals(type)) {
            // binary text encoding is not escaped
            return new BinaryEncoding(type, nullSequence);
        }
        if (DATE.equals(type)) {
            return new DateEncoding(type, nullSequence);
        }
        if (type instanceof TimestampType) {
            return new TimestampEncoding((TimestampType) type, nullSequence);
        }
        if (type instanceof ArrayType) {
            TextColumnEncoding elementEncoding = getEncoding(type.getTypeParameters().get(0), depth + 1);
            return new ListEncoding(
                    type,
                    nullSequence,
                    getSeparator(depth + 1),
                    escapeByte,
                    elementEncoding);
        }
        if (type instanceof MapType) {
            TextColumnEncoding keyEncoding = getEncoding(type.getTypeParameters().get(0), depth + 2);
            TextColumnEncoding valueEncoding = getEncoding(type.getTypeParameters().get(1), depth + 2);
            return new MapEncoding(
                    type,
                    nullSequence,
                    getSeparator(depth + 1),
                    getSeparator(depth + 2),
                    escapeByte,
                    keyEncoding,
                    valueEncoding);
        }
        if (type instanceof RowType) {
            List<TextColumnEncoding> fieldEncodings = type.getTypeParameters().stream()
                    .map(fieldType -> getEncoding(fieldType, depth + 1))
                    .collect(toImmutableList());
            return new StructEncoding(
                    type,
                    nullSequence,
                    getSeparator(depth + 1),
                    escapeByte,
                    lastColumnTakesRest,
                    fieldEncodings);
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private byte getSeparator(int depth)
    {
        if (depth >= separators.length) {
            throw new NotEnoughSeparatorsException(depth);
        }
        return separators[depth];
    }

    private static class NotEnoughSeparatorsException
            extends RuntimeException
    {
        private final int depth;

        public NotEnoughSeparatorsException(int depth)
        {
            this.depth = depth;
        }

        public int getDepth()
        {
            return depth;
        }
    }
}
