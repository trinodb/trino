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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.encodings.text.TextEncodingOptions.NestingLevels.EXTENDED;
import static io.trino.hive.formats.encodings.text.TextEncodingOptions.NestingLevels.EXTENDED_ADDITIONAL;
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
import static java.util.Objects.requireNonNull;

public class TextColumnEncodingFactory
        implements ColumnEncodingFactory
{
    private final TextEncodingOptions textEncodingOptions;

    public TextColumnEncodingFactory()
    {
        this(TextEncodingOptions.builder().build());
    }

    public TextColumnEncodingFactory(TextEncodingOptions textEncodingOptions)
    {
        this.textEncodingOptions = requireNonNull(textEncodingOptions, "simpleOptions is null");
    }

    @Override
    public TextColumnEncoding getEncoding(Type type)
    {
        try {
            return getEncoding(type, 0);
        }
        catch (NotEnoughSeparatorsException e) {
            if (e.getDepth() > EXTENDED_ADDITIONAL.getLevels()) {
                throw new IllegalArgumentException(format(
                        "Type %s requires %s nesting levels, which is not possible",
                        type,
                        e.getDepth()));
            }
            if (e.getDepth() > EXTENDED.getLevels()) {
                throw new IllegalArgumentException(format(
                        "Type %s requires %s nesting levels, which can be enabled with the %s table property",
                        type,
                        e.getDepth(),
                        EXTENDED_ADDITIONAL.getTableProperty()));
            }
            throw new IllegalArgumentException(format(
                    "Type %s requires %s nesting levels, which can be enabled with the %s table property",
                    type,
                    e.getDepth(),
                    EXTENDED.getTableProperty()));
        }
    }

    private TextColumnEncoding getEncoding(Type type, int depth)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
            return new LongEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (type instanceof DecimalType) {
            return new DecimalEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (REAL.equals(type)) {
            return new FloatEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (DOUBLE.equals(type)) {
            return new DoubleEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return new StringEncoding(type, textEncodingOptions.getNullSequence(), textEncodingOptions.getEscapeByte(), textEncodingOptions.getSeparators());
        }
        if (VARBINARY.equals(type)) {
            // binary text encoding is not escaped
            return new BinaryEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (DATE.equals(type)) {
            return new DateEncoding(type, textEncodingOptions.getNullSequence());
        }
        if (type instanceof TimestampType) {
            return new TimestampEncoding((TimestampType) type, textEncodingOptions.getNullSequence(), textEncodingOptions.getTimestampFormats());
        }
        if (type instanceof ArrayType) {
            TextColumnEncoding elementEncoding = getEncoding(type.getTypeParameters().get(0), depth + 1);
            return new ListEncoding(
                    type,
                    textEncodingOptions.getNullSequence(),
                    getSeparator(depth + 1),
                    textEncodingOptions.getEscapeByte(),
                    elementEncoding);
        }
        if (type instanceof MapType) {
            TextColumnEncoding keyEncoding = getEncoding(type.getTypeParameters().get(0), depth + 2);
            TextColumnEncoding valueEncoding = getEncoding(type.getTypeParameters().get(1), depth + 2);
            return new MapEncoding(
                    type,
                    textEncodingOptions.getNullSequence(),
                    getSeparator(depth + 1),
                    getSeparator(depth + 2),
                    textEncodingOptions.getEscapeByte(),
                    keyEncoding,
                    valueEncoding);
        }
        if (type instanceof RowType) {
            List<TextColumnEncoding> fieldEncodings = type.getTypeParameters().stream()
                    .map(fieldType -> getEncoding(fieldType, depth + 1))
                    .collect(toImmutableList());
            return new StructEncoding(
                    type,
                    textEncodingOptions.getNullSequence(),
                    getSeparator(depth + 1),
                    textEncodingOptions.getEscapeByte(),
                    textEncodingOptions.isLastColumnTakesRest(),
                    fieldEncodings);
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private byte getSeparator(int depth)
    {
        if (depth >= textEncodingOptions.getSeparators().length()) {
            throw new NotEnoughSeparatorsException(depth);
        }
        return textEncodingOptions.getSeparators().getByte(depth);
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
