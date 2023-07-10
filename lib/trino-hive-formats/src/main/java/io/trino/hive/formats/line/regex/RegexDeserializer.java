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
package io.trino.hive.formats.line.regex;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveFormatUtils.parseHiveDate;
import static io.trino.hive.formats.HiveFormatUtils.parseHiveTimestamp;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Pattern.DOTALL;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Deserializer that is bug for bug compatible with Hive RegexSerde.
 */
public class RegexDeserializer
        implements LineDeserializer
{
    private final Pattern inputPattern;
    private final List<Column> columns;

    public RegexDeserializer(List<Column> columns, String regex, boolean caseSensitive)
    {
        this.inputPattern = Pattern.compile(regex, DOTALL + (caseSensitive ? Pattern.CASE_INSENSITIVE : 0));
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return columns.stream()
                .map(Column::type)
                .collect(toImmutableList());
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
            throws IOException
    {
        Matcher matcher = inputPattern.matcher(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8));
        builder.declarePosition();
        if (!matcher.matches()) {
            for (int i = 0; i < columns.size(); i++) {
                builder.getBlockBuilder(i).appendNull();
            }
            return;
        }

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            int ordinal = column.ordinal();
            BlockBuilder blockBuilder = builder.getBlockBuilder(i);
            String value = ordinal < matcher.groupCount() ? matcher.group(ordinal + 1) : null;
            if (value == null) {
                blockBuilder.appendNull();
                continue;
            }
            serializeValue(value, column, blockBuilder);
        }
    }

    private static void serializeValue(String value, Column column, BlockBuilder builder)
    {
        try {
            Type type = column.type();
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(builder, Boolean.parseBoolean(value));
            }
            else if (BIGINT.equals(type)) {
                type.writeLong(builder, Long.parseLong(value));
            }
            else if (INTEGER.equals(type)) {
                type.writeLong(builder, Integer.parseInt(value));
            }
            else if (SMALLINT.equals(type)) {
                type.writeLong(builder, Short.parseShort(value));
            }
            else if (TINYINT.equals(type)) {
                type.writeLong(builder, Byte.parseByte(value));
            }
            else if (type instanceof DecimalType decimalType) {
                serializeDecimal(value, decimalType, builder);
            }
            else if (REAL.equals(type)) {
                type.writeLong(builder, floatToRawIntBits(Float.parseFloat(value)));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(builder, Double.parseDouble(value));
            }
            else if (DATE.equals(type)) {
                type.writeLong(builder, toIntExact(parseHiveDate(value).toEpochDay()));
            }
            else if (type instanceof TimestampType timestampType) {
                DecodedTimestamp timestamp = parseHiveTimestamp(value);
                createTimestampEncoder(timestampType, UTC).write(timestamp, builder);
            }
            else if (type instanceof VarcharType varcharType) {
                type.writeSlice(builder, truncateToLength(Slices.utf8Slice(value), varcharType));
            }
            else if (type instanceof CharType charType) {
                type.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(value), charType));
            }
            else {
                throw new UnsupportedTypeException(column);
            }
        }
        catch (UnsupportedTypeException e) {
            throw e;
        }
        catch (RuntimeException e) {
            // invalid columns are ignored
            builder.appendNull();
        }
    }

    private static void serializeDecimal(String value, DecimalType decimalType, BlockBuilder builder)
    {
        BigDecimal bigDecimal;
        try {
            bigDecimal = new BigDecimal(value).setScale(DecimalConversions.intScale(decimalType.getScale()), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException(format("Cannot convert '%s' to %s. Value is not a number.", value, decimalType));
        }

        if (overflows(bigDecimal, decimalType.getPrecision())) {
            throw new IllegalArgumentException(format("Cannot convert '%s' to %s. Value too large.", value, decimalType));
        }

        if (decimalType.isShort()) {
            decimalType.writeLong(builder, bigDecimal.unscaledValue().longValueExact());
        }
        else {
            decimalType.writeObject(builder, Int128.valueOf(bigDecimal.unscaledValue()));
        }
    }

    public static class UnsupportedTypeException
            extends RuntimeException
    {
        private final Column column;

        public UnsupportedTypeException(Column column)
        {
            super("Unsupported column type: " + column);
            this.column = column;
        }

        public Column getColumn()
        {
            return column;
        }
    }
}
