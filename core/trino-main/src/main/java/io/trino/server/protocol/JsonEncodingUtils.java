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
package io.trino.server.protocol;

import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.VariantType;
import io.trino.spi.variant.Variant;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.JsonType;
import io.trino.type.SqlIntervalDayTime;
import io.trino.type.SqlIntervalYearMonth;
import io.trino.util.variant.VariantUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class JsonEncodingUtils
{
    private JsonEncodingUtils() {}

    private static final BigintEncoder BIGINT_ENCODER = new BigintEncoder();
    private static final BooleanEncoder BOOLEAN_ENCODER = new BooleanEncoder();
    private static final IntegerEncoder INTEGER_ENCODER = new IntegerEncoder();
    private static final SmallintEncoder SMALLINT_ENCODER = new SmallintEncoder();
    private static final DoubleEncoder DOUBLE_ENCODER = new DoubleEncoder();
    private static final RealEncoder REAL_ENCODER = new RealEncoder();
    private static final TinyintEncoder TINYINT_ENCODER = new TinyintEncoder();
    private static final VarcharEncoder VARCHAR_ENCODER = new VarcharEncoder();
    private static final VarbinaryEncoder VARBINARY_ENCODER = new VarbinaryEncoder();
    private static final JsonEncoder JSON_ENCODER = new JsonEncoder();

    public static TypeEncoder[] createTypeEncoders(Session session, List<Type> types)
    {
        verify(!types.isEmpty(), "Columns must not be empty");

        Set<String> clientCapabilities = session.getClientCapabilities();
        boolean supportsParametricDateTime = clientCapabilities.contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());
        boolean supportsParametricInterval = clientCapabilities.contains(ClientCapabilities.PARAMETRIC_INTERVAL.toString());
        boolean supportsVariant = clientCapabilities.contains(ClientCapabilities.VARIANT.toString());
        boolean supportsVariantBinary = clientCapabilities.contains(ClientCapabilities.VARIANT_BINARY.toString());

        return types.stream()
                .map(type -> createTypeEncoder(type, supportsParametricDateTime, supportsParametricInterval, supportsVariant, supportsVariantBinary))
                .toArray(TypeEncoder[]::new);
    }

    public static TypeEncoder createTypeEncoder(Type type, boolean supportsParametricDateTime, boolean supportsParametricInterval, boolean supportsVariant, boolean supportsVariantBinary)
    {
        return switch (type) {
            case BigintType _ -> BIGINT_ENCODER;
            case BooleanType _ -> BOOLEAN_ENCODER;
            case IntegerType _ -> INTEGER_ENCODER;
            case SmallintType _ -> SMALLINT_ENCODER;
            case DoubleType _ -> DOUBLE_ENCODER;
            case RealType _ -> REAL_ENCODER;
            case TinyintType _ -> TINYINT_ENCODER;
            case VarcharType _ -> VARCHAR_ENCODER;
            case VarbinaryType _ -> VARBINARY_ENCODER;
            case JsonType _ -> JSON_ENCODER;
            case CharType charType -> new CharEncoder(charType.getLength());
            case VariantType _ -> new VariantEncoder(supportsVariant, supportsVariantBinary);
            case DecimalType decimalType -> decimalType.isShort() ? new ShortDecimalEncoder(decimalType) : new LongDecimalEncoder(decimalType);
            case DateType _ -> new DateEncoder();
            case TimeType timeType -> new TimeEncoder(timeType, supportsParametricDateTime);
            case TimeWithTimeZoneType timeType -> new TimeWithTimeZoneEncoder(timeType, supportsParametricDateTime);
            case TimestampType timestampType -> new TimestampEncoder(timestampType, supportsParametricDateTime);
            case TimestampWithTimeZoneType timestampType -> new TimestampWithTimeZoneEncoder(timestampType, supportsParametricDateTime);
            case ArrayType arrayType -> new ArrayEncoder(arrayType, createTypeEncoder(arrayType.getElementType(), supportsParametricDateTime, supportsParametricInterval, supportsVariant, supportsVariantBinary));
            case MapType mapType -> new MapEncoder(mapType, supportsParametricInterval, createTypeEncoder(mapType.getValueType(), supportsParametricDateTime, supportsParametricInterval, supportsVariant, supportsVariantBinary));
            case RowType rowType -> new RowEncoder(rowType, rowType.getFieldTypes()
                    .stream()
                    .map(elementType -> createTypeEncoder(elementType, supportsParametricDateTime, supportsParametricInterval, supportsVariant, supportsVariantBinary))
                    .toArray(TypeEncoder[]::new));
            case Type _ -> new TypeObjectValueEncoder(type, supportsParametricDateTime, supportsParametricInterval);
        };
    }

    public static void writePagesToJsonGenerator(Consumer<TrinoException> throwableConsumer, JsonGenerator generator, TypeEncoder[] typeEncoders, int[] sourcePageChannels, List<Page> pages)
    {
        verify(typeEncoders.length == sourcePageChannels.length, "Source page channels and type encoders must have the same length");
        try {
            generator.writeStartArray();

            for (Page page : pages) {
                Block[] blocks = new Block[sourcePageChannels.length];
                for (int i = 0; i < sourcePageChannels.length; i++) {
                    blocks[i] = page.getBlock(sourcePageChannels[i]);
                }
                for (int position = 0; position < page.getPositionCount(); position++) {
                    generator.writeStartArray();
                    for (int column = 0; column < typeEncoders.length; column++) {
                        typeEncoders[column].encode(generator, blocks[column], position);
                    }
                    generator.writeEndArray();
                }
            }
            generator.writeEndArray();
            generator.flush(); // final flush to have the data written to the output stream
        }
        catch (TrinoException e) {
            throwableConsumer.accept(e);
        }
        catch (Exception e) {
            throwableConsumer.accept(new TrinoException(SERIALIZATION_ERROR, "Could not serialize data to JSON", e));
        }
    }

    public sealed interface TypeEncoder
    {
        void encode(JsonGenerator generator, Block block, int position)
                throws IOException;
    }

    private static final class BigintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(BIGINT.getLong(block, position));
        }
    }

    private static final class IntegerEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(INTEGER.getInt(block, position));
        }
    }

    private static final class BooleanEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeBoolean(BOOLEAN.getBoolean(block, position));
        }
    }

    private static final class SmallintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(SMALLINT.getShort(block, position));
        }
    }

    private static final class TinyintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(TINYINT.getByte(block, position));
        }
    }

    private static final class DoubleEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(DOUBLE.getDouble(block, position));
        }
    }

    private static final class RealEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(REAL.getFloat(block, position));
        }
    }

    private static final class VarcharEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            Slice slice = VARCHAR.getSlice(block, position);
            generator.writeUTF8String(slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
    }

    private static final class CharEncoder
            implements TypeEncoder
    {
        private final int length;

        private CharEncoder(int length)
        {
            this.length = length;
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            Slice slice = padSpaces(VARCHAR.getSlice(block, position), length);
            generator.writeString(slice.toStringUtf8());
        }
    }

    private static final class VarbinaryEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            // Optimization: avoid copying Slice to byte array
            Slice slice = VARBINARY.getSlice(block, position);
            generator.writeBinary(slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
    }

    private static final class VariantEncoder
            implements TypeEncoder
    {
        private final boolean supportsVariant;
        private final boolean supportsVariantBinary;

        public VariantEncoder(boolean supportsVariant, boolean supportsVariantBinary)
        {
            this.supportsVariant = supportsVariant;
            this.supportsVariantBinary = supportsVariantBinary;
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            Variant variant = VARIANT.getObject(block, position);
            if (supportsVariantBinary) {
                generator.writeStartObject();
                generator.writeStringField("metadata", Base64.getEncoder().encodeToString(variant.metadata().toSlice().getBytes()));
                generator.writeStringField("value", Base64.getEncoder().encodeToString(variant.data().getBytes()));
                generator.writeEndObject();
            }
            else {
                String json = VariantUtil.asJson(variant).toStringUtf8();
                if (supportsVariant) {
                    generator.writeRawValue(json);
                }
                else {
                    generator.writeString(json);
                }
            }
        }
    }

    private static final class JsonEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            // Legacy text-only wire form. The typed-envelope variant (and the
            // JSON_PARSED_ITEM_ENCODING capability that gates it) lives in the
            // follow-on client-protocol commit.
            Json json = (Json) JsonType.JSON.getObject(block, position);
            generator.writeString(JsonItems.toText(json).toStringUtf8());
        }
    }

    private static final class ArrayEncoder
            implements TypeEncoder
    {
        private final ArrayType arrayType;
        private final TypeEncoder typeEncoder;

        public ArrayEncoder(ArrayType arrayType, TypeEncoder typeEncoder)
        {
            this.arrayType = requireNonNull(arrayType, "arrayType is null");
            this.typeEncoder = requireNonNull(typeEncoder, "typeEncoder is null");
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            Block arrayBlock = arrayType.getObject(block, position);
            generator.writeStartArray();
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                typeEncoder.encode(generator, arrayBlock, i);
            }
            generator.writeEndArray();
        }
    }

    private static final class MapEncoder
            implements TypeEncoder
    {
        private final MapType mapType;
        private final boolean supportsParametricInterval;
        private final TypeEncoder valueEncoder;

        public MapEncoder(MapType mapType, boolean supportsParametricInterval, TypeEncoder valueEncoder)
        {
            this.mapType = requireNonNull(mapType, "mapType is null");
            this.supportsParametricInterval = supportsParametricInterval;
            this.valueEncoder = requireNonNull(valueEncoder, "valueEncoder is null");
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            SqlMap map = mapType.getObject(block, position);
            int offset = map.getRawOffset();
            Block keyBlock = map.getRawKeyBlock();
            Block valueBlock = map.getRawValueBlock();

            verify(keyBlock.getPositionCount() == valueBlock.getPositionCount(), "Key and value blocks have different number of positions");
            boolean roundIntervalKeys = !supportsParametricInterval && containsDayTimeInterval(mapType.getKeyType());
            Set<String> legacyIntervalKeys = roundIntervalKeys ? new HashSet<>() : Set.of();
            generator.writeStartObject();
            for (int i = 0; i < map.getSize(); i++) {
                // Map keys are always serialized as strings for backward compatibility with existing clients.
                // Map values are always properly encoded using their types.
                // TODO: improve in v2 JSON format
                Object key = mapType.getKeyType().getObjectValue(keyBlock, offset + i);
                String encodedKey = roundIntervalKeys ? formatLegacyMapKey(key) : key.toString();
                if (roundIntervalKeys && !legacyIntervalKeys.add(encodedKey)) {
                    throw new TrinoException(NOT_SUPPORTED, "Interval map keys collide at millisecond precision; use a client supporting PARAMETRIC_INTERVAL");
                }
                generator.writeFieldName(encodedKey);
                valueEncoder.encode(generator, valueBlock, offset + i);
            }
            generator.writeEndObject();
        }
    }

    private static boolean containsDayTimeInterval(Type type)
    {
        return type instanceof IntervalDayTimeType || type.getTypeParameters().stream().anyMatch(JsonEncodingUtils::containsDayTimeInterval);
    }

    private static String formatLegacyMapKey(Object key)
    {
        return switch (key) {
            case null -> "null";
            case SqlIntervalDayTime interval -> interval.formatLegacy();
            case List<?> values -> values.stream().map(JsonEncodingUtils::formatLegacyMapKey).collect(joining(", ", "[", "]"));
            case Map<?, ?> values -> values.entrySet().stream()
                    .map(entry -> formatLegacyMapKey(entry.getKey()) + "=" + formatLegacyMapKey(entry.getValue()))
                    .collect(joining(", ", "{", "}"));
            default -> key.toString();
        };
    }

    private static final class RowEncoder
            implements TypeEncoder
    {
        private final RowType rowType;
        private final TypeEncoder[] fieldEncoders;

        public RowEncoder(RowType rowType, TypeEncoder[] fieldEncoders)
        {
            this.rowType = requireNonNull(rowType, "rowType is null");
            this.fieldEncoders = requireNonNull(fieldEncoders, "fieldEncoders is null");
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            SqlRow row = rowType.getObject(block, position);
            generator.writeStartArray();
            for (int i = 0; i < row.getFieldCount(); i++) {
                fieldEncoders[i].encode(generator, row.getRawFieldBlock(i), row.getRawIndex());
            }
            generator.writeEndArray();
        }
    }

    private static void writeDecimal(JsonGenerator generator, char[] buffer, int start, int scale, boolean negative)
            throws IOException
    {
        while (buffer.length - start <= scale) {
            buffer[--start] = '0';
        }
        if (scale > 0) {
            int integralDigits = buffer.length - start - scale;
            System.arraycopy(buffer, start, buffer, start - 1, integralDigits);
            start--;
            buffer[buffer.length - scale - 1] = '.';
        }
        if (negative) {
            buffer[--start] = '-';
        }
        generator.writeString(buffer, start, buffer.length - start);
    }

    private static int writeDigits(char[] buffer, int offset, long value, int digits)
    {
        int end = offset + digits;
        for (int index = end - 1; index >= offset; index--) {
            buffer[index] = (char) ('0' + value % 10);
            value /= 10;
        }
        return end;
    }

    private static int writeDate(char[] buffer, LocalDate date)
    {
        int year = date.getYear();
        int offset = 0;
        if (year < 0) {
            buffer[offset++] = '-';
        }
        else if (year > 9999) {
            buffer[offset++] = '+';
        }
        int yearDigits = 4;
        for (int remaining = Math.abs(year) / 10_000; remaining != 0; remaining /= 10) {
            yearDigits++;
        }
        offset = writeDigits(buffer, offset, Math.abs(year), yearDigits);
        buffer[offset++] = '-';
        offset = writeDigits(buffer, offset, date.getMonthValue(), 2);
        buffer[offset++] = '-';
        return writeDigits(buffer, offset, date.getDayOfMonth(), 2);
    }

    private static int writeTime(char[] buffer, int offset, long picos, int precision)
    {
        offset = writeDigits(buffer, offset, picos / 3_600_000_000_000_000L, 2);
        buffer[offset++] = ':';
        offset = writeDigits(buffer, offset, (picos / 60_000_000_000_000L) % 60, 2);
        buffer[offset++] = ':';
        offset = writeDigits(buffer, offset, (picos / 1_000_000_000_000L) % 60, 2);
        if (precision > 0) {
            buffer[offset++] = '.';
            // Write all twelve fractional digits, then expose only the declared precision.
            writeDigits(buffer, offset, picos % 1_000_000_000_000L, 12);
            offset += precision;
        }
        return offset;
    }

    private static void writeTimestamp(JsonGenerator generator, LocalDateTime dateTime, long fraction, int precision, String zone)
            throws IOException
    {
        char[] buffer = new char[40 + (zone == null ? 0 : zone.length() + 1)];
        int length = writeDate(buffer, dateTime.toLocalDate());
        buffer[length++] = ' ';
        long picos = dateTime.toLocalTime().toSecondOfDay() * 1_000_000_000_000L + fraction;
        length = writeTime(buffer, length, picos, precision);
        if (zone != null) {
            buffer[length++] = ' ';
            zone.getChars(0, zone.length(), buffer, length);
            length += zone.length();
        }
        generator.writeString(buffer, 0, length);
    }

    private record ShortDecimalEncoder(DecimalType type)
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            long value = type.getLong(block, position);
            char[] buffer = new char[22];
            boolean negative = value < 0;
            value = Math.abs(value);
            int start = buffer.length;
            do {
                buffer[--start] = (char) ('0' + value % 10);
                value /= 10;
            }
            while (value != 0);
            writeDecimal(generator, buffer, start, type.getScale(), negative);
        }
    }

    private record LongDecimalEncoder(DecimalType type)
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            Int128ArrayBlock valueBlock = (Int128ArrayBlock) block.getUnderlyingValueBlock();
            int valuePosition = block.getUnderlyingValuePosition(position);
            long high = valueBlock.getInt128High(valuePosition);
            long low = valueBlock.getInt128Low(valuePosition);
            boolean negative = high < 0;
            if (negative) {
                high = ~high + (low == 0 ? 1 : 0);
                low = -low;
            }
            char[] buffer = new char[42];
            int start = buffer.length;
            do {
                // Divide the unsigned 128-bit magnitude by 10^9, one 32-bit limb at a time.
                long quotientHigh = high / 1_000_000_000;
                long remainder = high % 1_000_000_000;
                long dividend = (remainder << 32) | (low >>> 32);
                long quotientUpper = dividend / 1_000_000_000;
                remainder = dividend % 1_000_000_000;
                dividend = (remainder << 32) | (low & 0xFFFF_FFFFL);
                low = (quotientUpper << 32) | (dividend / 1_000_000_000);
                high = quotientHigh;
                int digits = (int) (dividend % 1_000_000_000);
                if (high != 0 || low != 0) {
                    start -= 9;
                    writeDigits(buffer, start, digits, 9);
                }
                else {
                    do {
                        buffer[--start] = (char) ('0' + digits % 10);
                        digits /= 10;
                    }
                    while (digits != 0);
                }
            }
            while (high != 0 || low != 0);
            writeDecimal(generator, buffer, start, type.getScale(), negative);
        }
    }

    private static final class DateEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            char[] buffer = new char[16];
            int length = writeDate(buffer, LocalDate.ofEpochDay(DATE.getInt(block, position)));
            generator.writeString(buffer, 0, length);
        }
    }

    private record TimeEncoder(TimeType type, boolean supportsParametricDateTime)
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            long picos = type.getLong(block, position);
            int precision = type.getPrecision();
            if (!supportsParametricDateTime) {
                picos = round(picos, 9) % PICOSECONDS_PER_DAY;
                precision = 3;
            }
            char[] buffer = new char[21];
            int length = writeTime(buffer, 0, picos, precision);
            generator.writeString(buffer, 0, length);
        }
    }

    private record TimeWithTimeZoneEncoder(TimeWithTimeZoneType type, boolean supportsParametricDateTime)
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            long picos;
            int offsetMinutes;
            if (type.isShort()) {
                long packed = type.getLong(block, position);
                picos = unpackTimeNanos(packed) * PICOSECONDS_PER_NANOSECOND;
                offsetMinutes = unpackOffsetMinutes(packed);
            }
            else {
                Fixed12Block valueBlock = (Fixed12Block) block.getUnderlyingValueBlock();
                int valuePosition = block.getUnderlyingValuePosition(position);
                picos = valueBlock.getFixed12First(valuePosition);
                offsetMinutes = valueBlock.getFixed12Second(valuePosition);
            }
            int precision = type.getPrecision();
            if (!supportsParametricDateTime) {
                picos = round(picos, 9) % PICOSECONDS_PER_DAY;
                precision = 3;
            }
            char[] buffer = new char[27];
            int length = writeTime(buffer, 0, picos, precision);
            buffer[length++] = offsetMinutes >= 0 ? '+' : '-';
            length = writeDigits(buffer, length, Math.abs(offsetMinutes / 60), 2);
            buffer[length++] = ':';
            length = writeDigits(buffer, length, Math.abs(offsetMinutes % 60), 2);
            generator.writeString(buffer, 0, length);
        }
    }

    private record TimestampEncoder(TimestampType type, boolean supportsParametricDateTime)
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            long epochMicros;
            int picosOfMicro;
            if (type.isShort()) {
                epochMicros = type.getLong(block, position);
                picosOfMicro = 0;
            }
            else {
                Fixed12Block valueBlock = (Fixed12Block) block.getUnderlyingValueBlock();
                int valuePosition = block.getUnderlyingValuePosition(position);
                epochMicros = valueBlock.getFixed12First(valuePosition);
                picosOfMicro = valueBlock.getFixed12Second(valuePosition);
            }
            int precision = type.getPrecision();
            if (!supportsParametricDateTime) {
                epochMicros = round(epochMicros, 3);
                picosOfMicro = 0;
                precision = 3;
            }
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(floorDiv(epochMicros, 1_000_000), 0, UTC);
            long fraction = floorMod(epochMicros, 1_000_000) * 1_000_000L + picosOfMicro;
            writeTimestamp(generator, dateTime, fraction, precision, null);
        }
    }

    private record TimestampWithTimeZoneEncoder(TimestampWithTimeZoneType type, boolean supportsParametricDateTime)
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            long packedEpochMillis;
            int picosOfMilli;
            if (type.isShort()) {
                packedEpochMillis = type.getLong(block, position);
                picosOfMilli = 0;
            }
            else {
                Fixed12Block valueBlock = (Fixed12Block) block.getUnderlyingValueBlock();
                int valuePosition = block.getUnderlyingValuePosition(position);
                packedEpochMillis = valueBlock.getFixed12First(valuePosition);
                picosOfMilli = valueBlock.getFixed12Second(valuePosition);
            }
            long epochMillis = unpackMillisUtc(packedEpochMillis);
            int precision = type.getPrecision();
            if (!supportsParametricDateTime) {
                if (round(picosOfMilli, 9) == PICOSECONDS_PER_MILLISECOND) {
                    epochMillis++;
                }
                picosOfMilli = 0;
                precision = 3;
            }
            ZoneId zone = unpackZoneKey(packedEpochMillis).getZoneId();
            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zone);
            long fraction = floorMod(epochMillis, 1000) * 1_000_000_000L + picosOfMilli;
            writeTimestamp(generator, dateTime, fraction, precision, zone.getId());
        }
    }

    private static final class TypeObjectValueEncoder
            implements TypeEncoder
    {
        private final Type type;
        private final boolean supportsParametricDateTime;
        private final boolean supportsParametricInterval;

        public TypeObjectValueEncoder(Type type, boolean supportsParametricDateTime, boolean supportsParametricInterval)
        {
            this.type = requireNonNull(type, "type is null");
            this.supportsParametricDateTime = supportsParametricDateTime;
            this.supportsParametricInterval = supportsParametricInterval;
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            Object value = roundParametricTypes(type.getObjectValue(block, position));

            switch (value) {
                case BigDecimal bigDecimalValue -> generator.writeNumber(bigDecimalValue);
                case SqlDate dateValue -> generator.writeString(dateValue.toString());
                case SqlDecimal decimalValue -> generator.writeString(decimalValue.toString());
                // When client does not have NUMBER capability, NUMBER values are sent as varchar (strings).
                // When it has the capability, they are also sent as strings.
                case SqlNumber number -> generator.writeString(number.toString());
                case SqlIntervalDayTime intervalValue -> generator.writeString(supportsParametricInterval ? intervalValue.toString() : intervalValue.formatLegacy());
                case SqlIntervalYearMonth intervalValue -> generator.writeString(intervalValue.toString());
                case SqlTime timeValue -> generator.writeString(timeValue.toString());
                case SqlTimeWithTimeZone timeWithTimeZone -> generator.writeString(timeWithTimeZone.toString());
                case SqlTimestamp timestamp -> generator.writeString(timestamp.toString());
                case SqlTimestampWithTimeZone timestampWithTimeZone -> generator.writeString(timestampWithTimeZone.toString());
                case SqlVarbinary sqlVarbinary -> generator.writeBinary(sqlVarbinary.getBytes());
                default -> generator.writePOJO(value);
            }
        }

        private Object roundParametricTypes(Object value)
        {
            return switch (value) {
                case SqlTimestamp sqlTimestamp when !supportsParametricDateTime -> sqlTimestamp.roundTo(3);
                case SqlTimestampWithTimeZone sqlTimestampWithTimeZone when !supportsParametricDateTime -> sqlTimestampWithTimeZone.roundTo(3);
                case SqlTime sqlTime when !supportsParametricDateTime -> sqlTime.roundTo(3);
                case SqlTimeWithTimeZone sqlTimeWithTimeZone when !supportsParametricDateTime -> sqlTimeWithTimeZone.roundTo(3);
                default -> value;
            };
        }
    }
}
