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
package io.trino.plugin.hudi.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Integer.parseInt;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class HudiAvroSerializer
{
    private static final int[] NANO_FACTOR = {
            -1, // 0, no need to multiply
            100_000_000, // 1 digit after the dot
            10_000_000, // 2 digits after the dot
            1_000_000, // 3 digits after the dot
            100_000, // 4 digits after the dot
            10_000, // 5 digits after the dot
            1000, // 6 digits after the dot
            100, // 7 digits after the dot
            10, // 8 digits after the dot
            1, // 9 digits after the dot
    };

    private static final AvroDecimalConverter DECIMAL_CONVERTER = new AvroDecimalConverter();
    private final SynthesizedColumnHandler synthesizedColumnHandler;

    private final List<HiveColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Schema schema;

    public HudiAvroSerializer(List<HiveColumnHandle> columnHandles, SynthesizedColumnHandler synthesizedColumnHandler)
    {
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(HiveColumnHandle::getType).toList();
        // Fetches projected schema
        this.schema = constructSchema(columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getName).toList(),
                columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getHiveType).toList());
        this.synthesizedColumnHandler = synthesizedColumnHandler;
    }

    public IndexedRecord serialize(Page sourcePage, int position)
    {
        IndexedRecord record = new GenericData.Record(schema);
        for (int i = 0; i < columnTypes.size(); i++) {
            Object value = getValue(sourcePage, i, position);
            record.put(i, value);
        }
        return record;
    }

    public Object getValue(Page sourcePage, int channel, int position)
    {
        return columnTypes.get(channel).getObjectValue(null, sourcePage.getBlock(channel), position);
    }

    public void buildRecordInPage(PageBuilder pageBuilder, IndexedRecord record)
    {
        pageBuilder.declarePosition();
        int blockSeq = 0;
        for (int channel = 0; channel < columnTypes.size(); channel++, blockSeq++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(blockSeq);
            HiveColumnHandle columnHandle = columnHandles.get(channel);
            if (synthesizedColumnHandler.isSynthesizedColumn(columnHandle)) {
                synthesizedColumnHandler.getColumnStrategy(columnHandle).appendToBlock(output, columnTypes.get(channel));
            }
            else {
                // Record may not be projected, get index from it
                int fieldPosInSchema = record.getSchema().getField(columnHandle.getName()).pos();
                appendTo(columnTypes.get(channel), record.get(fieldPosInSchema), output);
            }
        }
    }

    public static void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(SMALLINT)) {
                    type.writeLong(output, ((Number) value).shortValue());
                }
                else if (type.equals(TINYINT)) {
                    type.writeLong(output, ((Number) value).byteValue());
                }
                else if (type.equals(REAL)) {
                    if (value instanceof Number) {
                        // Directly get the float value from the Number
                        // This preserves the fractional part
                        float floatValue = ((Number) value).floatValue();

                        // Get the IEEE 754 single-precision 32-bit representation of this float
                        int intBits = Float.floatToRawIntBits(floatValue);

                        // The writeLong method expects these int bits, passed as a long
                        // NOTE: Java handles the widening conversion from int to long
                        type.writeLong(output, intBits);
                    }
                    else {
                        // Handle cases where 'value' is not a Number
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Unhandled type for %s: %s | value type: %s", javaType.getSimpleName(), type, value.getClass().getName()));
                    }
                }
                else if (type instanceof DecimalType decimalType) {
                    if (value instanceof SqlDecimal sqlDecimal) {
                        if (decimalType.isShort()) {
                            writeShortDecimal(output, sqlDecimal.toBigDecimal().unscaledValue().longValue());
                        }
                        else {
                            writeBigDecimal(decimalType, output, sqlDecimal.toBigDecimal());
                        }
                    }
                    else if (value instanceof GenericData.Fixed fixed) {
                        verify(decimalType.isShort(), "The type should be short decimal");
                        BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), fixed.bytes());
                        type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Unhandled type for %s: %s | value type: %s", javaType.getSimpleName(), type, value.getClass().getName()));
                    }
                }
                else if (type.equals(DATE)) {
                    if (value instanceof SqlDate sqlDate) {
                        type.writeLong(output, sqlDate.getDays());
                    }
                    else if (value instanceof Integer days) {
                        ((DateType) type).writeInt(output, days);
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Unhandled type for %s: %s | value type: %s", javaType.getSimpleName(), type, value.getClass().getName()));
                    }
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    type.writeLong(output, toTrinoTimestamp(((Utf8) value).toString()));
                }
                else if (type.equals(TIME_MICROS)) {
                    type.writeLong(output, (long) value * PICOSECONDS_PER_MICROSECOND);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (type.getJavaType() == Int128.class) {
                writeObject(output, type, value);
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == LongTimestamp.class) {
                if (value instanceof SqlTimestamp sqlTimestamp) {
                    // value is read from parquet
                    // From tests, sqlTimestamp is a UTC epoch that is converted from ZoneId#systemDefault()
                    // IMPORTANT: Even when session's zoneId != ZoneId#systemDefault(), ZoneId#systemDefault() is used calculate/produce the false UTC.
                    // The current sqlTimestamp is calculated as such:
                    // 1. The true UTC timestamp that is stored in file is assumed to be in the local timezone
                    // 2. Trino will them attempt to convert this to a false UTC by subtracting the timezone's offset (factoring offset rules like DST)
                    // Hence, to calculate the true UTC, we will just have to reverse the steps

                    // Reconstruct the original local wall time from sqlTimestamp's fields
                    long microsFromSqlTs = sqlTimestamp.getEpochMicros();
                    // picosFromSqlTs is defined as "picoseconds within the microsecond" (0 to 999,999)
                    int picosFromSqlTs = sqlTimestamp.getPicosOfMicros();
                    long secondsComponent = microsFromSqlTs / 1_000_000L;
                    // Storing nanos component separately from seconds component, hence the modulo to remove secondsComponent
                    int nanosComponent = (int) ((microsFromSqlTs % 1_000_000L) * 1000L + picosFromSqlTs / 1000L);
                    LocalDateTime originalLocalWallTime = LocalDateTime.ofEpochSecond(secondsComponent, nanosComponent, ZoneOffset.UTC);

                    // Determine the ZoneId in which originalLocalWallTime was observed
                    ZoneId assumedOriginalZoneId = ZoneId.systemDefault();

                    // Convert to true UTC by interpreting the local wall time in its original zone
                    // This correctly handles DST for that zone at that specific historical date/time.
                    ZonedDateTime zdtInOriginalZone;
                    try {
                        zdtInOriginalZone = originalLocalWallTime.atZone(assumedOriginalZoneId);
                    }
                    catch (DateTimeException e) {
                        // Handle cases where the local time is invalid in the zone (e.g., during DST "spring forward" gap) or ambiguous (during DST "fall back" overlap).
                        // For now, rethrow or log, as robustly handling this requires a defined policy.
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Cannot uniquely or validly map local time %s to zone %s: %s",
                                        originalLocalWallTime, assumedOriginalZoneId, e.getMessage()), e);
                    }
                    Instant trueUtcInstant = zdtInOriginalZone.toInstant();

                    // Extract true UTC epoch micros and picos
                    long trueUtcEpochSeconds = trueUtcInstant.getEpochSecond();
                    long trueUtcEpochMicrosContributionFromSeconds;
                    try {
                        trueUtcEpochMicrosContributionFromSeconds = Math.multiplyExact(trueUtcEpochSeconds, 1_000_000L);
                    }
                    catch (ArithmeticException e) {
                        // Multiplication could overflow if epochSeconds is approximately more than 292,271 years from epoch
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                                "Timestamp " + trueUtcInstant + " is too far in the past or future to be represented as microseconds in a long.", e);
                    }

                    long trueUtcEpochMicrosContributionFromNanos = trueUtcInstant.getNano() / 1000L;
                    long trueUtcEpochMicros;

                    try {
                        trueUtcEpochMicros = Math.addExact(trueUtcEpochMicrosContributionFromSeconds, trueUtcEpochMicrosContributionFromNanos);
                    }
                    catch (ArithmeticException e) {
                        // Addition could also theoretically overflow if epochMicrosContributionFromSeconds is:
                        // 1. Very close to Long.MAX_VALUE and trueUtcEpochMicrosContributionFromNanos is positive
                        // 2. Very close to Long.MIN_VALUE and trueUtcEpochMicrosContributionFromNanos is negative
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                                "Timestamp " + trueUtcInstant + " results in microsecond representation overflow after adding nanosecond component.", e);
                    }

                    int truePicosOfMicros = (trueUtcInstant.getNano() % 1000) * 1000;
                    ((Fixed12BlockBuilder) output).writeFixed12(trueUtcEpochMicros, truePicosOfMicros);
                }
                else if (value instanceof Long epochMicros) {
                    // value is read from log
                    // epochMicros is in micros, no nanos or picos component
                    ((Fixed12BlockBuilder) output).writeFixed12(epochMicros, 0);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR,
                            format("Unhandled type for %s: %s | value type: %s", javaType.getSimpleName(), type, value.getClass().getName()));
                }
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                verify(type.equals(TIMESTAMP_TZ_MICROS));
                long epochMicros = (long) value;
                int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
                type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
            }
            else if (type instanceof ArrayType arrayType) {
                writeArray((ArrayBlockBuilder) output, (List<?>) value, arrayType);
            }
            else if (type instanceof RowType rowType) {
                if (value instanceof List<?> list) {
                    // value is read from parquet
                    writeRow((RowBlockBuilder) output, rowType, list);
                }
                else if (value instanceof GenericRecord record) {
                    // value is read from log
                    writeRow((RowBlockBuilder) output, rowType, record);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR,
                            format("Unhandled type for %s: %s | value type: %s", javaType.getSimpleName(), type, value.getClass().getName()));
                }
            }
            else if (type instanceof MapType mapType) {
                writeMap((MapBlockBuilder) output, mapType, (Map<?, ?>) value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException cce) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("ClassCastException for type %s: %s with error %s", javaType.getSimpleName(), type, cce));
        }
    }

    public static LocalDateTime toLocalDateTime(String datetime)
    {
        int dotPosition = datetime.indexOf('.');
        if (dotPosition == -1) {
            // no sub-second element
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime));
        }
        LocalDateTime result = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime.substring(0, dotPosition)));
        // has sub-second element, so convert to nanosecond
        String nanosStr = datetime.substring(dotPosition + 1);
        int nanoOfSecond = parseInt(nanosStr) * NANO_FACTOR[nanosStr.length()];
        return result.withNano(nanoOfSecond);
    }

    public static long toTrinoTimestamp(String datetime)
    {
        Instant instant = toLocalDateTime(datetime).toInstant(UTC);
        return (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType) {
            if (value instanceof Utf8) {
                type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
            }
            else if (value instanceof String) {
                type.writeSlice(output, utf8Slice((String) value));
            }
            else {
                type.writeSlice(output, utf8Slice(value.toString()));
            }
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                type.writeSlice(output, Slices.wrappedHeapBuffer((ByteBuffer) value));
            }
            else if (value instanceof SqlVarbinary sqlVarbinary) {
                type.writeSlice(output, Slices.wrappedBuffer(sqlVarbinary.getBytes()));
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s | value type: %s", type.getJavaType().getSimpleName(), type, value.getClass().getName()));
            }
        }
        else if (type instanceof CharType) {
            String stringValue;
            if (value instanceof Utf8) {
                stringValue = ((Utf8) value).toString();
            }
            else if (value instanceof String) {
                stringValue = (String) value;
            }
            else {
                // Fallback: convert any other object to its string representation
                stringValue = value.toString();
            }
            // IMPORTANT: Char types may be padded with trailing "space" characters to make up for length if the contents are lesser than defined length.
            // Need to trim out trailing spaces as Slice representing Char should not have trailing spaces
            type.writeSlice(output, utf8Slice(stringValue.trim()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s | value type: %s", type.getJavaType().getSimpleName(), type, value.getClass().getName()));
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            BigDecimal valueAsBigDecimal;
            if (value instanceof SqlDecimal sqlDecimal) {
                valueAsBigDecimal = sqlDecimal.toBigDecimal();
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s | value type: %s", type.getJavaType().getSimpleName(), type, value.getClass().getName()));
            }

            Object trinoNativeDecimalValue = Decimals.encodeScaledValue(valueAsBigDecimal, decimalType.getScale());
            type.writeObject(output, trinoNativeDecimalValue);
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private static void writeArray(ArrayBlockBuilder output, List<?> value, ArrayType arrayType)
    {
        Type elementType = arrayType.getElementType();
        output.buildEntry(elementBuilder -> {
            for (Object element : value) {
                appendTo(elementType, element, elementBuilder);
            }
        });
    }

    private static void writeRow(RowBlockBuilder output, RowType rowType, GenericRecord record)
    {
        List<RowType.Field> fields = rowType.getFields();
        output.buildEntry(fieldBuilders -> {
            for (int index = 0; index < fields.size(); index++) {
                RowType.Field field = fields.get(index);
                appendTo(field.getType(), record.get(field.getName().orElse("field" + index)), fieldBuilders.get(index));
            }
        });
    }

    private static void writeRow(RowBlockBuilder output, RowType rowType, List<?> list)
    {
        List<RowType.Field> fields = rowType.getFields();
        output.buildEntry(fieldBuilders -> {
            for (int index = 0; index < fields.size(); index++) {
                RowType.Field field = fields.get(index);
                appendTo(field.getType(), list.get(index), fieldBuilders.get(index));
            }
        });
    }

    private static void writeMap(MapBlockBuilder output, MapType mapType, Map<?, ?> value)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        output.buildEntry((keyBuilder, valueBuilder) -> {
            for (Map.Entry<?, ?> entry : value.entrySet()) {
                appendTo(keyType, entry.getKey(), keyBuilder);
                appendTo(valueType, entry.getValue(), valueBuilder);
            }
        });
    }

    static class AvroDecimalConverter
    {
        private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();

        BigDecimal convert(int precision, int scale, byte[] bytes)
        {
            Schema schema = new Schema.Parser().parse(format("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}", precision, scale));
            return AVRO_DECIMAL_CONVERSION.fromBytes(ByteBuffer.wrap(bytes), schema, schema.getLogicalType());
        }
    }
}
