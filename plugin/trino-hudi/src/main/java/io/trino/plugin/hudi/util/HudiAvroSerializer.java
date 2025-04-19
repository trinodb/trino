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
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
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

    private final List<HiveColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Schema schema;
    private final SynthesizedColumnHandler synthesizedColumnHandler;

    public HudiAvroSerializer(List<HiveColumnHandle> columnHandles, SynthesizedColumnHandler synthesizedColumnHandler)
    {
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(HiveColumnHandle::getType).toList();
        // Fetches projected schema
        this.schema = constructSchema(
                columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getName).toList(),
                columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getHiveType).toList(),
                false);
        this.synthesizedColumnHandler = synthesizedColumnHandler;
    }

    public IndexedRecord serialize(SourcePage sourcePage, int position)
    {
        IndexedRecord record = new GenericData.Record(schema);
        for (int i = 0; i < columnTypes.size(); i++) {
            Object value = getValue(sourcePage, i, position);
            record.put(i, value);
        }
        return record;
    }

    public Object getValue(SourcePage sourcePage, int channel, int position)
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
                synthesizedColumnHandler.getColumnStrategy(columnHandle).appendToBlock(output);
            }
            else {
                // Record may not be projected, get index from it
                int fieldPosInSchema = record.getSchema().getField(columnHandle.getName()).pos();
                appendTo(columnTypes.get(channel), record.get(fieldPosInSchema), output);
            }
        }
    }

    public void appendTo(Type type, Object value, BlockBuilder output)
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
                else if (type instanceof DecimalType decimalType) {
                    verify(decimalType.isShort(), "The type should be short decimal");
                    BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
                    type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, ((Number) value).intValue());
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
                type.writeObject(output, value);
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
                writeRow((RowBlockBuilder) output, rowType, (GenericRecord) value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
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

    private void writeSlice(BlockBuilder output, Type type, Object value)
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
            else {
                output.appendNull();
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            verify(!decimalType.isShort(), "The type should be long decimal");
            BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeArray(ArrayBlockBuilder output, List<?> value, ArrayType arrayType)
    {
        Type elementType = arrayType.getElementType();
        output.buildEntry(elementBuilder -> {
            for (Object element : value) {
                appendTo(elementType, element, elementBuilder);
            }
        });
    }

    private void writeRow(RowBlockBuilder output, RowType rowType, GenericRecord record)
    {
        List<RowType.Field> fields = rowType.getFields();
        output.buildEntry(fieldBuilders -> {
            for (int index = 0; index < fields.size(); index++) {
                RowType.Field field = fields.get(index);
                appendTo(field.getType(), record.get(field.getName().orElse("field" + index)), fieldBuilders.get(index));
            }
        });
    }

    static class AvroDecimalConverter
    {
        private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();

        BigDecimal convert(int precision, int scale, Object value)
        {
            Schema schema = new Schema.Parser().parse(format("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}", precision, scale));
            return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer) value, schema, schema.getLogicalType());
        }
    }
}
