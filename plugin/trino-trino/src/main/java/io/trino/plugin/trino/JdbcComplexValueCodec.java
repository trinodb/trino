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
package io.trino.plugin.trino;

import io.airlift.slice.Slices;
import io.trino.jdbc.Row;
import io.trino.jdbc.RowField;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapHashTables;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;

final class JdbcComplexValueCodec
{
    private JdbcComplexValueCodec() {}

    static Block readArray(ResultSet rs, int columnIndex, Type elementType)
            throws SQLException
    {
        Array jdbcArray = rs.getArray(columnIndex);
        if (jdbcArray == null) {
            return null;
        }
        Object[] elements = toObjectArray(jdbcArray);
        BlockBuilder builder = elementType.createBlockBuilder(null, elements.length);
        for (Object element : elements) {
            writeJdbcValueToBlock(element, elementType, builder);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    static SqlMap readMap(ResultSet rs, int columnIndex, MapType mapType)
            throws SQLException
    {
        Object value = rs.getObject(columnIndex);
        if (value == null) {
            return null;
        }
        Map<Object, Object> map = (Map<Object, Object>) value;
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        BlockBuilder keyBuilder = keyType.createBlockBuilder(null, map.size());
        BlockBuilder valueBuilder = valueType.createBlockBuilder(null, map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            writeJdbcValueToBlock(entry.getKey(), keyType, keyBuilder);
            writeJdbcValueToBlock(entry.getValue(), valueType, valueBuilder);
        }
        return new SqlMap(mapType, MapHashTables.HashBuildMode.DUPLICATE_NOT_CHECKED, keyBuilder.build(), valueBuilder.build());
    }

    static SqlRow readRow(ResultSet rs, int columnIndex, RowType rowType)
            throws SQLException
    {
        Object value = rs.getObject(columnIndex);
        if (value == null) {
            return null;
        }

        List<RowType.Field> fields = rowType.getFields();
        Block[] fieldBlocks = new Block[fields.size()];
        List<Object> fieldValues = toFieldValues(value);
        for (int index = 0; index < fields.size(); index++) {
            BlockBuilder builder = fields.get(index).getType().createBlockBuilder(null, 1);
            Object fieldValue = index < fieldValues.size() ? fieldValues.get(index) : null;
            writeJdbcValueToBlock(fieldValue, fields.get(index).getType(), builder);
            fieldBlocks[index] = builder.build();
        }
        return new SqlRow(0, fieldBlocks);
    }

    @SuppressWarnings("unchecked")
    private static void writeJdbcValueToBlock(Object value, Type type, BlockBuilder builder)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }

        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            Object[] elements = toObjectArray(value);
            BlockBuilder elementBuilder = elementType.createBlockBuilder(null, elements.length);
            for (Object element : elements) {
                writeJdbcValueToBlock(element, elementType, elementBuilder);
            }
            type.writeObject(builder, elementBuilder.build());
            return;
        }
        if (type instanceof MapType mapType) {
            Map<Object, Object> map = (Map<Object, Object>) value;
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            BlockBuilder keyBuilder = keyType.createBlockBuilder(null, map.size());
            BlockBuilder valueBuilder = valueType.createBlockBuilder(null, map.size());
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                writeJdbcValueToBlock(entry.getKey(), keyType, keyBuilder);
                writeJdbcValueToBlock(entry.getValue(), valueType, valueBuilder);
            }
            type.writeObject(builder, new SqlMap(mapType, MapHashTables.HashBuildMode.DUPLICATE_NOT_CHECKED, keyBuilder.build(), valueBuilder.build()));
            return;
        }
        if (type instanceof RowType rowType) {
            List<Object> fieldValues = toFieldValues(value);
            Block[] fieldBlocks = new Block[rowType.getFields().size()];
            for (int index = 0; index < rowType.getFields().size(); index++) {
                BlockBuilder fieldBuilder = rowType.getFields().get(index).getType().createBlockBuilder(null, 1);
                Object fieldValue = index < fieldValues.size() ? fieldValues.get(index) : null;
                writeJdbcValueToBlock(fieldValue, rowType.getFields().get(index).getType(), fieldBuilder);
                fieldBlocks[index] = fieldBuilder.build();
            }
            type.writeObject(builder, new SqlRow(0, fieldBlocks));
            return;
        }

        writeScalarJdbcValueToBlock(value, type, builder);
    }

    private static void writeScalarJdbcValueToBlock(Object value, Type type, BlockBuilder builder)
    {
        if (type instanceof CharType charType) {
            type.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(value.toString()), charType));
            return;
        }
        if (type instanceof VarcharType) {
            type.writeSlice(builder, Slices.utf8Slice(value.toString()));
            return;
        }
        if (type == BIGINT) {
            type.writeLong(builder, ((Number) value).longValue());
            return;
        }
        if (type == INTEGER) {
            type.writeLong(builder, ((Number) value).intValue());
            return;
        }
        if (type == SMALLINT) {
            type.writeLong(builder, ((Number) value).shortValue());
            return;
        }
        if (type == TINYINT) {
            type.writeLong(builder, ((Number) value).byteValue());
            return;
        }
        if (type == DOUBLE) {
            type.writeDouble(builder, ((Number) value).doubleValue());
            return;
        }
        if (type == REAL) {
            type.writeLong(builder, Float.floatToIntBits(((Number) value).floatValue()));
            return;
        }
        if (type == BOOLEAN) {
            type.writeBoolean(builder, (Boolean) value);
            return;
        }
        if (type instanceof VarbinaryType) {
            type.writeSlice(builder, Slices.wrappedBuffer((byte[]) value));
            return;
        }
        if (type instanceof DateType) {
            long epochDay;
            if (value instanceof Date sqlDate) {
                epochDay = sqlDate.toLocalDate().toEpochDay();
            }
            else if (value instanceof LocalDate localDate) {
                epochDay = localDate.toEpochDay();
            }
            else {
                epochDay = TemporalTransportCodec.parseDate(value.toString()).toEpochDay();
            }
            type.writeLong(builder, epochDay);
            return;
        }
        if (type instanceof TimeType) {
            long picos;
            if (value instanceof Time sqlTime) {
                picos = sqlTime.toLocalTime().toNanoOfDay() * 1_000L;
            }
            else if (value instanceof LocalTime localTime) {
                picos = localTime.toNanoOfDay() * 1_000L;
            }
            else {
                picos = LocalTime.parse(value.toString()).toNanoOfDay() * 1_000L;
            }
            type.writeLong(builder, picos);
            return;
        }
        if (type instanceof TimestampType timestampType) {
            long epochMicros;
            if (value instanceof Timestamp sqlTimestamp) {
                LocalDateTime localDateTime = sqlTimestamp.toLocalDateTime();
                long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
                epochMicros = epochSecond * 1_000_000L + localDateTime.getNano() / 1_000L;
            }
            else if (value instanceof LocalDateTime localDateTime) {
                long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
                epochMicros = epochSecond * 1_000_000L + localDateTime.getNano() / 1_000L;
            }
            else {
                throw new TrinoException(JDBC_ERROR, format("Unsupported timestamp value type: %s", value.getClass().getName()));
            }
            if (timestampType.isShort()) {
                type.writeLong(builder, epochMicros);
            }
            else {
                int picoFraction = value instanceof Timestamp sqlTimestamp ? (sqlTimestamp.toLocalDateTime().getNano() % 1_000) * 1_000 : 0;
                type.writeObject(builder, new LongTimestamp(epochMicros, picoFraction));
            }
            return;
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            OffsetDateTime offsetDateTime;
            if (value instanceof OffsetDateTime odt) {
                offsetDateTime = odt;
            }
            else if (value instanceof Timestamp timestamp) {
                offsetDateTime = timestamp.toInstant().atOffset(ZoneOffset.UTC);
            }
            else {
                offsetDateTime = OffsetDateTime.parse(value.toString());
            }
            if (timestampWithTimeZoneType.isShort()) {
                type.writeLong(builder, DateTimeEncoding.packDateTimeWithZone(
                        offsetDateTime.toInstant().toEpochMilli(),
                        TimeZoneKey.getTimeZoneKey(offsetDateTime.getOffset().getId())));
            }
            else {
                long epochMillis = offsetDateTime.toInstant().toEpochMilli();
                int picosOfMilli = (offsetDateTime.getNano() % 1_000_000) * 1_000;
                type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        epochMillis,
                        picosOfMilli,
                        TimeZoneKey.getTimeZoneKey(offsetDateTime.getOffset().getId())));
            }
            return;
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = value instanceof BigDecimal bd ? bd : new BigDecimal(value.toString());
            if (decimalType.isShort()) {
                type.writeLong(builder, decimal.setScale(decimalType.getScale(), UNNECESSARY).unscaledValue().longValueExact());
            }
            else {
                type.writeObject(builder, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
            }
            return;
        }
        if (type instanceof UuidType) {
            type.writeSlice(builder, TrinoSpecialTypeCodec.uuidSlice(value.toString()));
            return;
        }
        if (TrinoTypeClassifier.isJsonType(type)) {
            type.writeSlice(builder, TrinoSpecialTypeCodec.jsonSlice(value.toString()));
            return;
        }
        if (TrinoTypeClassifier.isIpAddressType(type)) {
            type.writeSlice(builder, TrinoSpecialTypeCodec.ipAddressSlice(value.toString()));
            return;
        }
        if (TrinoTypeClassifier.isNumberType(type)) {
            type.writeObject(builder, TrinoNumberCodec.toTrinoNumber(value));
            return;
        }
        throw new TrinoException(JDBC_ERROR, format(
                "Unsupported type %s for JDBC value of class %s",
                type.getDisplayName(),
                value.getClass().getName()));
    }

    private static Object[] toObjectArray(Object value)
    {
        if (value instanceof Array sqlArray) {
            try {
                return (Object[]) sqlArray.getArray();
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
        if (value instanceof List<?> list) {
            return list.toArray();
        }
        return (Object[]) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> toFieldValues(Object value)
    {
        if (value instanceof Row trinoRow) {
            List<Object> result = new ArrayList<>();
            for (RowField field : trinoRow.getFields()) {
                result.add(field.getValue());
            }
            return result;
        }
        if (value instanceof List<?> list) {
            return (List<Object>) list;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> result = new ArrayList<>();
            for (Object fieldValue : iterable) {
                result.add(fieldValue);
            }
            return result;
        }
        return List.of();
    }
}
