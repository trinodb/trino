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
package io.trino.hive.formats.line.openxjson;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.HiveFormatUtils;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.line.openxjson.JsonWriter.writeJsonArray;
import static io.trino.hive.formats.line.openxjson.JsonWriter.writeJsonObject;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Objects.requireNonNull;

public class OpenXJsonSerializer
        implements LineSerializer
{
    private static final DateTimeFormatter UTC_PRINT_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .appendLiteral('Z')
            .toFormatter();

    private final List<Column> columns;
    private final OpenXJsonOptions options;

    public OpenXJsonSerializer(List<Column> columns, OpenXJsonOptions options)
    {
        this.columns = ImmutableList.copyOf(columns);
        this.options = requireNonNull(options, "options is null");
        for (Column column : columns) {
            if (!isSupportedType(column.type())) {
                throw new IllegalArgumentException("Unsupported column type: " + column);
            }
        }
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return columns.stream().map(Column::type).collect(toImmutableList());
    }

    @Override
    public void write(Page page, int position, SliceOutput sliceOutput)
            throws IOException
    {
        Map<String, Object> jsonObject = new LinkedHashMap<>();

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            Column column = columns.get(columnIndex);
            String fieldName = column.name();

            Block block = page.getBlock(columnIndex);
            Object fieldValue = writeValue(column.type(), block, position);
            if (options.isExplicitNull() || fieldValue != null) {
                jsonObject.put(fieldName, fieldValue);
            }
        }
        sliceOutput.write(writeJsonObject(jsonObject).getBytes(StandardCharsets.UTF_8));
    }

    private Object writeValue(Type type, Block block, int position)
            throws InvalidJsonException
    {
        if (block.isNull(position)) {
            return null;
        }

        if (BOOLEAN.equals(type)) {
            return BOOLEAN.getBoolean(block, position);
        }
        else if (BIGINT.equals(type)) {
            return BIGINT.getLong(block, position);
        }
        else if (INTEGER.equals(type)) {
            return INTEGER.getInt(block, position);
        }
        else if (SMALLINT.equals(type)) {
            return SMALLINT.getShort(block, position);
        }
        else if (TINYINT.equals(type)) {
            return TINYINT.getByte(block, position);
        }
        else if (type instanceof DecimalType) {
            // decimal type is read-only in Hive, but we support it
            SqlDecimal value = (SqlDecimal) type.getObjectValue(null, block, position);
            return value.toBigDecimal().toString();
        }
        else if (REAL.equals(type)) {
            return REAL.getFloat(block, position);
        }
        else if (DOUBLE.equals(type)) {
            return DOUBLE.getDouble(block, position);
        }
        else if (DATE.equals(type)) {
            // date type is read-only in Hive, but we support it
            return HiveFormatUtils.formatHiveDate(block, position);
        }
        else if (type instanceof TimestampType) {
            SqlTimestamp objectValue = (SqlTimestamp) type.getObjectValue(null, block, position);
            LocalDateTime localDateTime = objectValue.toLocalDateTime();
            return UTC_PRINT_FORMATTER.format(localDateTime);
        }
        else if (VARBINARY.equals(type)) {
            // varbinary type is read-only in Hive, but we support it
            return Base64.getEncoder().encodeToString(VARBINARY.getSlice(block, position).getBytes());
        }
        else if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        else if (type instanceof CharType charType) {
            // char type is read-only in Hive, but we support it
            return Chars.padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        else if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            Block arrayBlock = arrayType.getObject(block, position);

            List<Object> jsonArray = new ArrayList<>();
            for (int arrayIndex = 0; arrayIndex < arrayBlock.getPositionCount(); arrayIndex++) {
                Object elementValue = writeValue(elementType, arrayBlock, arrayIndex);
                jsonArray.add(elementValue);
            }
            return jsonArray;
        }
        else if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            if (isStructuralType(keyType)) {
                throw new RuntimeException("Unsupported map key type: " + keyType);
            }
            Type valueType = mapType.getValueType();
            Block mapBlock = mapType.getObject(block, position);

            Map<String, Object> jsonMap = new LinkedHashMap<>();
            for (int mapIndex = 0; mapIndex < mapBlock.getPositionCount(); mapIndex += 2) {
                try {
                    Object key = writeValue(keyType, mapBlock, mapIndex);
                    if (key == null) {
                        throw new RuntimeException("OpenX JsonSerDe can not write a null map key");
                    }
                    String fieldName;
                    if (key instanceof Map<?, ?> jsonObject) {
                        fieldName = writeJsonObject(jsonObject);
                    }
                    else if (key instanceof List<?> list) {
                        fieldName = writeJsonArray(list);
                    }
                    else {
                        fieldName = key.toString();
                    }

                    Object value = writeValue(valueType, mapBlock, mapIndex + 1);
                    jsonMap.put(fieldName, value);
                }
                catch (InvalidJsonException ignored) {
                }
            }
            return jsonMap;
        }
        else if (type instanceof RowType rowType) {
            List<Field> fields = rowType.getFields();
            Block rowBlock = rowType.getObject(block, position);

            Map<String, Object> jsonObject = new LinkedHashMap<>();
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                Field field = fields.get(fieldIndex);
                String fieldName = field.getName().orElseThrow();
                Object fieldValue = writeValue(field.getType(), rowBlock, fieldIndex);
                if (options.isExplicitNull() || fieldValue != null) {
                    jsonObject.put(fieldName, fieldValue);
                }
            }
            return jsonObject;
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    public static boolean isSupportedType(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return isSupportedType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return !isStructuralType(mapType.getKeyType()) &&
                   isSupportedType(mapType.getKeyType()) &&
                   isSupportedType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(Field::getType)
                    .allMatch(OpenXJsonSerializer::isSupportedType);
        }

        return BOOLEAN.equals(type) ||
               BIGINT.equals(type) ||
               INTEGER.equals(type) ||
               SMALLINT.equals(type) ||
               TINYINT.equals(type) ||
               type instanceof DecimalType ||
               REAL.equals(type) ||
               DOUBLE.equals(type) ||
               DATE.equals(type) ||
               type instanceof TimestampType ||
               VARBINARY.equals(type) ||
               type instanceof VarcharType ||
               type instanceof CharType;
    }

    private static boolean isStructuralType(Type type)
    {
        return type instanceof MapType || type instanceof ArrayType || type instanceof RowType;
    }
}
