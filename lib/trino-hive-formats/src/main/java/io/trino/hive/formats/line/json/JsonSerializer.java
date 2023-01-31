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
package io.trino.hive.formats.line.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
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
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Locale;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;

/**
 * Deserializer that is bug for bug compatible with Hive JsonSerDe.
 */
public class JsonSerializer
        implements LineSerializer
{
    private final RowType type;
    private final JsonFactory jsonFactory;

    public JsonSerializer(List<Column> columns)
    {
        this.type = RowType.from(columns.stream()
                .map(column -> field(column.name().toLowerCase(Locale.ROOT), column.type()))
                .collect(toImmutableList()));

        jsonFactory = new JsonFactory();
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return type.getTypeParameters();
    }

    @Override
    public void write(Page page, int position, SliceOutput sliceOutput)
            throws IOException
    {
        try (JsonGenerator generator = jsonFactory.createGenerator((OutputStream) sliceOutput)) {
            writeStruct(generator, type, page::getBlock, position);
        }
    }

    private static void writeStruct(JsonGenerator generator, RowType rowType, IntFunction<Block> blocks, int position)
            throws IOException
    {
        generator.writeStartObject();
        List<Field> fields = rowType.getFields();
        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            Field field = fields.get(fieldIndex);
            generator.writeFieldName(field.getName().orElseThrow());
            Block block = blocks.apply(fieldIndex);
            writeValue(generator, field.getType(), block, position);
        }
        generator.writeEndObject();
    }

    private static void writeValue(JsonGenerator generator, Type type, Block block, int position)
            throws IOException
    {
        if (block.isNull(position)) {
            generator.writeNull();
            return;
        }

        if (BOOLEAN.equals(type)) {
            generator.writeBoolean(BOOLEAN.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            generator.writeNumber(BIGINT.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            generator.writeNumber(INTEGER.getLong(block, position));
        }
        else if (SMALLINT.equals(type)) {
            generator.writeNumber(SMALLINT.getLong(block, position));
        }
        else if (TINYINT.equals(type)) {
            generator.writeNumber(TINYINT.getLong(block, position));
        }
        else if (type instanceof DecimalType) {
            SqlDecimal value = (SqlDecimal) type.getObjectValue(null, block, position);
            generator.writeNumber(value.toBigDecimal().toString());
        }
        else if (REAL.equals(type)) {
            generator.writeNumber(intBitsToFloat((int) REAL.getLong(block, position)));
        }
        else if (DOUBLE.equals(type)) {
            generator.writeNumber(DOUBLE.getDouble(block, position));
        }
        else if (DATE.equals(type)) {
            generator.writeString(HiveFormatUtils.formatHiveDate(block, position));
        }
        else if (type instanceof TimestampType) {
            generator.writeString(HiveFormatUtils.formatHiveTimestamp(type, block, position));
        }
        else if (VARBINARY.equals(type)) {
            // This corrupts the data, but this is exactly what Hive does, so we get the same result as Hive
            String value = type.getSlice(block, position).toStringUtf8();
            generator.writeString(value);
        }
        else if (type instanceof VarcharType) {
            generator.writeString(type.getSlice(block, position).toStringUtf8());
        }
        else if (type instanceof CharType charType) {
            generator.writeString(Chars.padSpaces(charType.getSlice(block, position), charType).toStringUtf8());
        }
        else if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            Block arrayBlock = arrayType.getObject(block, position);

            generator.writeStartArray();
            for (int arrayIndex = 0; arrayIndex < arrayBlock.getPositionCount(); arrayIndex++) {
                writeValue(generator, elementType, arrayBlock, arrayIndex);
            }
            generator.writeEndArray();
        }
        else if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            Block mapBlock = mapType.getObject(block, position);

            generator.writeStartObject();
            for (int mapIndex = 0; mapIndex < mapBlock.getPositionCount(); mapIndex += 2) {
                generator.writeFieldName(toMapKey(keyType, mapBlock, mapIndex));
                writeValue(generator, valueType, mapBlock, mapIndex + 1);
            }
            generator.writeEndObject();
        }
        else if (type instanceof RowType rowType) {
            List<Field> fields = rowType.getFields();
            Block rowBlock = rowType.getObject(block, position);

            generator.writeStartObject();
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                Field field = fields.get(fieldIndex);
                generator.writeFieldName(field.getName().orElseThrow());
                writeValue(generator, field.getType(), rowBlock, fieldIndex);
            }
            generator.writeEndObject();
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    private static String toMapKey(Type type, Block block, int position)
    {
        checkArgument(!block.isNull(position), "map key is null");

        if (BOOLEAN.equals(type)) {
            return String.valueOf(BOOLEAN.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            return String.valueOf(BIGINT.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            return String.valueOf(INTEGER.getLong(block, position));
        }
        else if (SMALLINT.equals(type)) {
            return String.valueOf(SMALLINT.getLong(block, position));
        }
        else if (TINYINT.equals(type)) {
            return String.valueOf(TINYINT.getLong(block, position));
        }
        else if (type instanceof DecimalType) {
            return type.getObjectValue(null, block, position).toString();
        }
        else if (REAL.equals(type)) {
            return String.valueOf(intBitsToFloat((int) REAL.getLong(block, position)));
        }
        else if (DOUBLE.equals(type)) {
            return String.valueOf(DOUBLE.getDouble(block, position));
        }
        else if (DATE.equals(type)) {
            return HiveFormatUtils.formatHiveDate(block, position);
        }
        else if (type instanceof TimestampType) {
            return HiveFormatUtils.formatHiveTimestamp(type, block, position);
        }
        else if (VARBINARY.equals(type)) {
            // This corrupts the data, but this is exactly what Hive does, so we get the same result as Hive
            return type.getSlice(block, position).toStringUtf8();
        }
        else if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        else if (type instanceof CharType charType) {
            return Chars.padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        throw new UnsupportedOperationException("Unsupported map key type: " + type);
    }
}
