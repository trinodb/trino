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
import com.fasterxml.jackson.core.io.SerializedString;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
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
import static java.util.Objects.requireNonNull;

/**
 * Deserializer that is bug for bug compatible with Hive JsonSerDe.
 */
public class JsonSerializer
        implements LineSerializer
{
    private final RowType type;
    private final JsonFactory jsonFactory;
    private final FieldWriter[] fieldWriters;

    public JsonSerializer(List<Column> columns)
    {
        this.type = RowType.from(columns.stream()
                .map(column -> field(column.name().toLowerCase(Locale.ROOT), column.type()))
                .collect(toImmutableList()));

        fieldWriters = createRowTypeFieldWriters(this.type);

        jsonFactory = jsonFactory();
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
            generator.writeStartObject();
            for (int field = 0; field < fieldWriters.length; field++) {
                fieldWriters[field].writeField(generator, page.getBlock(field), position);
            }
            generator.writeEndObject();
        }
    }

    private static ValueWriter createValueWriter(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return (generator, block, position) -> generator.writeBoolean(BOOLEAN.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            return (generator, block, position) -> generator.writeNumber(BIGINT.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            return (generator, block, position) -> generator.writeNumber(INTEGER.getInt(block, position));
        }
        else if (SMALLINT.equals(type)) {
            return (generator, block, position) -> generator.writeNumber(SMALLINT.getShort(block, position));
        }
        else if (TINYINT.equals(type)) {
            return (generator, block, position) -> generator.writeNumber(TINYINT.getByte(block, position));
        }
        else if (type instanceof DecimalType decimalType) {
            return (generator, block, position) -> {
                SqlDecimal value = (SqlDecimal) decimalType.getObjectValue(null, block, position);
                generator.writeNumber(value.toBigDecimal().toString());
            };
        }
        else if (REAL.equals(type)) {
            return (generator, block, position) -> generator.writeNumber(REAL.getFloat(block, position));
        }
        else if (DOUBLE.equals(type)) {
            return (generator, block, position) -> generator.writeNumber(DOUBLE.getDouble(block, position));
        }
        else if (DATE.equals(type)) {
            return (generator, block, position) -> generator.writeString(HiveFormatUtils.formatHiveDate(block, position));
        }
        else if (type instanceof TimestampType timestampType) {
            return (generator, block, position) -> generator.writeString(HiveFormatUtils.formatHiveTimestamp(timestampType, block, position));
        }
        else if (VARBINARY.equals(type)) {
            return (generator, block, position) -> {
                // This corrupts the data, but this is exactly what Hive does, so we get the same result as Hive
                String value = VARBINARY.getSlice(block, position).toStringUtf8();
                generator.writeString(value);
            };
        }
        else if (type instanceof VarcharType varcharType) {
            return (generator, block, position) -> generator.writeString(varcharType.getSlice(block, position).toStringUtf8());
        }
        else if (type instanceof CharType charType) {
            return (generator, block, position) -> generator.writeString(Chars.padSpaces(charType.getSlice(block, position), charType).toStringUtf8());
        }
        else if (type instanceof ArrayType arrayType) {
            return new ArrayValueWriter(arrayType, createValueWriter(arrayType.getElementType()));
        }
        else if (type instanceof MapType mapType) {
            return new MapValueWriter(mapType, createMapKeyFunction(mapType.getKeyType()), createValueWriter(mapType.getValueType()));
        }
        else if (type instanceof RowType rowType) {
            return new RowValueWriter(rowType, createRowTypeFieldWriters(rowType));
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    private static FieldWriter[] createRowTypeFieldWriters(RowType rowType)
    {
        List<Field> fields = rowType.getFields();
        FieldWriter[] writers = new FieldWriter[fields.size()];
        int index = 0;
        for (Field field : fields) {
            writers[index++] = new FieldWriter(new SerializedString(field.getName().orElseThrow()), createValueWriter(field.getType()));
        }
        return writers;
    }

    private static ToMapKeyFunction createMapKeyFunction(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return (block, position) -> String.valueOf(BOOLEAN.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            return (block, position) -> String.valueOf(BIGINT.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            return (block, position) -> String.valueOf(INTEGER.getInt(block, position));
        }
        else if (SMALLINT.equals(type)) {
            return (block, position) -> String.valueOf(SMALLINT.getShort(block, position));
        }
        else if (TINYINT.equals(type)) {
            return (block, position) -> String.valueOf(TINYINT.getByte(block, position));
        }
        else if (type instanceof DecimalType decimalType) {
            return (block, position) -> decimalType.getObjectValue(null, block, position).toString();
        }
        else if (REAL.equals(type)) {
            return (block, position) -> String.valueOf(REAL.getFloat(block, position));
        }
        else if (DOUBLE.equals(type)) {
            return (block, position) -> String.valueOf(DOUBLE.getDouble(block, position));
        }
        else if (DATE.equals(type)) {
            return HiveFormatUtils::formatHiveDate;
        }
        else if (type instanceof TimestampType timestampType) {
            return (block, position) -> HiveFormatUtils.formatHiveTimestamp(timestampType, block, position);
        }
        else if (VARBINARY.equals(type)) {
            // This corrupts the data, but this is exactly what Hive does, so we get the same result as Hive
            return (block, position) -> VARBINARY.getSlice(block, position).toStringUtf8();
        }
        else if (type instanceof VarcharType varcharType) {
            return (block, position) -> varcharType.getSlice(block, position).toStringUtf8();
        }
        else if (type instanceof CharType charType) {
            return (block, position) -> Chars.padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        throw new UnsupportedOperationException("Unsupported map key type: " + type);
    }

    private record FieldWriter(SerializedString fieldName, ValueWriter valueWriter)
    {
        /**
         * Writes the combined field name and value for the given position into the JSON output
         */
        public void writeField(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            generator.writeFieldName(fieldName);
            valueWriter.writeValue(generator, block, position);
        }
    }

    private interface ValueWriter
    {
        default void writeValue(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
            }
            else {
                writeNonNull(generator, block, position);
            }
        }

        /**
         * Writes only a single position value as JSON without any field name. This caller <strong>must</strong> ensure
         * that the block position is non-null before invoking this method.
         */
        void writeNonNull(JsonGenerator generator, Block block, int position)
                throws IOException;
    }

    private record ArrayValueWriter(ArrayType arrayType, ValueWriter elementWriter)
            implements ValueWriter
    {
        @Override
        public void writeNonNull(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            Block arrayBlock = requireNonNull(arrayType.getObject(block, position));
            generator.writeStartArray();
            for (int arrayIndex = 0; arrayIndex < arrayBlock.getPositionCount(); arrayIndex++) {
                elementWriter.writeValue(generator, arrayBlock, arrayIndex);
            }
            generator.writeEndArray();
        }
    }

    private interface ToMapKeyFunction
    {
        String apply(Block mapBlock, int mapIndex);
    }

    private record MapValueWriter(MapType mapType, ToMapKeyFunction toMapKey, ValueWriter valueWriter)
            implements ValueWriter
    {
        @Override
        public void writeNonNull(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            Block mapBlock = requireNonNull(mapType.getObject(block, position));
            generator.writeStartObject();
            for (int mapIndex = 0; mapIndex < mapBlock.getPositionCount(); mapIndex += 2) {
                checkArgument(!mapBlock.isNull(mapIndex), "map key is null");
                generator.writeFieldName(toMapKey.apply(mapBlock, mapIndex));
                valueWriter.writeValue(generator, mapBlock, mapIndex + 1);
            }
            generator.writeEndObject();
        }
    }

    private record RowValueWriter(RowType rowType, FieldWriter[] fieldWriters)
            implements ValueWriter
    {
        @Override
        public void writeNonNull(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            Block rowBlock = requireNonNull(rowType.getObject(block, position));
            generator.writeStartObject();
            for (int field = 0; field < fieldWriters.length; field++) {
                FieldWriter writer = fieldWriters[field];
                writer.writeField(generator, rowBlock, field);
            }
            generator.writeEndObject();
        }
    }
}
