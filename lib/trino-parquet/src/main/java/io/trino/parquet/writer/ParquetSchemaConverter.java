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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

public class ParquetSchemaConverter
{
    private Map<List<String>, Type> primitiveTypes = new HashMap<>();
    private final MessageType messageType;

    public ParquetSchemaConverter(List<Type> types, List<String> columnNames)
    {
        requireNonNull(types, "types is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(types.size() == columnNames.size(), "types size not equals to columnNames size");
        this.messageType = convert(types, columnNames);
    }

    private MessageType convert(List<Type> types, List<String> columnNames)
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < types.size(); i++) {
            builder.addField(convert(types.get(i), columnNames.get(i), ImmutableList.of()));
        }
        return builder.named("trino_schema");
    }

    private org.apache.parquet.schema.Type convert(Type type, String name, List<String> parent)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            return getRowType((RowType) type, name, parent);
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            return getMapType((MapType) type, name, parent);
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            return getArrayType((ArrayType) type, name, parent);
        }
        else {
            return getPrimitiveType(type, name, parent);
        }
    }

    private org.apache.parquet.schema.Type getPrimitiveType(Type type, String name, List<String> parent)
    {
        List<String> fullName = ImmutableList.<String>builder().addAll(parent).add(name).build();
        primitiveTypes.put(fullName, type);
        if (BOOLEAN.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, OPTIONAL).named(name);
        }
        if (INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named(name);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.getPrecision() <= 9) {
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(OriginalType.DECIMAL)
                        .precision(decimalType.getPrecision())
                        .scale(decimalType.getScale()).named(name);
            }
            else if (decimalType.isShort()) {
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(OriginalType.DECIMAL)
                        .precision(decimalType.getPrecision())
                        .scale(decimalType.getScale()).named(name);
            }
            else {
                return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(16)
                        .as(OriginalType.DECIMAL)
                        .precision(decimalType.getPrecision())
                        .scale(decimalType.getScale()).named(name);
            }
        }
        if (DATE.equals(type)) {
            return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.DATE).named(name);
        }
        if (BIGINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL).named(name);
        }

        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;

            if (timestampType.getPrecision() <= 3) {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(name);
            }
            if (timestampType.getPrecision() <= 6) {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named(name);
            }
            if (timestampType.getPrecision() <= 9) {
                // Per https://github.com/apache/parquet-format/blob/master/LogicalTypes.md, nanosecond precision timestamp should be stored as INT64
                // even though it can only hold values within 1677-09-21 00:12:43 and 2262-04-11 23:47:16 range.
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)).named(name);
            }
        }
        if (DOUBLE.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, OPTIONAL).named(name);
        }
        if (RealType.REAL.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, OPTIONAL).named(name);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(name);
        }
        if (type instanceof VarbinaryType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).named(name);
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported primitive type: %s", type));
    }

    private org.apache.parquet.schema.Type getArrayType(ArrayType type, String name, List<String> parent)
    {
        Type elementType = type.getElementType();
        return Types.list(OPTIONAL)
                .element(convert(elementType, "array", ImmutableList.<String>builder().addAll(parent).add(name).add("list").build()))
                .named(name);
    }

    private org.apache.parquet.schema.Type getMapType(MapType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("key_value").build();
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        return Types.map(OPTIONAL)
                .key(convert(keyType, "key", parent))
                .value(convert(valueType, "value", parent))
                .named(name);
    }

    private org.apache.parquet.schema.Type getRowType(RowType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).build();
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(OPTIONAL);
        for (RowType.Field field : type.getFields()) {
            checkArgument(field.getName().isPresent(), "field in struct type doesn't have name");
            builder.addField(convert(field.getType(), field.getName().get(), parent));
        }
        return builder.named(name);
    }

    public Map<List<String>, Type> getPrimitiveTypes()
    {
        return primitiveTypes;
    }

    public MessageType getMessageType()
    {
        return messageType;
    }
}
