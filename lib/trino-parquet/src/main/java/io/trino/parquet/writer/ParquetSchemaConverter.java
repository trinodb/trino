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
import com.google.common.collect.ImmutableMap;
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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class ParquetSchemaConverter
{
    // Map precision to the number bytes needed for binary conversion.
    // Based on org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
    private static final int[] PRECISION_TO_BYTE_COUNT = new int[MAX_PRECISION + 1];

    static {
        for (int precision = 1; precision <= MAX_PRECISION; precision++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[precision] = (int) Math.ceil((Math.log(Math.pow(10, precision) - 1) / Math.log(2) + 1) / 8);
        }
    }

    public static final boolean HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING = true;
    public static final boolean HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING = true;

    private final Map<List<String>, Type> primitiveTypes;
    private final MessageType messageType;

    public ParquetSchemaConverter(List<Type> types, List<String> columnNames, boolean useLegacyDecimalEncoding, boolean useInt96TimestampEncoding)
    {
        requireNonNull(types, "types is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(types.size() == columnNames.size(), "types size not equals to columnNames size");
        ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder = ImmutableMap.builder();
        messageType = convert(types, columnNames, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesBuilder::put);
        primitiveTypes = primitiveTypesBuilder.buildOrThrow();
    }

    public Map<List<String>, Type> getPrimitiveTypes()
    {
        return primitiveTypes;
    }

    public MessageType getMessageType()
    {
        return messageType;
    }

    private static MessageType convert(
            List<Type> types,
            List<String> columnNames,
            boolean useLegacyDecimalEncoding,
            boolean useInt96TimestampEncoding,
            BiConsumer<List<String>, Type> primitiveTypesConsumer)
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < types.size(); i++) {
            builder.addField(convert(types.get(i), columnNames.get(i), ImmutableList.of(), OPTIONAL, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer));
        }
        return builder.named("trino_schema");
    }

    private static org.apache.parquet.schema.Type convert(
            Type type,
            String name,
            List<String> parent,
            Repetition repetition,
            boolean useLegacyDecimalEncoding,
            boolean useInt96TimestampEncoding,
            BiConsumer<List<String>, Type> primitiveTypesConsumer)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            return getRowType((RowType) type, name, parent, repetition, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer);
        }
        if (MAP.equals(type.getTypeSignature().getBase())) {
            return getMapType((MapType) type, name, parent, repetition, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer);
        }
        if (ARRAY.equals(type.getTypeSignature().getBase())) {
            return getArrayType((ArrayType) type, name, parent, repetition, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer);
        }
        return getPrimitiveType(type, name, parent, repetition, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer);
    }

    private static org.apache.parquet.schema.Type getPrimitiveType(
            Type type,
            String name,
            List<String> parent,
            Repetition repetition,
            boolean useLegacyDecimalEncoding,
            boolean useInt96TimestampEncoding,
            BiConsumer<List<String>, Type> primitiveTypesConsumer)
    {
        List<String> fullName = ImmutableList.<String>builder().addAll(parent).add(name).build();
        primitiveTypesConsumer.accept(fullName, type);
        if (BOOLEAN.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name);
        }
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#signed-integers
        // INT(32, true) and INT(64, true) are implied by the int32 and int64 primitive types if no other annotation is present.
        // Implementations may use these annotations to produce smaller in-memory representations when reading data.
        if (TINYINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .as(intType(8, true))
                    .named(name);
        }
        if (SMALLINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .as(intType(16, true))
                    .named(name);
        }
        if (INTEGER.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .as(intType(32, true))
                    .named(name);
        }
        if (type instanceof DecimalType decimalType) {
            // Apache Hive version 3 or lower does not support reading decimals encoded as INT32/INT64
            if (!useLegacyDecimalEncoding) {
                if (decimalType.getPrecision() <= 9) {
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .as(decimalType(decimalType.getScale(), decimalType.getPrecision()))
                            .named(name);
                }
                if (decimalType.isShort()) {
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                            .as(decimalType(decimalType.getScale(), decimalType.getPrecision()))
                            .named(name);
                }
            }
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                    .length(PRECISION_TO_BYTE_COUNT[decimalType.getPrecision()])
                    .as(decimalType(decimalType.getScale(), decimalType.getPrecision()))
                    .named(name);
        }
        if (DATE.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).as(LogicalTypeAnnotation.dateType()).named(name);
        }
        if (BIGINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                    .as(intType(64, true))
                    .named(name);
        }

        if (type instanceof TimestampType timestampType) {
            // Apache Hive version 3.x or lower does not support reading timestamps encoded as INT64
            if (useInt96TimestampEncoding) {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition).named(name);
            }

            if (timestampType.getPrecision() <= 3) {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(name);
            }
            if (timestampType.getPrecision() <= 6) {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named(name);
            }
            if (timestampType.getPrecision() <= 9) {
                // Per https://github.com/apache/parquet-format/blob/master/LogicalTypes.md, nanosecond precision timestamp should be stored as INT64
                // even though it can only hold values within 1677-09-21 00:12:43 and 2262-04-11 23:47:16 range.
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)).named(name);
            }
        }
        if (DOUBLE.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name);
        }
        if (RealType.REAL.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).as(LogicalTypeAnnotation.stringType()).named(name);
        }
        if (type instanceof VarbinaryType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name);
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported primitive type: %s", type));
    }

    private static org.apache.parquet.schema.Type getArrayType(
            ArrayType type,
            String name,
            List<String> parent,
            Repetition repetition,
            boolean useLegacyDecimalEncoding,
            boolean useInt96TimestampEncoding,
            BiConsumer<List<String>, Type> primitiveTypesConsumer)
    {
        Type elementType = type.getElementType();
        return Types.list(repetition)
                .element(convert(elementType, "element", ImmutableList.<String>builder().addAll(parent).add(name).add("list").build(), OPTIONAL, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer))
                .named(name);
    }

    private static org.apache.parquet.schema.Type getMapType(
            MapType type,
            String name,
            List<String> parent,
            Repetition repetition,
            boolean useLegacyDecimalEncoding,
            boolean useInt96TimestampEncoding,
            BiConsumer<List<String>, Type> primitiveTypesConsumer)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("key_value").build();
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        return Types.map(repetition)
                .key(convert(keyType, "key", parent, REQUIRED, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer))
                .value(convert(valueType, "value", parent, OPTIONAL, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer))
                .named(name);
    }

    private static org.apache.parquet.schema.Type getRowType(
            RowType type,
            String name,
            List<String> parent,
            Repetition repetition,
            boolean useLegacyDecimalEncoding,
            boolean useInt96TimestampEncoding,
            BiConsumer<List<String>, Type> primitiveTypesConsumer)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).build();
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);
        for (RowType.Field field : type.getFields()) {
            checkArgument(field.getName().isPresent(), "field in struct type doesn't have name");
            builder.addField(convert(field.getType(), field.getName().get(), parent, OPTIONAL, useLegacyDecimalEncoding, useInt96TimestampEncoding, primitiveTypesConsumer));
        }
        return builder.named(name);
    }
}
