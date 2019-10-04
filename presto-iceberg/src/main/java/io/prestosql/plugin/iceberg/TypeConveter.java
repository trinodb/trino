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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveType.HIVE_BINARY;
import static io.prestosql.plugin.hive.HiveType.HIVE_BOOLEAN;
import static io.prestosql.plugin.hive.HiveType.HIVE_BYTE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DATE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.prestosql.plugin.hive.HiveType.HIVE_FLOAT;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_SHORT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.prestosql.plugin.hive.util.HiveUtil.isArrayType;
import static io.prestosql.plugin.hive.util.HiveUtil.isMapType;
import static io.prestosql.plugin.hive.util.HiveUtil.isRowType;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;

public final class TypeConveter
{
    private static final Map<Class<? extends Type>, org.apache.iceberg.types.Type> PRESTO_TO_ICEBERG = ImmutableMap.<Class<? extends Type>, org.apache.iceberg.types.Type>builder()
            .put(BooleanType.class, Types.BooleanType.get())
            .put(VarbinaryType.class, Types.BinaryType.get())
            .put(DateType.class, Types.DateType.get())
            .put(DoubleType.class, Types.DoubleType.get())
            .put(BigintType.class, Types.LongType.get())
            .put(RealType.class, Types.FloatType.get())
            .put(IntegerType.class, Types.IntegerType.get())
            .put(TimeType.class, Types.TimeType.get())
            .put(TimestampType.class, Types.TimestampType.withoutZone())
            .put(TimestampWithTimeZoneType.class, Types.TimestampType.withZone())
            .put(VarcharType.class, Types.StringType.get())
            .build();

    private TypeConveter() {}

    public static Type toPrestoType(org.apache.iceberg.types.Type type, TypeManager typeManager)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BINARY:
            case FIXED:
                return VarbinaryType.VARBINARY;
            case DATE:
                return DateType.DATE;
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return DecimalType.createDecimalType(decimalType.precision(), decimalType.scale());
            case DOUBLE:
                return DoubleType.DOUBLE;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case INTEGER:
                return IntegerType.INTEGER;
            case TIME:
                return TimeType.TIME;
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) type;
                if (timestampType.shouldAdjustToUTC()) {
                    return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
                }
                return TimestampType.TIMESTAMP;
            case UUID:
            case STRING:
                return VarcharType.createUnboundedVarcharType();
            case LIST:
                Types.ListType listType = (Types.ListType) type;
                return new ArrayType(toPrestoType(listType.elementType(), typeManager));
            case MAP:
                Types.MapType mapType = (Types.MapType) type;
                TypeSignature keyType = toPrestoType(mapType.keyType(), typeManager).getTypeSignature();
                TypeSignature valueType = toPrestoType(mapType.valueType(), typeManager).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.typeParameter(keyType), TypeSignatureParameter.typeParameter(valueType)));
            case STRUCT:
                List<Types.NestedField> fields = ((Types.StructType) type).fields();
                return RowType.from(fields.stream()
                        .map(field -> new RowType.Field(Optional.of(field.name()), toPrestoType(field.type(), typeManager)))
                        .collect(toImmutableList()));
            default:
                throw new UnsupportedOperationException(format("Cannot convert from Iceberg type '%s' (%s) to Presto type", type, type.typeId()));
        }
    }

    public static org.apache.iceberg.types.Type toIcebergType(Type type)
    {
        if (PRESTO_TO_ICEBERG.containsKey(type.getClass())) {
            return PRESTO_TO_ICEBERG.get(type.getClass());
        }
        if (type instanceof DecimalType) {
            return fromDecimal((DecimalType) type);
        }
        if (type instanceof RowType) {
            return fromRow((RowType) type);
        }
        if (type instanceof ArrayType) {
            return fromArray((ArrayType) type);
        }
        if (type instanceof MapType) {
            return fromMap((MapType) type);
        }
        throw new PrestoException(NOT_SUPPORTED, "Type not supported for Iceberg: " + type.getDisplayName());
    }

    public static HiveType toHiveType(Type type)
    {
        return HiveType.toHiveType(toHiveTypeInfo(type));
    }

    private static org.apache.iceberg.types.Type fromDecimal(DecimalType type)
    {
        return Types.DecimalType.of(type.getPrecision(), type.getScale());
    }

    private static org.apache.iceberg.types.Type fromRow(RowType type)
    {
        List<Types.NestedField> fields = new ArrayList<>();
        for (RowType.Field field : type.getFields()) {
            String name = field.getName().orElseThrow(() ->
                    new PrestoException(NOT_SUPPORTED, "Row type field does not have a name: " + type.getDisplayName()));
            fields.add(Types.NestedField.required(fields.size() + 1, name, toIcebergType(field.getType())));
        }
        return Types.StructType.of(fields);
    }

    private static org.apache.iceberg.types.Type fromArray(ArrayType type)
    {
        return Types.ListType.ofOptional(1, toIcebergType(type.getElementType()));
    }

    private static org.apache.iceberg.types.Type fromMap(MapType type)
    {
        return Types.MapType.ofOptional(1, 2, toIcebergType(type.getKeyType()), toIcebergType(type.getValueType()));
    }

    private static TypeInfo toHiveTypeInfo(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return HIVE_BOOLEAN.getTypeInfo();
        }
        if (BIGINT.equals(type)) {
            return HIVE_LONG.getTypeInfo();
        }
        if (INTEGER.equals(type)) {
            return HIVE_INT.getTypeInfo();
        }
        if (SMALLINT.equals(type)) {
            return HIVE_SHORT.getTypeInfo();
        }
        if (TINYINT.equals(type)) {
            return HIVE_BYTE.getTypeInfo();
        }
        if (REAL.equals(type)) {
            return HIVE_FLOAT.getTypeInfo();
        }
        if (DOUBLE.equals(type)) {
            return HIVE_DOUBLE.getTypeInfo();
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return HIVE_STRING.getTypeInfo();
            }
            if (varcharType.getBoundedLength() <= HiveVarchar.MAX_VARCHAR_LENGTH) {
                return getVarcharTypeInfo(varcharType.getBoundedLength());
            }
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported VARCHAR types: VARCHAR(<=%d), VARCHAR.", type, HiveVarchar.MAX_VARCHAR_LENGTH));
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            int charLength = charType.getLength();
            if (charLength <= HiveChar.MAX_CHAR_LENGTH) {
                return getCharTypeInfo(charLength);
            }
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported CHAR types: CHAR(<=%d).",
                    type, HiveChar.MAX_CHAR_LENGTH));
        }
        if (VARBINARY.equals(type)) {
            return HIVE_BINARY.getTypeInfo();
        }
        if (DATE.equals(type)) {
            return HIVE_DATE.getTypeInfo();
        }
        if (TIMESTAMP.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            // Hive does not have TIMESTAMP_WITH_TIME_ZONE, this is just a work around for iceberg.
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (isArrayType(type)) {
            TypeInfo elementType = toHiveTypeInfo(type.getTypeParameters().get(0));
            return getListTypeInfo(elementType);
        }
        if (isMapType(type)) {
            TypeInfo keyType = toHiveTypeInfo(type.getTypeParameters().get(0));
            TypeInfo valueType = toHiveTypeInfo(type.getTypeParameters().get(1));
            return getMapTypeInfo(keyType, valueType);
        }
        if (isRowType(type)) {
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            for (TypeSignatureParameter parameter : type.getTypeSignature().getParameters()) {
                if (!parameter.isNamedTypeSignature()) {
                    throw new IllegalArgumentException(format("Expected all parameters to be named type, but got %s", parameter));
                }
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                if (!namedTypeSignature.getName().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, format("Anonymous row type is not supported in Hive. Please give each field a name: %s", type));
                }
                fieldNames.add(namedTypeSignature.getName().get());
            }
            return getStructTypeInfo(
                    fieldNames.build(),
                    type.getTypeParameters().stream()
                            .map(TypeConveter::toHiveTypeInfo)
                            .collect(toList()));
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", type));
    }
}
