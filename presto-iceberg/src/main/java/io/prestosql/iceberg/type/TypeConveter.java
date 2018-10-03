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
package io.prestosql.iceberg.type;

import com.google.common.collect.ImmutableList;
import com.netflix.iceberg.types.Types;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;

public class TypeConveter
{
    private static Map<Class<Type>, com.netflix.iceberg.types.Type> prestoTypeToIcebergType = new HashMap()
    {
        {
            put(BooleanType.class, Types.BooleanType.get());
            put(VarbinaryType.class, Types.BinaryType.get());
            put(DateType.class, Types.DateType.get());
            put(DoubleType.class, Types.DoubleType.get());
            put(BigintType.class, Types.LongType.get());
            put(RealType.class, Types.FloatType.get());
            put(IntegerType.class, Types.IntegerType.get());
            put(TimeType.class, Types.TimeType.get());
            put(TimestampType.class, Types.TimestampType.withoutZone());
            put(TimestampWithTimeZoneType.class, Types.TimestampType.withZone());
            put(VarcharType.class, Types.StringType.get());
        }
    };

    private TypeConveter()
    {
    }

    public static Type convert(com.netflix.iceberg.types.Type type, TypeManager typeManager)
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
                com.netflix.iceberg.types.Types.DecimalType decimalType = (com.netflix.iceberg.types.Types.DecimalType) type;
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
                com.netflix.iceberg.types.Types.TimestampType timestampType = (com.netflix.iceberg.types.Types.TimestampType) type;
                if (timestampType.shouldAdjustToUTC()) {
                    return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
                }
                else {
                    return TimestampType.TIMESTAMP;
                }
            case UUID:
            case STRING:
                return VarcharType.createUnboundedVarcharType();
            case STRUCT:
                com.netflix.iceberg.types.Types.StructType structType = (com.netflix.iceberg.types.Types.StructType) type;
                List<com.netflix.iceberg.types.Types.NestedField> fields = structType.fields();
                List<RowType.Field> fieldList = new ArrayList<>();
                for (com.netflix.iceberg.types.Types.NestedField field : fields) {
                    fieldList.add(new RowType.Field(Optional.of(field.name()), convert(field.type(), typeManager)));
                }
                return RowType.from(fieldList);
            case LIST:
                com.netflix.iceberg.types.Types.ListType listType = (com.netflix.iceberg.types.Types.ListType) type;
                return new ArrayType(convert(listType.elementType(), typeManager));
            case MAP:
                com.netflix.iceberg.types.Types.MapType mapType = (com.netflix.iceberg.types.Types.MapType) type;
                Type keyType = convert(mapType.keyType(), typeManager);
                Type valType = convert(mapType.valueType(), typeManager);
                MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
                MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
                MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
                MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
                MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
                return new MapType(
                        keyType,
                        valType,
                        keyBlockNativeEquals,
                        keyBlockEquals,
                        keyNativeHashCode,
                        keyBlockHashCode);
            default:
                throw new UnsupportedOperationException("can not fromIceberg type id = " + type.typeId() + " from iceberg to preto type, type = " + type);
        }
    }

    public static com.netflix.iceberg.types.Type convert(Type type)
    {
        if (prestoTypeToIcebergType.containsKey(type.getClass())) {
            return prestoTypeToIcebergType.get(type.getClass());
        }
        else if (type instanceof DecimalType) {
            return handle((DecimalType) type);
        }
        else if (type instanceof RowType) {
            return handle((RowType) type);
        }
        else if (type instanceof ArrayType) {
            return handle((ArrayType) type);
        }
        else if (type instanceof MapType) {
            return handle((MapType) type);
        }
        else {
            throw new IllegalArgumentException("Iceberg does not support presto type " + type);
        }
    }

    private static com.netflix.iceberg.types.Type handle(DecimalType type)
    {
        return Types.DecimalType.of(type.getPrecision(), type.getScale());
    }

    private static com.netflix.iceberg.types.Type handle(RowType type)
    {
        final List<RowType.Field> fields = type.getFields();
        // TODO 1 needs to be an incremented ID and field.getName() is optional so we need to throw an exception if it has no value.
        final List<Types.NestedField> icebergRowFields = fields.stream().map(field -> Types.NestedField.required(1, field.getName().get(), convert(field.getType()))).collect(Collectors.toList());
        return Types.StructType.of(icebergRowFields);
    }

    private static com.netflix.iceberg.types.Type handle(ArrayType type)
    {
        return Types.ListType.ofOptional(1, convert(type.getElementType()));
    }

    private static com.netflix.iceberg.types.Type handle(MapType type)
    {
        return Types.MapType.ofOptional(1, 2, convert(type.getKeyType()), convert(type.getValueType()));
    }
}
