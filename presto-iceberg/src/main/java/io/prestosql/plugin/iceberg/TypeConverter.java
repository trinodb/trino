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
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.spi.PrestoException;
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
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.iceberg.IcebergUtil.ICEBERG_FIELD_ID_KEY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public final class TypeConverter
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

    private TypeConverter() {}

    public static Type toPrestoType(org.apache.iceberg.types.Type type, TypeManager typeManager)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case UUID:
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
            case STRING:
                return VarcharType.createUnboundedVarcharType();
            case LIST:
                Types.ListType listType = (Types.ListType) type;
                return new ArrayType(toPrestoType(listType.elementType(), typeManager));
            case MAP:
                Types.MapType mapType = (Types.MapType) type;
                TypeSignature keyType = toPrestoType(mapType.keyType(), typeManager).getTypeSignature();
                TypeSignature valueType = toPrestoType(mapType.valueType(), typeManager).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
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

    private static List<OrcType> toOrcType(int nextFieldTypeIndex, org.apache.iceberg.types.Type type, int fieldId)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BOOLEAN, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case INTEGER:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case LONG:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case FLOAT:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.FLOAT, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case DOUBLE:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.DOUBLE, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case DATE:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.DATE, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case TIME:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case TIMESTAMP:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.TIMESTAMP, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case STRING:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.STRING, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case UUID:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case FIXED:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case BINARY:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.empty(), Optional.empty()));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.DECIMAL, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)), Optional.empty(), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale())));
            case STRUCT:
                return toOrcStructType(nextFieldTypeIndex, (Types.StructType) type, fieldId);
            case LIST:
                return toOrcListType(nextFieldTypeIndex, (Types.ListType) type, fieldId);
            case MAP:
                return toOrcMapType(nextFieldTypeIndex, (Types.MapType) type, fieldId);
            default:
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Iceberg type: %s", type));
        }
    }

    public static List<OrcType> toOrcStructType(int nextFieldTypeIndex, Types.StructType structType, int fieldId)
    {
        nextFieldTypeIndex++;
        List<Integer> fieldTypeIndexes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<List<OrcType>> fieldTypesList = new ArrayList<>();
        for (Types.NestedField field : structType.fields()) {
            fieldTypeIndexes.add(nextFieldTypeIndex);
            fieldNames.add(field.name());
            List<OrcType> fieldOrcTypes = toOrcType(nextFieldTypeIndex, field.type(), field.fieldId());
            fieldTypesList.add(fieldOrcTypes);
            nextFieldTypeIndex += fieldOrcTypes.size();
        }

        ImmutableList.Builder<OrcType> orcTypes = ImmutableList.builder();
        orcTypes.add(new OrcType(
                OrcType.OrcTypeKind.STRUCT,
                fieldTypeIndexes,
                fieldNames,
                ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
        fieldTypesList.forEach(orcTypes::addAll);

        return orcTypes.build();
    }

    private static List<OrcType> toOrcListType(int nextFieldTypeIndex, Types.ListType listType, int fieldId)
    {
        nextFieldTypeIndex++;
        List<OrcType> itemTypes = toOrcType(nextFieldTypeIndex, listType.elementType(), listType.elementId());

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcType.OrcTypeKind.LIST,
                ImmutableList.of(nextFieldTypeIndex),
                ImmutableList.of("item"),
                ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        orcTypes.addAll(itemTypes);
        return orcTypes;
    }

    private static List<OrcType> toOrcMapType(int nextFieldTypeIndex, Types.MapType mapType, int fieldId)
    {
        nextFieldTypeIndex++;
        List<OrcType> keyTypes = toOrcType(nextFieldTypeIndex, mapType.keyType(), mapType.keyId());
        List<OrcType> valueTypes = toOrcType(nextFieldTypeIndex + keyTypes.size(), mapType.valueType(), mapType.valueId());

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcType.OrcTypeKind.MAP,
                ImmutableList.of(nextFieldTypeIndex, nextFieldTypeIndex + keyTypes.size()),
                ImmutableList.of("key", "value"),
                ImmutableMap.of(ICEBERG_FIELD_ID_KEY, Integer.toString(fieldId)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        orcTypes.addAll(keyTypes);
        orcTypes.addAll(valueTypes);
        return orcTypes;
    }
}
