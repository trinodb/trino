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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static java.lang.String.format;

public final class TypeConverter
{
    public static final String ORC_ICEBERG_ID_KEY = "iceberg.id";
    public static final String ORC_ICEBERG_REQUIRED_KEY = "iceberg.required";
    public static final String ICEBERG_LONG_TYPE = "iceberg.long-type";
    public static final String ICEBERG_BINARY_TYPE = "iceberg.binary-type";

    private TypeConverter() {}

    public static Type toTrinoType(org.apache.iceberg.types.Type type, TypeManager typeManager)
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
                return TIME_MICROS;
            case TIMESTAMP:
                return ((Types.TimestampType) type).shouldAdjustToUTC() ? TIMESTAMP_TZ_MICROS : TIMESTAMP_MICROS;
            case STRING:
                return VarcharType.createUnboundedVarcharType();
            case UUID:
                return UuidType.UUID;
            case LIST:
                Types.ListType listType = (Types.ListType) type;
                return new ArrayType(toTrinoType(listType.elementType(), typeManager));
            case MAP:
                Types.MapType mapType = (Types.MapType) type;
                TypeSignature keyType = toTrinoType(mapType.keyType(), typeManager).getTypeSignature();
                TypeSignature valueType = toTrinoType(mapType.valueType(), typeManager).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.typeParameter(keyType), TypeSignatureParameter.typeParameter(valueType)));
            case STRUCT:
                List<Types.NestedField> fields = ((Types.StructType) type).fields();
                return RowType.from(fields.stream()
                        .map(field -> new RowType.Field(Optional.of(field.name()), toTrinoType(field.type(), typeManager)))
                        .collect(toImmutableList()));
        }
        throw new UnsupportedOperationException(format("Cannot convert from Iceberg type '%s' (%s) to Trino type", type, type.typeId()));
    }

    public static org.apache.iceberg.types.Type toIcebergTypeForNewColumn(Type type, AtomicInteger nextFieldId)
    {
        return toIcebergTypeInternal(type, Optional.empty(), Optional.of(nextFieldId));
    }

    public static org.apache.iceberg.types.Type toIcebergType(Type type, ColumnIdentity columnIdentity)
    {
        return toIcebergTypeInternal(type, Optional.of(columnIdentity), Optional.empty());
    }

    private static org.apache.iceberg.types.Type toIcebergTypeInternal(Type type, Optional<ColumnIdentity> columnIdentity, Optional<AtomicInteger> nextFieldId)
    {
        if (type instanceof BooleanType) {
            return Types.BooleanType.get();
        }
        if (type instanceof IntegerType) {
            return Types.IntegerType.get();
        }
        if (type instanceof BigintType) {
            return Types.LongType.get();
        }
        if (type instanceof RealType) {
            return Types.FloatType.get();
        }
        if (type instanceof DoubleType) {
            return Types.DoubleType.get();
        }
        if (type instanceof DecimalType) {
            return fromDecimal((DecimalType) type);
        }
        if (type instanceof VarcharType) {
            return Types.StringType.get();
        }
        if (type instanceof VarbinaryType) {
            return Types.BinaryType.get();
        }
        if (type instanceof DateType) {
            return Types.DateType.get();
        }
        if (type.equals(TIME_MICROS)) {
            return Types.TimeType.get();
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return Types.TimestampType.withoutZone();
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return Types.TimestampType.withZone();
        }
        if (type.equals(UUID)) {
            return Types.UUIDType.get();
        }
        if (type instanceof RowType) {
            return fromRow((RowType) type, columnIdentity, nextFieldId);
        }
        if (type instanceof ArrayType) {
            return fromArray((ArrayType) type, columnIdentity, nextFieldId);
        }
        if (type instanceof MapType) {
            return fromMap((MapType) type, columnIdentity, nextFieldId);
        }
        if (type instanceof TimeType) {
            throw new TrinoException(NOT_SUPPORTED, format("Time precision (%s) not supported for Iceberg. Use \"time(6)\" instead.", ((TimeType) type).getPrecision()));
        }
        if (type instanceof TimestampType) {
            throw new TrinoException(NOT_SUPPORTED, format("Timestamp precision (%s) not supported for Iceberg. Use \"timestamp(6)\" instead.", ((TimestampType) type).getPrecision()));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            throw new TrinoException(NOT_SUPPORTED, format("Timestamp precision (%s) not supported for Iceberg. Use \"timestamp(6) with time zone\" instead.", ((TimestampWithTimeZoneType) type).getPrecision()));
        }
        throw new TrinoException(NOT_SUPPORTED, "Type not supported for Iceberg: " + type.getDisplayName());
    }

    private static org.apache.iceberg.types.Type fromDecimal(DecimalType type)
    {
        return Types.DecimalType.of(type.getPrecision(), type.getScale());
    }

    private static org.apache.iceberg.types.Type fromRow(RowType type, Optional<ColumnIdentity> columnIdentity, Optional<AtomicInteger> nextFieldId)
    {
        checkExactlyOne(columnIdentity, nextFieldId);

        List<Types.NestedField> fields = new ArrayList<>();
        for (int i = 0; i < type.getFields().size(); i++) {
            int fieldIndex = i;
            Optional<ColumnIdentity> childColumnIdentity = columnIdentity.map(column -> column.getChildren().get(fieldIndex));
            int id = childColumnIdentity
                    .map(ColumnIdentity::getId)
                    .orElseGet(() -> nextFieldId.get().getAndIncrement());

            RowType.Field field = type.getFields().get(fieldIndex);
            String name = field.getName().orElseThrow(() ->
                    new TrinoException(NOT_SUPPORTED, "Row type field does not have a name: " + type.getDisplayName()));
            fields.add(Types.NestedField.optional(id, name, toIcebergTypeInternal(field.getType(), childColumnIdentity, nextFieldId)));
        }
        return Types.StructType.of(fields);
    }

    private static org.apache.iceberg.types.Type fromArray(ArrayType type, Optional<ColumnIdentity> columnIdentity, Optional<AtomicInteger> nextFieldId)
    {
        checkExactlyOne(columnIdentity, nextFieldId);
        Optional<ColumnIdentity> childColumnIdentity = columnIdentity.map(identity -> getOnlyElement(identity.getChildren()));
        int id = childColumnIdentity
                .map(ColumnIdentity::getId)
                .orElseGet(() -> nextFieldId.get().getAndIncrement());
        return Types.ListType.ofOptional(id, toIcebergTypeInternal(type.getElementType(), childColumnIdentity, nextFieldId));
    }

    private static org.apache.iceberg.types.Type fromMap(MapType type, Optional<ColumnIdentity> columnIdentity, Optional<AtomicInteger> nextFieldId)
    {
        checkExactlyOne(columnIdentity, nextFieldId);
        Optional<ColumnIdentity> keyColumnIdentity = columnIdentity.map(column -> column.getChildren().get(0));
        Optional<ColumnIdentity> valueColumnIdentity = columnIdentity.map(column -> column.getChildren().get(1));
        int keyId = keyColumnIdentity
                .map(ColumnIdentity::getId)
                .orElseGet(() -> nextFieldId.get().getAndIncrement());
        int valueId = valueColumnIdentity
                .map(ColumnIdentity::getId)
                .orElseGet(() -> nextFieldId.get().getAndIncrement());
        return Types.MapType.ofOptional(
                keyId,
                valueId,
                toIcebergTypeInternal(type.getKeyType(), keyColumnIdentity, nextFieldId),
                toIcebergTypeInternal(type.getValueType(), valueColumnIdentity, nextFieldId));
    }

    private static void checkExactlyOne(Optional<ColumnIdentity> columnIdentity, Optional<AtomicInteger> nextFieldId)
    {
        if ((columnIdentity.isPresent() && nextFieldId.isEmpty()) || (columnIdentity.isEmpty() && nextFieldId.isPresent())) {
            return;
        }
        throw new IllegalArgumentException("Either a column identity or nextFieldId is expected");
    }

    public static ColumnMetadata<OrcType> toOrcType(Schema schema)
    {
        return new ColumnMetadata<>(toOrcStructType(0, schema.asStruct(), ImmutableMap.of()));
    }

    private static List<OrcType> toOrcType(int nextFieldTypeIndex, org.apache.iceberg.types.Type type, Map<String, String> attributes)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return ImmutableList.of(new OrcType(OrcTypeKind.BOOLEAN, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case INTEGER:
                return ImmutableList.of(new OrcType(OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case LONG:
                return ImmutableList.of(new OrcType(OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case FLOAT:
                return ImmutableList.of(new OrcType(OrcTypeKind.FLOAT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DOUBLE:
                return ImmutableList.of(new OrcType(OrcTypeKind.DOUBLE, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DATE:
                return ImmutableList.of(new OrcType(OrcTypeKind.DATE, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case TIME:
                attributes = ImmutableMap.<String, String>builder()
                        .putAll(attributes)
                        .put(ICEBERG_LONG_TYPE, "TIME")
                        .buildOrThrow();
                return ImmutableList.of(new OrcType(OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case TIMESTAMP:
                OrcTypeKind timestampKind = ((Types.TimestampType) type).shouldAdjustToUTC() ? OrcTypeKind.TIMESTAMP_INSTANT : OrcTypeKind.TIMESTAMP;
                return ImmutableList.of(new OrcType(timestampKind, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case STRING:
                return ImmutableList.of(new OrcType(OrcTypeKind.STRING, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case FIXED:
                return ImmutableList.of(new OrcType(OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case BINARY:
                return ImmutableList.of(new OrcType(OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return ImmutableList.of(new OrcType(OrcTypeKind.DECIMAL, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.of(decimalType.precision()), Optional.of(decimalType.scale()), attributes));
            case UUID:
                attributes = ImmutableMap.<String, String>builder()
                        .putAll(attributes)
                        .put(ICEBERG_BINARY_TYPE, "UUID")
                        .buildOrThrow();
                return ImmutableList.of(new OrcType(OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case STRUCT:
                return toOrcStructType(nextFieldTypeIndex, (Types.StructType) type, attributes);
            case LIST:
                return toOrcListType(nextFieldTypeIndex, (Types.ListType) type, attributes);
            case MAP:
                return toOrcMapType(nextFieldTypeIndex, (Types.MapType) type, attributes);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported Iceberg type: " + type);
    }

    private static List<OrcType> toOrcStructType(int nextFieldTypeIndex, Types.StructType structType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;
        List<OrcColumnId> fieldTypeIndexes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<List<OrcType>> fieldTypesList = new ArrayList<>();
        for (Types.NestedField field : structType.fields()) {
            fieldTypeIndexes.add(new OrcColumnId(nextFieldTypeIndex));
            fieldNames.add(field.name());
            Map<String, String> fieldAttributes = ImmutableMap.<String, String>builder()
                    .put(ORC_ICEBERG_ID_KEY, Integer.toString(field.fieldId()))
                    .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(field.isRequired()))
                    .buildOrThrow();
            List<OrcType> fieldOrcTypes = toOrcType(nextFieldTypeIndex, field.type(), fieldAttributes);
            fieldTypesList.add(fieldOrcTypes);
            nextFieldTypeIndex += fieldOrcTypes.size();
        }

        ImmutableList.Builder<OrcType> orcTypes = ImmutableList.builder();
        orcTypes.add(new OrcType(
                OrcTypeKind.STRUCT,
                fieldTypeIndexes,
                fieldNames,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                attributes));
        fieldTypesList.forEach(orcTypes::addAll);

        return orcTypes.build();
    }

    private static List<OrcType> toOrcListType(int nextFieldTypeIndex, Types.ListType listType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;
        Map<String, String> elementAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(listType.elementId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(listType.isElementRequired()))
                .buildOrThrow();
        List<OrcType> itemTypes = toOrcType(nextFieldTypeIndex, listType.elementType(), elementAttributes);

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcTypeKind.LIST,
                ImmutableList.of(new OrcColumnId(nextFieldTypeIndex)),
                ImmutableList.of("item"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                attributes));

        orcTypes.addAll(itemTypes);
        return orcTypes;
    }

    private static List<OrcType> toOrcMapType(int nextFieldTypeIndex, Types.MapType mapType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;
        Map<String, String> keyAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(mapType.keyId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(true))
                .buildOrThrow();
        List<OrcType> keyTypes = toOrcType(nextFieldTypeIndex, mapType.keyType(), keyAttributes);
        Map<String, String> valueAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(mapType.valueId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(mapType.isValueRequired()))
                .buildOrThrow();
        List<OrcType> valueTypes = toOrcType(nextFieldTypeIndex + keyTypes.size(), mapType.valueType(), valueAttributes);

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcTypeKind.MAP,
                ImmutableList.of(new OrcColumnId(nextFieldTypeIndex), new OrcColumnId(nextFieldTypeIndex + keyTypes.size())),
                ImmutableList.of("key", "value"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                attributes));

        orcTypes.addAll(keyTypes);
        orcTypes.addAll(valueTypes);
        return orcTypes;
    }
}
