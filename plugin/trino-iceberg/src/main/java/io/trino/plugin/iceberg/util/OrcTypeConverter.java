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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class OrcTypeConverter
{
    public static final String ORC_ICEBERG_ID_KEY = "iceberg.id";
    public static final String ORC_ICEBERG_REQUIRED_KEY = "iceberg.required";
    public static final String ICEBERG_LONG_TYPE = "iceberg.long-type";
    public static final String ICEBERG_BINARY_TYPE = "iceberg.binary-type";

    private OrcTypeConverter() {}

    public static ColumnMetadata<OrcType> toOrcType(Schema schema)
    {
        return new ColumnMetadata<>(toOrcStructType(0, schema.asStruct(), ImmutableMap.of()));
    }

    private static List<OrcType> toOrcType(int nextFieldTypeIndex, Type type, Map<String, String> attributes)
    {
        return switch (type.typeId()) {
            case BOOLEAN -> ImmutableList.of(new OrcType(OrcTypeKind.BOOLEAN, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case INTEGER -> ImmutableList.of(new OrcType(OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case LONG -> ImmutableList.of(new OrcType(OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case FLOAT -> ImmutableList.of(new OrcType(OrcTypeKind.FLOAT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DOUBLE -> ImmutableList.of(new OrcType(OrcTypeKind.DOUBLE, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DATE -> ImmutableList.of(new OrcType(OrcTypeKind.DATE, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case TIME -> {
                attributes = ImmutableMap.<String, String>builder()
                        .putAll(attributes)
                        .put(ICEBERG_LONG_TYPE, "TIME")
                        .buildOrThrow();
                yield ImmutableList.of(new OrcType(OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            }
            case TIMESTAMP -> {
                OrcTypeKind timestampKind = ((TimestampType) type).shouldAdjustToUTC() ? OrcTypeKind.TIMESTAMP_INSTANT : OrcTypeKind.TIMESTAMP;
                yield ImmutableList.of(new OrcType(timestampKind, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            }
            case STRING -> ImmutableList.of(new OrcType(OrcTypeKind.STRING, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case FIXED, BINARY -> ImmutableList.of(new OrcType(OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DECIMAL -> {
                DecimalType decimalType = (DecimalType) type;
                yield ImmutableList.of(new OrcType(OrcTypeKind.DECIMAL, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.of(decimalType.precision()), Optional.of(decimalType.scale()), attributes));
            }
            case UUID -> {
                attributes = ImmutableMap.<String, String>builder()
                        .putAll(attributes)
                        .put(ICEBERG_BINARY_TYPE, "UUID")
                        .buildOrThrow();
                yield ImmutableList.of(new OrcType(OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            }
            case STRUCT -> toOrcStructType(nextFieldTypeIndex, (StructType) type, attributes);
            case LIST -> toOrcListType(nextFieldTypeIndex, (ListType) type, attributes);
            case MAP -> toOrcMapType(nextFieldTypeIndex, (MapType) type, attributes);
        };
    }

    private static List<OrcType> toOrcStructType(int nextFieldTypeIndex, StructType structType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;

        List<OrcColumnId> fieldTypeIndexes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<List<OrcType>> fieldTypesList = new ArrayList<>();
        for (NestedField field : structType.fields()) {
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

        return ImmutableList.<OrcType>builder()
                .add(new OrcType(
                        OrcTypeKind.STRUCT,
                        fieldTypeIndexes,
                        fieldNames,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        attributes))
                .addAll(fieldTypesList.stream().flatMap(List::stream).iterator())
                .build();
    }

    private static List<OrcType> toOrcListType(int nextFieldTypeIndex, ListType listType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;

        Map<String, String> elementAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(listType.elementId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(listType.isElementRequired()))
                .buildOrThrow();
        List<OrcType> itemTypes = toOrcType(nextFieldTypeIndex, listType.elementType(), elementAttributes);

        return ImmutableList.<OrcType>builder()
                .add(new OrcType(
                        OrcTypeKind.LIST,
                        ImmutableList.of(new OrcColumnId(nextFieldTypeIndex)),
                        ImmutableList.of("item"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        attributes))
                .addAll(itemTypes)
                .build();
    }

    private static List<OrcType> toOrcMapType(int nextFieldTypeIndex, MapType mapType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;

        List<OrcType> keyTypes = toOrcType(nextFieldTypeIndex, mapType.keyType(), ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(mapType.keyId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(true))
                .buildOrThrow());

        Map<String, String> valueAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(mapType.valueId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(mapType.isValueRequired()))
                .buildOrThrow();
        List<OrcType> valueTypes = toOrcType(nextFieldTypeIndex + keyTypes.size(), mapType.valueType(), valueAttributes);

        return ImmutableList.<OrcType>builder()
                .add(new OrcType(
                        OrcTypeKind.MAP,
                        ImmutableList.of(new OrcColumnId(nextFieldTypeIndex), new OrcColumnId(nextFieldTypeIndex + keyTypes.size())),
                        ImmutableList.of("key", "value"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        attributes))
                .addAll(keyTypes)
                .addAll(valueTypes)
                .build();
    }
}
