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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class DeltaLakeSchemaSupport
{
    private DeltaLakeSchemaSupport() {}

    // only non-parametrized types are stored here
    private static final Map<Type, String> PRIMITIVE_TYPE_MAPPING = ImmutableMap.<Type, String>builder()
            .put(BIGINT, "long")
            .put(INTEGER, "integer")
            .put(SMALLINT, "short")
            .put(TINYINT, "byte")
            .put(REAL, "float")
            .put(DOUBLE, "double")
            .put(BOOLEAN, "boolean")
            .put(VARBINARY, "binary")
            .put(DATE, "date")
            .buildOrThrow();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    public static List<DeltaLakeColumnHandle> extractPartitionColumns(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        return extractPartitionColumns(extractSchema(metadataEntry, typeManager), metadataEntry.getCanonicalPartitionColumns());
    }

    public static List<DeltaLakeColumnHandle> extractPartitionColumns(List<ColumnMetadata> schema, List<String> canonicalPartitionColumns)
    {
        if (canonicalPartitionColumns.isEmpty()) {
            return ImmutableList.of();
        }
        return schema.stream()
                .filter(entry -> canonicalPartitionColumns.contains(entry.getName()))
                .map(entry -> new DeltaLakeColumnHandle(entry.getName(), entry.getType(), PARTITION_KEY))
                .collect(toImmutableList());
    }

    public static String serializeSchemaAsJson(List<DeltaLakeColumnHandle> columns)
    {
        try {
            return OBJECT_MAPPER.writeValueAsString(serializeStructType(columns));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to encode Delta Lake schema", e);
        }
    }

    private static Map<String, Object> serializeStructType(List<DeltaLakeColumnHandle> columns)
    {
        ImmutableMap.Builder<String, Object> schema = ImmutableMap.builder();

        schema.put("fields", columns.stream().map(column -> serializeStructField(column.getName(), column.getType())).collect(toImmutableList()));
        schema.put("type", "struct");

        return schema.buildOrThrow();
    }

    private static Map<String, Object> serializeStructField(String name, Type type)
    {
        ImmutableMap.Builder<String, Object> fieldContents = ImmutableMap.builder();

        fieldContents.put("metadata", ImmutableMap.of());
        fieldContents.put("name", name);
        fieldContents.put("nullable", true); // TODO: Is column nullability configurable in Presto?
        fieldContents.put("type", serializeColumnType(type));

        return fieldContents.buildOrThrow();
    }

    private static Object serializeColumnType(Type columnType)
    {
        if (columnType instanceof ArrayType) {
            return serializeArrayType((ArrayType) columnType);
        }
        if (columnType instanceof RowType) {
            return serializeStructType((RowType) columnType);
        }
        if (columnType instanceof MapType) {
            return serializeMapType((MapType) columnType);
        }
        return serializePrimitiveType(columnType);
    }

    private static Map<String, Object> serializeArrayType(ArrayType arrayType)
    {
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("type", "array");
        fields.put("containsNull", true);
        fields.put("elementType", serializeColumnType(arrayType.getElementType()));

        return fields.buildOrThrow();
    }

    private static Map<String, Object> serializeMapType(MapType mapType)
    {
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("keyType", serializeColumnType(mapType.getKeyType()));
        fields.put("type", "map");
        fields.put("valueContainsNull", true);
        fields.put("valueType", serializeColumnType(mapType.getValueType()));

        return fields.buildOrThrow();
    }

    private static Map<String, Object> serializeStructType(RowType rowType)
    {
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("type", "struct");
        fields.put("fields", rowType.getFields().stream().map(field -> serializeStructField(field.getName().orElse(null), field.getType())).collect(toImmutableList()));

        return fields.buildOrThrow();
    }

    private static String serializePrimitiveType(Type type)
    {
        return serializeSupportedPrimitiveType(type)
                .orElseThrow(() -> new TypeNotFoundException(type.getTypeSignature()));
    }

    private static Optional<String> serializeSupportedPrimitiveType(Type type)
    {
        if (type instanceof TimestampWithTimeZoneType) {
            return Optional.of("timestamp");
        }
        if (type instanceof VarcharType) {
            return Optional.of("string");
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return Optional.of(String.format("decimal(%s,%s)", decimalType.getPrecision(), decimalType.getScale()));
        }
        return Optional.ofNullable(PRIMITIVE_TYPE_MAPPING.get(type));
    }

    public static void validateType(Type type)
    {
        validateType(Optional.empty(), type);
    }

    private static void validateType(Optional<Type> rootType, Type type)
    {
        rootType.ifPresent(root -> {
            if (type instanceof TimestampWithTimeZoneType) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Nested TIMESTAMP types are not supported, invalid type: " + root);
            }
        });

        if (HiveUtil.isStructuralType(type)) {
            validateStructuralType(Optional.of(rootType.orElse(type)), type);
        }
        else {
            validatePrimitiveType(type);
        }
    }

    private static void validateStructuralType(Optional<Type> rootType, Type type)
    {
        if (type instanceof ArrayType) {
            validateType(rootType, ((ArrayType) type).getElementType());
        }

        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            validateType(rootType, mapType.getKeyType());
            validateType(rootType, mapType.getValueType());
        }

        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            rowType.getFields().forEach(field -> validateType(rootType, field.getType()));
        }
    }

    private static void validatePrimitiveType(Type type)
    {
        if (serializeSupportedPrimitiveType(type).isEmpty() ||
                (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).getPrecision() != 3)) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Unsupported type: " + type);
        }
    }

    public static String serializeStatsAsJson(DeltaLakeFileStatistics fileStatistics)
            throws JsonProcessingException
    {
        return OBJECT_MAPPER.writeValueAsString(fileStatistics);
    }

    public static List<ColumnMetadata> extractSchema(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        return Optional.ofNullable(metadataEntry.getSchemaString())
                .map(json -> getColumnMetadata(json, typeManager))
                .orElseThrow(() -> new IllegalStateException("Serialized schema not found in transaction log for " + metadataEntry.getName()));
    }

    @VisibleForTesting
    static List<ColumnMetadata> getColumnMetadata(String json, TypeManager typeManager)
    {
        try {
            return stream(OBJECT_MAPPER.readTree(json).get("fields").elements())
                    .map(node -> mapColumn(typeManager, node))
                    .collect(toImmutableList());
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse serialized schema: " + json, e);
        }
    }

    private static ColumnMetadata mapColumn(TypeManager typeManager, JsonNode node)
    {
        String fieldName = node.get("name").asText();
        JsonNode typeNode = node.get("type");
        boolean nullable = node.get("nullable").asBoolean();
        return ColumnMetadata.builder()
                .setName(fieldName)
                .setType(buildType(typeManager, typeNode))
                .setNullable(nullable)
                .build();
    }

    private static Type buildType(TypeManager typeManager, JsonNode typeNode)
    {
        if (typeNode.isContainerNode()) {
            return buildContainerType(typeManager, typeNode);
        }
        String primitiveType = typeNode.asText();
        if (primitiveType.startsWith(StandardTypes.DECIMAL)) {
            return typeManager.fromSqlType(primitiveType);
        }
        switch (primitiveType) {
            case "string":
                return VARCHAR;
            case "long":
                return BIGINT;
            case "integer":
                return INTEGER;
            case "short":
                return SMALLINT;
            case "byte":
                return TINYINT;
            case "float":
                return REAL;
            case "double":
                return DOUBLE;
            case "boolean":
                return BOOLEAN;
            case "binary":
                return VARBINARY;
            case "date":
                return DATE;
            case "timestamp":
                // Spark/DeltaLake stores timestamps in UTC, but renders them in session time zone.
                // For more info, see https://delta-users.slack.com/archives/GKTUWT03T/p1585760533005400
                // and https://cwiki.apache.org/confluence/display/Hive/Different+TIMESTAMP+types
                return createTimestampWithTimeZoneType(3);
            default:
                throw new TypeNotFoundException(new TypeSignature(primitiveType));
        }
    }

    private static Type buildContainerType(TypeManager typeManager, JsonNode typeNode)
    {
        String containerType = typeNode.get("type").asText();
        switch (containerType) {
            case "array":
                return buildArrayType(typeManager, typeNode);
            case "map":
                return buildMapType(typeManager, typeNode);
            case "struct":
                return buildRowType(typeManager, typeNode);
            default:
                throw new TypeNotFoundException(new TypeSignature(containerType));
        }
    }

    private static RowType buildRowType(TypeManager typeManager, JsonNode typeNode)
    {
        return (RowType) typeManager.getType(TypeSignature.rowType(stream(typeNode.get("fields").elements())
                .map(element -> TypeSignatureParameter.namedField(
                        // We lower case the struct field names.
                        // Otherwise, Trino will refuse to write to columns whose struct type has field names containing upper case characters.
                        // Users can't work around this by casting in their queries because Trino parser always lower case types.
                        // TODO: This is a hack. Engine should be able to handle identifiers in a case insensitive way where necessary.
                        // See also HiveTypeTranslator#toTypeSingature.
                        TransactionLogAccess.canonicalizeColumnName(element.get("name").asText()),
                        buildType(typeManager, element.get("type")).getTypeSignature()))
                .collect(toImmutableList())));
    }

    private static ArrayType buildArrayType(TypeManager typeManager, JsonNode typeNode)
    {
        return (ArrayType) typeManager.getType(TypeSignature.arrayType(buildType(typeManager, typeNode.get("elementType")).getTypeSignature()));
    }

    private static MapType buildMapType(TypeManager typeManager, JsonNode typeNode)
    {
        return (MapType) typeManager.getType(TypeSignature.mapType(
                buildType(typeManager, typeNode.get("keyType")).getTypeSignature(),
                buildType(typeManager, typeNode.get("valueType")).getTypeSignature()));
    }

    private static Optional<Location> getLocation(JsonProcessingException e)
    {
        return Optional.ofNullable(e.getLocation()).map(location -> new Location(location.getLineNr(), location.getColumnNr()));
    }
}
