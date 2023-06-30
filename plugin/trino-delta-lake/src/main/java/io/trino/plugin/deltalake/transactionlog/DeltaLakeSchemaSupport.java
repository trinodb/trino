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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
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

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class DeltaLakeSchemaSupport
{
    private DeltaLakeSchemaSupport() {}

    public static final String APPEND_ONLY_CONFIGURATION_KEY = "delta.appendOnly";
    public static final String COLUMN_MAPPING_MODE_CONFIGURATION_KEY = "delta.columnMapping.mode";
    public static final String MAX_COLUMN_ID_CONFIGURATION_KEY = "delta.columnMapping.maxColumnId";

    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features
    // TODO: Add support for 'deletionVectors' and 'timestampNTZ' reader features
    private static final Set<String> SUPPORTED_READER_FEATURES = ImmutableSet.<String>builder()
            .add("columnMapping")
            .build();

    public enum ColumnMappingMode
    {
        ID,
        NAME,
        NONE,
        UNKNOWN,
        /**/;
    }

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

    public static boolean isAppendOnly(MetadataEntry metadataEntry)
    {
        return parseBoolean(metadataEntry.getConfiguration().getOrDefault(APPEND_ONLY_CONFIGURATION_KEY, "false"));
    }

    public static ColumnMappingMode getColumnMappingMode(MetadataEntry metadata)
    {
        String columnMappingMode = metadata.getConfiguration().getOrDefault(COLUMN_MAPPING_MODE_CONFIGURATION_KEY, "none");
        return Enums.getIfPresent(ColumnMappingMode.class, columnMappingMode.toUpperCase(ENGLISH)).or(ColumnMappingMode.UNKNOWN);
    }

    public static int getMaxColumnId(MetadataEntry metadata)
    {
        String maxColumnId = metadata.getConfiguration().get(MAX_COLUMN_ID_CONFIGURATION_KEY);
        requireNonNull(maxColumnId, () -> MAX_COLUMN_ID_CONFIGURATION_KEY + " metadata configuration property not found");
        return Integer.parseInt(maxColumnId);
    }

    public static List<DeltaLakeColumnHandle> extractPartitionColumns(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        return extractPartitionColumns(extractSchema(metadataEntry, typeManager), metadataEntry.getCanonicalPartitionColumns());
    }

    public static List<DeltaLakeColumnHandle> extractPartitionColumns(List<DeltaLakeColumnMetadata> schema, List<String> canonicalPartitionColumns)
    {
        if (canonicalPartitionColumns.isEmpty()) {
            return ImmutableList.of();
        }
        return schema.stream()
                .filter(entry -> canonicalPartitionColumns.contains(entry.getName()))
                .map(entry -> new DeltaLakeColumnHandle(entry.getName(), entry.getType(), OptionalInt.empty(), entry.getPhysicalName(), entry.getPhysicalColumnType(), PARTITION_KEY, Optional.empty()))
                .collect(toImmutableList());
    }

    public static String serializeSchemaAsJson(
            List<String> columnNames,
            Map<String, Object> columnTypes,
            Map<String, String> columnComments,
            Map<String, Boolean> columnNullability,
            Map<String, Map<String, Object>> columnMetadata)
    {
        try {
            return OBJECT_MAPPER.writeValueAsString(serializeStructType(columnNames, columnTypes, columnComments, columnNullability, columnMetadata));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to encode Delta Lake schema", e);
        }
    }

    private static Map<String, Object> serializeStructType(
            List<String> columnNames,
            Map<String, Object> columnTypes,
            Map<String, String> columnComments,
            Map<String, Boolean> columnNullability,
            Map<String, Map<String, Object>> columnMetadata)
    {
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-type
        ImmutableMap.Builder<String, Object> schema = ImmutableMap.builder();

        schema.put("type", "struct");
        schema.put("fields", columnNames.stream()
                .map(columnName -> serializeStructField(
                        columnName,
                        columnTypes.get(columnName),
                        columnComments.get(columnName),
                        columnNullability.get(columnName),
                        columnMetadata.get(columnName)))
                .collect(toImmutableList()));

        return schema.buildOrThrow();
    }

    private static Map<String, Object> serializeStructField(String name, Object type, @Nullable String comment, @Nullable Boolean nullable, @Nullable Map<String, Object> metadata)
    {
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field
        ImmutableMap.Builder<String, Object> fieldContents = ImmutableMap.builder();

        fieldContents.put("name", name);
        fieldContents.put("type", type);
        fieldContents.put("nullable", nullable != null ? nullable : true);

        ImmutableMap.Builder<String, Object> columnMetadata = ImmutableMap.builder();
        if (comment != null) {
            columnMetadata.put("comment", comment);
        }
        if (metadata != null) {
            metadata.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals("comment"))
                    .forEach(entry -> columnMetadata.put(entry.getKey(), entry.getValue()));
        }
        fieldContents.put("metadata", columnMetadata.buildOrThrow());

        return fieldContents.buildOrThrow();
    }

    public static Object serializeColumnType(ColumnMappingMode columnMappingMode, AtomicInteger maxColumnId, Type columnType)
    {
        if (columnType instanceof ArrayType) {
            return serializeArrayType(columnMappingMode, maxColumnId, (ArrayType) columnType);
        }
        if (columnType instanceof RowType) {
            return serializeStructType(columnMappingMode, maxColumnId, (RowType) columnType);
        }
        if (columnType instanceof MapType) {
            return serializeMapType(columnMappingMode, maxColumnId, (MapType) columnType);
        }
        return serializePrimitiveType(columnType);
    }

    private static Map<String, Object> serializeArrayType(ColumnMappingMode columnMappingMode, AtomicInteger maxColumnId, ArrayType arrayType)
    {
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#array-type
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("type", "array");
        fields.put("elementType", serializeColumnType(columnMappingMode, maxColumnId, arrayType.getElementType()));
        fields.put("containsNull", true);

        return fields.buildOrThrow();
    }

    private static Map<String, Object> serializeMapType(ColumnMappingMode columnMappingMode, AtomicInteger maxColumnId, MapType mapType)
    {
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#map-type
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("type", "map");
        fields.put("keyType", serializeColumnType(columnMappingMode, maxColumnId, mapType.getKeyType()));
        fields.put("valueType", serializeColumnType(columnMappingMode, maxColumnId, mapType.getValueType()));
        fields.put("valueContainsNull", true);

        return fields.buildOrThrow();
    }

    private static Map<String, Object> serializeStructType(ColumnMappingMode columnMappingMode, AtomicInteger maxColumnId, RowType rowType)
    {
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("type", "struct");
        fields.put("fields", rowType.getFields().stream()
                .map(field -> {
                    Object fieldType = serializeColumnType(columnMappingMode, maxColumnId, field.getType());
                    Map<String, Object> metadata = generateColumnMetadata(columnMappingMode, maxColumnId);
                    return serializeStructField(field.getName().orElse(null), fieldType, null, null, metadata);
                })
                .collect(toImmutableList()));

        return fields.buildOrThrow();
    }

    public static Map<String, Object> generateColumnMetadata(ColumnMappingMode columnMappingMode, AtomicInteger maxColumnId)
    {
        return switch (columnMappingMode) {
            case NONE -> {
                verify(maxColumnId.get() == 0, "maxColumnId must be 0 for column mapping mode 'none'");
                yield ImmutableMap.of();
            }
            case ID, NAME -> ImmutableMap.<String, Object>builder()
                    // Set both 'id' and 'physicalName' regardless of the mode https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping
                    // > There are two modes of column mapping, by name and by id.
                    // > In both modes, every column - nested or leaf - is assigned a unique physical name, and a unique 32-bit integer as an id.
                    .put("delta.columnMapping.id", maxColumnId.incrementAndGet())
                    .put("delta.columnMapping.physicalName", "col-" + UUID.randomUUID()) // This logic is same as DeltaColumnMapping.generatePhysicalName in Delta Lake
                    .buildOrThrow();
            default -> throw new IllegalArgumentException("Unexpected column mapping mode: " + columnMappingMode);
        };
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
        if (type instanceof DecimalType decimalType) {
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

        if (type instanceof MapType mapType) {
            validateType(rootType, mapType.getKeyType());
            validateType(rootType, mapType.getValueType());
        }

        if (type instanceof RowType rowType) {
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

    public static List<ColumnMetadata> extractColumnMetadata(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        return extractSchema(metadataEntry, typeManager).stream()
                .map(DeltaLakeColumnMetadata::getColumnMetadata)
                .collect(toImmutableList());
    }

    public static List<DeltaLakeColumnMetadata> extractSchema(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        ColumnMappingMode mappingMode = getColumnMappingMode(metadataEntry);
        verifySupportedColumnMapping(mappingMode);
        return Optional.ofNullable(metadataEntry.getSchemaString())
                .map(json -> getColumnMetadata(json, typeManager, mappingMode))
                .orElseThrow(() -> new IllegalStateException("Serialized schema not found in transaction log for " + metadataEntry.getName()));
    }

    public static void verifySupportedColumnMapping(ColumnMappingMode mappingMode)
    {
        if (mappingMode != ColumnMappingMode.ID && mappingMode != ColumnMappingMode.NAME && mappingMode != ColumnMappingMode.NONE) {
            throw new TrinoException(NOT_SUPPORTED, format("Only 'id', 'name' or 'none' is supported for the '%s' table property", COLUMN_MAPPING_MODE_CONFIGURATION_KEY));
        }
    }

    @VisibleForTesting
    static List<DeltaLakeColumnMetadata> getColumnMetadata(String json, TypeManager typeManager, ColumnMappingMode mappingMode)
    {
        try {
            return stream(OBJECT_MAPPER.readTree(json).get("fields").elements())
                    .map(node -> mapColumn(typeManager, node, mappingMode))
                    .collect(toImmutableList());
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse serialized schema: " + json, e);
        }
    }

    private static DeltaLakeColumnMetadata mapColumn(TypeManager typeManager, JsonNode node, ColumnMappingMode mappingMode)
    {
        String fieldName = node.get("name").asText();
        JsonNode typeNode = node.get("type");
        boolean nullable = node.get("nullable").asBoolean();
        Type columnType = buildType(typeManager, typeNode, false);
        OptionalInt fieldId = OptionalInt.empty();
        String physicalName;
        Type physicalColumnType;
        switch (mappingMode) {
            case ID:
                String columnMappingId = node.get("metadata").get("delta.columnMapping.id").asText();
                verify(!isNullOrEmpty(columnMappingId), "id is null or empty");
                fieldId = OptionalInt.of(Integer.parseInt(columnMappingId));
                // Databricks stores column statistics with physical name
                physicalName = node.get("metadata").get("delta.columnMapping.physicalName").asText();
                verify(!isNullOrEmpty(physicalName), "physicalName is null or empty");
                physicalColumnType = buildType(typeManager, typeNode, true);
                break;
            case NAME:
                physicalName = node.get("metadata").get("delta.columnMapping.physicalName").asText();
                verify(!isNullOrEmpty(physicalName), "physicalName is null or empty");
                physicalColumnType = buildType(typeManager, typeNode, true);
                break;
            default:
                physicalName = fieldName;
                physicalColumnType = columnType;
        }
        ColumnMetadata columnMetadata = ColumnMetadata.builder()
                .setName(fieldName)
                .setType(columnType)
                .setNullable(nullable)
                .setComment(Optional.ofNullable(getComment(node)))
                .build();
        return new DeltaLakeColumnMetadata(columnMetadata, fieldId, physicalName, physicalColumnType);
    }

    public static Map<String, Object> getColumnTypes(MetadataEntry metadataEntry)
    {
        return getColumnProperties(metadataEntry, node -> OBJECT_MAPPER.convertValue(node.get("type"), new TypeReference<>(){}));
    }

    public static Map<String, String> getColumnComments(MetadataEntry metadataEntry)
    {
        return getColumnProperties(metadataEntry, DeltaLakeSchemaSupport::getComment);
    }

    @Nullable
    private static String getComment(JsonNode node)
    {
        JsonNode comment = node.get("metadata").get("comment");
        return comment == null ? null : comment.asText();
    }

    public static Map<String, Boolean> getColumnsNullability(MetadataEntry metadataEntry)
    {
        return getColumnProperties(metadataEntry, node -> node.get("nullable").asBoolean());
    }

    public static Map<String, String> getColumnInvariants(MetadataEntry metadataEntry)
    {
        return getColumnProperties(metadataEntry, DeltaLakeSchemaSupport::getInvariants);
    }

    @Nullable
    private static String getInvariants(JsonNode node)
    {
        JsonNode invariants = node.get("metadata").get("delta.invariants");
        return invariants == null ? null : extractInvariantsExpression(invariants.asText());
    }

    private static String extractInvariantsExpression(String invariants)
    {
        try {
            return OBJECT_MAPPER.readTree(invariants).get("expression").get("expression").asText();
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse invariants expression: " + invariants, e);
        }
    }

    public static Map<String, String> getGeneratedColumnExpressions(MetadataEntry metadataEntry)
    {
        return getColumnProperties(metadataEntry, DeltaLakeSchemaSupport::getGeneratedColumnExpressions);
    }

    @Nullable
    private static String getGeneratedColumnExpressions(JsonNode node)
    {
        JsonNode generationExpression = node.get("metadata").get("delta.generationExpression");
        return generationExpression == null ? null : generationExpression.asText();
    }

    public static Map<String, String> getCheckConstraints(MetadataEntry metadataEntry)
    {
        return metadataEntry.getConfiguration().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("delta.constraints."))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static boolean changeDataFeedEnabled(MetadataEntry metadataEntry)
    {
        String enableChangeDataFeed = metadataEntry.getConfiguration().getOrDefault("delta.enableChangeDataFeed", "false");
        return parseBoolean(enableChangeDataFeed);
    }

    public static Map<String, Map<String, Object>> getColumnsMetadata(MetadataEntry metadataEntry)
    {
        return getColumnProperties(metadataEntry, node -> OBJECT_MAPPER.convertValue(node.get("metadata"), new TypeReference<>(){}));
    }

    public static <T> Map<String, T> getColumnProperties(MetadataEntry metadataEntry, Function<JsonNode, T> extractor)
    {
        return Optional.ofNullable(metadataEntry.getSchemaString())
                .map(json -> getColumnProperty(json, extractor))
                .orElseThrow(() -> new IllegalStateException("Serialized schema not found in transaction log for " + metadataEntry.getName()));
    }

    private static <T> Map<String, T> getColumnProperty(String json, Function<JsonNode, T> extractor)
    {
        try {
            return stream(OBJECT_MAPPER.readTree(json).get("fields").elements())
                    .map(field -> new AbstractMap.SimpleEntry<>(field.get("name").asText(), extractor.apply(field)))
                    .filter(entry -> entry.getValue() != null)
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse serialized schema: " + json, e);
        }
    }

    public static Set<String> unsupportedReaderFeatures(Set<String> features)
    {
        return Sets.difference(features, SUPPORTED_READER_FEATURES);
    }

    private static Type buildType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
    {
        if (typeNode.isContainerNode()) {
            return buildContainerType(typeManager, typeNode, usePhysicalName);
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

    private static Type buildContainerType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
    {
        String containerType = typeNode.get("type").asText();
        switch (containerType) {
            case "array":
                return buildArrayType(typeManager, typeNode, usePhysicalName);
            case "map":
                return buildMapType(typeManager, typeNode, usePhysicalName);
            case "struct":
                return buildRowType(typeManager, typeNode, usePhysicalName);
            default:
                throw new TypeNotFoundException(new TypeSignature(containerType));
        }
    }

    private static RowType buildRowType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
    {
        return (RowType) typeManager.getType(TypeSignature.rowType(stream(typeNode.get("fields").elements())
                .map(element -> {
                    String fieldName = usePhysicalName ? element.get("metadata").get("delta.columnMapping.physicalName").asText() : element.get("name").asText();
                    verify(!isNullOrEmpty(fieldName), "fieldName is null or empty");
                    return TypeSignatureParameter.namedField(
                            // We lower case the struct field names.
                            // Otherwise, Trino will refuse to write to columns whose struct type has field names containing upper case characters.
                            // Users can't work around this by casting in their queries because Trino parser always lower case types.
                            // TODO: This is a hack. Engine should be able to handle identifiers in a case insensitive way where necessary.
                            // See also HiveTypeTranslator#toTypeSingature.
                            TransactionLogAccess.canonicalizeColumnName(fieldName),
                            buildType(typeManager, element.get("type"), usePhysicalName).getTypeSignature());
                })
                .collect(toImmutableList())));
    }

    private static ArrayType buildArrayType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
    {
        return (ArrayType) typeManager.getType(TypeSignature.arrayType(buildType(typeManager, typeNode.get("elementType"), usePhysicalName).getTypeSignature()));
    }

    private static MapType buildMapType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
    {
        return (MapType) typeManager.getType(TypeSignature.mapType(
                buildType(typeManager, typeNode.get("keyType"), usePhysicalName).getTypeSignature(),
                buildType(typeManager, typeNode.get("valueType"), usePhysicalName).getTypeSignature()));
    }

    private static Optional<Location> getLocation(JsonProcessingException e)
    {
        return Optional.ofNullable(e.getLocation()).map(location -> new Location(location.getLineNr(), location.getColumnNr()));
    }
}
