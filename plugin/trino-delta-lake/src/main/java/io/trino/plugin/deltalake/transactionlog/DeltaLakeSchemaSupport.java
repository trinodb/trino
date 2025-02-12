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
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeTable;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.APPEND_ONLY_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.CHANGE_DATA_FEED_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.CHECK_CONSTRAINTS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.COLUMN_MAPPING_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.DELETION_VECTORS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.ICEBERG_COMPATIBILITY_V1_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.ICEBERG_COMPATIBILITY_V2_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.IDENTITY_COLUMNS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.INVARIANTS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
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

    private static final Logger log = Logger.get(DeltaLakeSchemaSupport.class);

    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings().trimResults();

    public static final String APPEND_ONLY_CONFIGURATION_KEY = "delta.appendOnly";
    public static final String COLUMN_MAPPING_MODE_CONFIGURATION_KEY = "delta.columnMapping.mode";
    public static final String COLUMN_MAPPING_PHYSICAL_NAME_CONFIGURATION_KEY = "delta.columnMapping.physicalName";
    public static final String MAX_COLUMN_ID_CONFIGURATION_KEY = "delta.columnMapping.maxColumnId";
    public static final String ISOLATION_LEVEL_CONFIGURATION_KEY = "delta.isolationLevel";
    public static final String DELETION_VECTORS_CONFIGURATION_KEY = "delta.enableDeletionVectors";
    // https://github.com/delta-io/delta/blob/master/docs/source/delta-uniform.md
    private static final String UNIVERSAL_FORMAT_CONFIGURATION_KEY = "delta.universalFormat.enabledFormats";

    public enum ColumnMappingMode
    {
        ID,
        NAME,
        NONE,
        UNKNOWN,
        /**/;
    }

    public enum IsolationLevel
    {
        WRITESERIALIZABLE("WriteSerializable"),
        SERIALIZABLE("Serializable");

        private final String value;

        IsolationLevel(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
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

    public static boolean isAppendOnly(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsWriterFeatures() && !protocolEntry.writerFeaturesContains(APPEND_ONLY_FEATURE_NAME)) {
            return false;
        }
        return parseBoolean(metadataEntry.getConfiguration().getOrDefault(APPEND_ONLY_CONFIGURATION_KEY, "false"));
    }

    public static boolean isDeletionVectorEnabled(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsWriterFeatures() && !protocolEntry.writerFeaturesContains(DELETION_VECTORS_FEATURE_NAME)) {
            return false;
        }
        return parseBoolean(metadataEntry.getConfiguration().get(DELETION_VECTORS_CONFIGURATION_KEY));
    }

    public static int getRandomPrefixLength(MetadataEntry metadataEntry)
    {
        boolean randomizeFilePrefixes = parseBoolean(metadataEntry.getConfiguration().get("delta.randomizeFilePrefixes"));
        if (randomizeFilePrefixes) {
            // 2 is the default value in Delta Lake
            int randomPrefixLength = Integer.parseInt(metadataEntry.getConfiguration().getOrDefault("delta.randomPrefixLength", "2"));
            checkArgument(randomPrefixLength >= 0, "randomPrefixLength must be >= 0: %s", randomPrefixLength);
            return randomPrefixLength;
        }
        return 0;
    }

    public static List<String> enabledUniversalFormats(MetadataEntry metadataEntry)
    {
        String formats = metadataEntry.getConfiguration().get(UNIVERSAL_FORMAT_CONFIGURATION_KEY);
        return formats == null ? ImmutableList.of() : SPLITTER.splitToList(formats);
    }

    public static ColumnMappingMode getColumnMappingMode(MetadataEntry metadata, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsReaderFeatures() || protocolEntry.supportsWriterFeatures()) {
            if (protocolEntry.writerFeaturesContains(ICEBERG_COMPATIBILITY_V1_FEATURE_NAME) || protocolEntry.writerFeaturesContains(ICEBERG_COMPATIBILITY_V2_FEATURE_NAME)) {
                String columnMappingMode = metadata.getConfiguration().get(COLUMN_MAPPING_MODE_CONFIGURATION_KEY);
                checkArgument(columnMappingMode != null && columnMappingMode.equals("name"), "Column mapping mode must be 'name' for Iceberg compatibility: %s", columnMappingMode);
                return ColumnMappingMode.NAME;
            }

            if (protocolEntry.supportsReaderFeatures() && protocolEntry.supportsWriterFeatures()) {
                boolean supportsColumnMappingReader = protocolEntry.readerFeaturesContains(COLUMN_MAPPING_FEATURE_NAME);
                boolean supportsColumnMappingWriter = protocolEntry.writerFeaturesContains(COLUMN_MAPPING_FEATURE_NAME);
                checkArgument(
                        supportsColumnMappingReader == supportsColumnMappingWriter,
                        "Both reader and writer features must have the same value for 'columnMapping'. reader: %s, writer: %s", supportsColumnMappingReader, supportsColumnMappingWriter);
                if (!supportsColumnMappingReader) {
                    return ColumnMappingMode.NONE;
                }
            }
        }
        String columnMappingMode = metadata.getConfiguration().getOrDefault(COLUMN_MAPPING_MODE_CONFIGURATION_KEY, "none");
        return Enums.getIfPresent(ColumnMappingMode.class, columnMappingMode.toUpperCase(ENGLISH)).or(ColumnMappingMode.UNKNOWN);
    }

    public static IsolationLevel getIsolationLevel(MetadataEntry metadata)
    {
        // WriteSerializable isolation level provides the best balance between data consistency and availability
        String isolationLevel = metadata.getConfiguration().getOrDefault(ISOLATION_LEVEL_CONFIGURATION_KEY, IsolationLevel.WRITESERIALIZABLE.getValue());
        return Enums.getIfPresent(IsolationLevel.class, isolationLevel.toUpperCase(ENGLISH)).toJavaUtil().orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported isolation level '%s'".formatted(isolationLevel)));
    }

    public static int getMaxColumnId(MetadataEntry metadata)
    {
        String maxColumnId = metadata.getConfiguration().get(MAX_COLUMN_ID_CONFIGURATION_KEY);
        requireNonNull(maxColumnId, MAX_COLUMN_ID_CONFIGURATION_KEY + " metadata configuration property not found");
        return Integer.parseInt(maxColumnId);
    }

    public static List<DeltaLakeColumnHandle> extractPartitionColumns(MetadataEntry metadataEntry, ProtocolEntry protocolEntry, TypeManager typeManager)
    {
        return extractPartitionColumns(extractSchema(metadataEntry, protocolEntry, typeManager), metadataEntry.getOriginalPartitionColumns());
    }

    public static List<DeltaLakeColumnHandle> extractPartitionColumns(List<DeltaLakeColumnMetadata> schema, List<String> originalPartitionColumns)
    {
        if (originalPartitionColumns.isEmpty()) {
            return ImmutableList.of();
        }
        return schema.stream()
                .filter(entry -> originalPartitionColumns.contains(entry.name()))
                .map(entry -> new DeltaLakeColumnHandle(entry.name(), entry.type(), OptionalInt.empty(), entry.physicalName(), entry.physicalColumnType(), PARTITION_KEY, Optional.empty()))
                .collect(toImmutableList());
    }

    public static String serializeSchemaAsJson(DeltaLakeTable deltaTable)
    {
        try {
            return OBJECT_MAPPER.writeValueAsString(serializeStructType(deltaTable));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to encode Delta Lake schema", e);
        }
    }

    private static Map<String, Object> serializeStructType(DeltaLakeTable deltaTable)
    {
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-type
        ImmutableMap.Builder<String, Object> schema = ImmutableMap.builder();

        schema.put("type", "struct");
        schema.put("fields", deltaTable.columns().stream()
                .map(column -> serializeStructField(
                        column.name(),
                        column.type(),
                        column.comment(),
                        column.nullable(),
                        column.metadata()))
                .collect(toImmutableList()));

        return schema.buildOrThrow();
    }

    private static Map<String, Object> serializeStructField(String name, Object type, @Nullable String comment, boolean nullable, @Nullable Map<String, Object> metadata)
    {
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field
        ImmutableMap.Builder<String, Object> fieldContents = ImmutableMap.builder();

        fieldContents.put("name", name);
        fieldContents.put("type", type);
        fieldContents.put("nullable", nullable);

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
        Set<String> fieldNames = new HashSet<>();
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();

        fields.put("type", "struct");
        fields.put("fields", rowType.getFields().stream()
                .map(field -> {
                    String name = field.getName().orElseThrow(() ->
                            new TrinoException(NOT_SUPPORTED, "Row type field does not have a name: " + rowType.getDisplayName()));
                    if (!fieldNames.add(name.toLowerCase(ENGLISH))) {
                        throw new TrinoException(DUPLICATE_COLUMN_NAME, "Field name '%s' specified more than once".formatted(name.toLowerCase(ENGLISH)));
                    }
                    Object fieldType = serializeColumnType(columnMappingMode, maxColumnId, field.getType());
                    Map<String, Object> metadata = generateColumnMetadata(columnMappingMode, maxColumnId);
                    return serializeStructField(name, fieldType, null, true, metadata);
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
        if (type instanceof TimestampType) {
            return Optional.of("timestamp_ntz");
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return Optional.of("timestamp");
        }
        if (type instanceof VarcharType) {
            return Optional.of("string");
        }
        if (type instanceof DecimalType decimalType) {
            return Optional.of(format("decimal(%s,%s)", decimalType.getPrecision(), decimalType.getScale()));
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
                (type instanceof TimestampType && ((TimestampType) type).getPrecision() != 6) ||
                (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).getPrecision() != 3)) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Unsupported type: " + type);
        }
    }

    public static String serializeStatsAsJson(DeltaLakeFileStatistics fileStatistics)
            throws JsonProcessingException
    {
        return OBJECT_MAPPER.writeValueAsString(fileStatistics);
    }

    public static List<ColumnMetadata> extractColumnMetadata(MetadataEntry metadataEntry, ProtocolEntry protocolEntry, TypeManager typeManager)
    {
        return extractSchema(metadataEntry, protocolEntry, typeManager).stream()
                .map(DeltaLakeColumnMetadata::columnMetadata)
                .collect(toImmutableList());
    }

    public static List<DeltaLakeColumnMetadata> extractSchema(MetadataEntry metadataEntry, ProtocolEntry protocolEntry, TypeManager typeManager)
    {
        ColumnMappingMode mappingMode = getColumnMappingMode(metadataEntry, protocolEntry);
        verifySupportedColumnMapping(mappingMode);
        return Optional.ofNullable(metadataEntry.getSchemaString())
                .map(json -> getColumnMetadata(json, typeManager, mappingMode, metadataEntry.getOriginalPartitionColumns()))
                .orElseThrow(() -> new IllegalStateException("Serialized schema not found in transaction log for " + metadataEntry.getName()));
    }

    public static void verifySupportedColumnMapping(ColumnMappingMode mappingMode)
    {
        if (mappingMode != ColumnMappingMode.ID && mappingMode != ColumnMappingMode.NAME && mappingMode != ColumnMappingMode.NONE) {
            throw new TrinoException(NOT_SUPPORTED, format("Only 'id', 'name' or 'none' is supported for the '%s' table property", COLUMN_MAPPING_MODE_CONFIGURATION_KEY));
        }
    }

    public static List<DeltaLakeColumnMetadata> getColumnMetadata(String json, TypeManager typeManager, ColumnMappingMode mappingMode, List<String> partitionColumns)
    {
        try {
            ImmutableList.Builder<DeltaLakeColumnMetadata> columns = ImmutableList.builder();
            Iterator<JsonNode> nodes = OBJECT_MAPPER.readTree(json).get("fields").elements();
            while (nodes.hasNext()) {
                try {
                    columns.add(mapColumn(typeManager, nodes.next(), mappingMode, partitionColumns));
                }
                catch (UnsupportedTypeException e) {
                    // Write operations are denied by unsupported 'variantType' writer feature,
                    // as well as reads of unsupported type-widened columns are skipped
                    log.debug("Skip unsupported column type: %s", e.getMessage());
                }
            }
            return columns.build();
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse serialized schema: " + json, e);
        }
    }

    private static DeltaLakeColumnMetadata mapColumn(TypeManager typeManager, JsonNode node, ColumnMappingMode mappingMode, List<String> partitionColumns)
            throws UnsupportedTypeException
    {
        String fieldName = node.get("name").asText();
        JsonNode typeNode = node.get("type");
        boolean nullable = node.get("nullable").asBoolean();
        Type columnType = buildType(typeManager, typeNode, false);
        OptionalInt fieldId = OptionalInt.empty();
        String physicalName;
        Type physicalColumnType;
        JsonNode metadata = node.get("metadata");
        verifyTypeChanges(metadata, typeNode, partitionColumns.contains(fieldName));
        switch (mappingMode) {
            case ID:
                String columnMappingId = metadata.get("delta.columnMapping.id").asText();
                verify(!isNullOrEmpty(columnMappingId), "id is null or empty");
                fieldId = OptionalInt.of(Integer.parseInt(columnMappingId));
                // Databricks stores column statistics with physical name
                physicalName = metadata.get("delta.columnMapping.physicalName").asText();
                verify(!isNullOrEmpty(physicalName), "physicalName is null or empty");
                physicalColumnType = buildType(typeManager, typeNode, true);
                break;
            case NAME:
                physicalName = metadata.get("delta.columnMapping.physicalName").asText();
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
        return new DeltaLakeColumnMetadata(columnMetadata, fieldName, fieldId, physicalName, physicalColumnType);
    }

    private static void verifyTypeChanges(JsonNode metadata, JsonNode typeNode, boolean isPartitionColumn)
            throws UnsupportedTypeException
    {
        // struct cannot be a partition column
        if (isStruct(typeNode)) {
            verifyStructTypeChanges(typeNode);
        }
        // partition columns should not be skipped as we retrieve them from snapshot files
        else if (!isPartitionColumn) {
            verifyTypeChanges(metadata);
        }
    }

    private static void verifyTypeChanges(JsonNode metadata)
            throws UnsupportedTypeException
    {
        if (metadata.has("delta.typeChanges")) {
            Iterator<JsonNode> typeChanges = metadata.get("delta.typeChanges").elements();
            while (typeChanges.hasNext()) {
                verifyTypeChange(typeChanges.next());
            }
        }
    }

    private static void verifyStructTypeChanges(JsonNode container)
            throws UnsupportedTypeException
    {
        Iterator<JsonNode> fields = container.get("fields").elements();
        while (fields.hasNext()) {
            JsonNode field = fields.next();
            JsonNode fieldMetadata = field.get("metadata");
            JsonNode typeNode = field.get("type");
            verifyTypeChanges(fieldMetadata, typeNode, false);
        }
    }

    private static void verifyTypeChange(JsonNode typeChange)
            throws UnsupportedTypeException
    {
        String fromType = typeChange.get("fromType").asText();
        String toType = typeChange.get("toType").asText();

        if ((fromType.equals("byte") && (toType.equals("short") || toType.equals("integer"))) ||
                (fromType.equals("short") && toType.equals("integer"))) {
            return;
        }
        throw new UnsupportedTypeException(fromType, toType);
    }

    private static boolean isStruct(JsonNode typeNode)
    {
        return typeNode.isContainerNode() && Objects.equals(typeNode.get("type").asText(), "struct");
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

    public static Map<String, Boolean> getColumnIdentities(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsWriterFeatures() && !protocolEntry.writerFeaturesContains(IDENTITY_COLUMNS_FEATURE_NAME)) {
            return ImmutableMap.of();
        }
        return getColumnProperties(metadataEntry, DeltaLakeSchemaSupport::isIdentityColumn);
    }

    private static boolean isIdentityColumn(JsonNode node)
    {
        return Streams.stream(node.get("metadata").fieldNames())
                .anyMatch(name -> name.startsWith("delta.identity."));
    }

    public static Map<String, String> getColumnInvariants(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsWriterFeatures()) {
            if (!protocolEntry.writerFeaturesContains(INVARIANTS_FEATURE_NAME)) {
                return ImmutableMap.of();
            }
            return getColumnProperties(metadataEntry, DeltaLakeSchemaSupport::getInvariantsWriterFeature);
        }
        return getColumnProperties(metadataEntry, DeltaLakeSchemaSupport::getInvariants);
    }

    @Nullable
    private static String getInvariantsWriterFeature(JsonNode node)
    {
        JsonNode invariants = node.get("metadata").get("delta.invariants");
        return invariants == null ? null : invariants.asText();
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

    public static Map<String, String> getCheckConstraints(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsWriterFeatures() && !protocolEntry.writerFeaturesContains(CHECK_CONSTRAINTS_FEATURE_NAME)) {
            return ImmutableMap.of();
        }
        return metadataEntry.getConfiguration().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("delta.constraints."))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Optional<Boolean> changeDataFeedEnabled(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        if (protocolEntry.supportsWriterFeatures() && !protocolEntry.writerFeaturesContains(CHANGE_DATA_FEED_FEATURE_NAME)) {
            return Optional.empty();
        }
        String enableChangeDataFeed = metadataEntry.getConfiguration().get(DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY);
        if (enableChangeDataFeed == null) {
            return Optional.empty();
        }
        return Optional.of(parseBoolean(enableChangeDataFeed));
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

    /**
     * @return the case-sensitive column names
     */
    public static List<String> getExactColumnNames(MetadataEntry metadataEntry)
    {
        try {
            return stream(OBJECT_MAPPER.readTree(metadataEntry.getSchemaString()).get("fields").elements())
                    .map(field -> field.get("name").asText())
                    .collect(toImmutableList());
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse serialized schema: " + metadataEntry.getSchemaString(), e);
        }
    }

    public static Type deserializeType(TypeManager typeManager, Object type, boolean usePhysicalName)
            throws UnsupportedTypeException
    {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(type);
            return buildType(typeManager, OBJECT_MAPPER.readTree(json), usePhysicalName);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Failed to deserialize type: " + type);
        }
    }

    private static Type buildType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
            throws UnsupportedTypeException
    {
        if (typeNode.isContainerNode()) {
            return buildContainerType(typeManager, typeNode, usePhysicalName);
        }
        String primitiveType = typeNode.asText();
        if (primitiveType.startsWith("decimal")) {
            return typeManager.fromSqlType(primitiveType);
        }
        return switch (primitiveType) {
            case "string" -> VARCHAR;
            case "long" -> BIGINT;
            case "integer" -> INTEGER;
            case "short" -> SMALLINT;
            case "byte" -> TINYINT;
            case "float" -> REAL;
            case "double" -> DOUBLE;
            case "boolean" -> BOOLEAN;
            case "binary" -> VARBINARY;
            case "date" -> DATE;
            // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#timestamp-without-timezone-timestampntz
            case "timestamp_ntz" -> TIMESTAMP_MICROS;
            // Spark/DeltaLake stores timestamps in UTC, but renders them in session time zone.
            // For more info, see https://delta-users.slack.com/archives/GKTUWT03T/p1585760533005400
            // and https://cwiki.apache.org/confluence/display/Hive/Different+TIMESTAMP+types
            case "timestamp" -> TIMESTAMP_TZ_MILLIS;
            case "variant" -> throw new UnsupportedTypeException("variant");
            default -> throw new TypeNotFoundException(new TypeSignature(primitiveType));
        };
    }

    private static Type buildContainerType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
            throws UnsupportedTypeException
    {
        String containerType = typeNode.get("type").asText();
        return switch (containerType) {
            case "array" -> buildArrayType(typeManager, typeNode, usePhysicalName);
            case "map" -> buildMapType(typeManager, typeNode, usePhysicalName);
            case "struct" -> buildRowType(typeManager, typeNode, usePhysicalName);
            default -> throw new TypeNotFoundException(new TypeSignature(containerType));
        };
    }

    private static RowType buildRowType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
            throws UnsupportedTypeException
    {
        ImmutableList.Builder<TypeSignatureParameter> fields = ImmutableList.builder();
        Iterator<JsonNode> elements = typeNode.get("fields").elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            String fieldName = usePhysicalName ? element.get("metadata").get("delta.columnMapping.physicalName").asText() : element.get("name").asText();
            verify(!isNullOrEmpty(fieldName), "fieldName is null or empty");
            fields.add(TypeSignatureParameter.namedField(
                    // We lower case the struct field names.
                    // Otherwise, Trino will refuse to write to columns whose struct type has field names containing upper case characters.
                    // Users can't work around this by casting in their queries because Trino parser always lower case types.
                    // TODO: This is a hack. Engine should be able to handle identifiers in a case insensitive way where necessary.
                    // See also HiveTypeTranslator#toTypeSingature.
                    TransactionLogAccess.canonicalizeColumnName(fieldName),
                    buildType(typeManager, element.get("type"), usePhysicalName).getTypeSignature()));
        }
        return (RowType) typeManager.getType(TypeSignature.rowType(fields.build()));
    }

    private static ArrayType buildArrayType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
            throws UnsupportedTypeException
    {
        return (ArrayType) typeManager.getType(TypeSignature.arrayType(buildType(typeManager, typeNode.get("elementType"), usePhysicalName).getTypeSignature()));
    }

    private static MapType buildMapType(TypeManager typeManager, JsonNode typeNode, boolean usePhysicalName)
            throws UnsupportedTypeException
    {
        return (MapType) typeManager.getType(TypeSignature.mapType(
                buildType(typeManager, typeNode.get("keyType"), usePhysicalName).getTypeSignature(),
                buildType(typeManager, typeNode.get("valueType"), usePhysicalName).getTypeSignature()));
    }

    private static Optional<Location> getLocation(JsonProcessingException e)
    {
        return Optional.ofNullable(e.getLocation()).map(location -> new Location(location.getLineNr(), location.getColumnNr()));
    }

    public static class UnsupportedTypeException
            extends Exception
    {
        public UnsupportedTypeException(String type)
        {
            super("Unsupported type: %s".formatted(requireNonNull(type, "type is null")));
        }

        public UnsupportedTypeException(String fromType, String toType)
        {
            super("Type change from '%s' to '%s' is not supported".formatted(requireNonNull(fromType, "fromType is null"), requireNonNull(toType, "toType is null")));
        }
    }
}
