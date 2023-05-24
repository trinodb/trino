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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeSignature;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Delta Lake specific utility which converts the Delta table schema to
 * a Parquet schema.
 * This utility is used instead of Hive's
 * {@link io.trino.parquet.writer.ParquetSchemaConverter}
 * in order to be able to include the field IDs in the Parquet schema.
 */
public final class DeltaLakeParquetSchemas
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private DeltaLakeParquetSchemas() {}

    public static DeltaLakeParquetSchemaMapping createParquetSchemaMapping(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        return createParquetSchemaMapping(metadataEntry, typeManager, false);
    }

    public static DeltaLakeParquetSchemaMapping createParquetSchemaMapping(MetadataEntry metadataEntry, TypeManager typeManager, boolean addChangeDataFeedFields)
    {
        DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry);
        return createParquetSchemaMapping(
                metadataEntry.getSchemaString(),
                typeManager,
                columnMappingMode,
                metadataEntry.getOriginalPartitionColumns(),
                addChangeDataFeedFields);
    }

    public static DeltaLakeParquetSchemaMapping createParquetSchemaMapping(
            String jsonSchema,
            TypeManager typeManager,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> partitionColumnNames)
    {
        return createParquetSchemaMapping(jsonSchema, typeManager, columnMappingMode, partitionColumnNames, false);
    }

    private static DeltaLakeParquetSchemaMapping createParquetSchemaMapping(
            String jsonSchema,
            TypeManager typeManager,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> partitionColumnNames,
            boolean addChangeDataFeedFields)
    {
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(columnMappingMode, "columnMappingMode is null");
        requireNonNull(partitionColumnNames, "partitionColumnNames is null");
        Types.MessageTypeBuilder builder = Types.buildMessage();
        ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder = ImmutableMap.builder();
        try {
            stream(OBJECT_MAPPER.readTree(jsonSchema).get("fields").elements())
                    .filter(fieldNode -> !partitionColumnNames.contains(fieldNode.get("name").asText()))
                    .map(fieldNode -> buildType(fieldNode, typeManager, columnMappingMode, ImmutableList.of(), primitiveTypesBuilder))
                    .forEach(builder::addField);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, getLocation(e), "Failed to parse serialized schema: " + jsonSchema, e);
        }
        if (addChangeDataFeedFields) {
            builder.addField(buildPrimitiveType("string", typeManager, OPTIONAL, DeltaLakeCdfPageSink.CHANGE_TYPE_COLUMN_NAME, OptionalInt.empty(), ImmutableList.of(), primitiveTypesBuilder));
        }

        return new DeltaLakeParquetSchemaMapping(builder.named("trino_schema"), primitiveTypesBuilder.buildOrThrow());
    }

    private static org.apache.parquet.schema.Type buildType(
            JsonNode fieldNode,
            TypeManager typeManager,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        JsonNode typeNode = fieldNode.get("type");
        OptionalInt fieldId = OptionalInt.empty();
        String physicalName;

        switch (columnMappingMode) {
            case ID -> {
                String columnMappingId = fieldNode.get("metadata").get("delta.columnMapping.id").asText();
                verify(!isNullOrEmpty(columnMappingId), "id is null or empty");
                fieldId = OptionalInt.of(Integer.parseInt(columnMappingId));
                // Databricks stores column statistics with physical name
                physicalName = fieldNode.get("metadata").get("delta.columnMapping.physicalName").asText();
                verify(!isNullOrEmpty(physicalName), "physicalName is null or empty");
            }
            case NAME -> {
                physicalName = fieldNode.get("metadata").get("delta.columnMapping.physicalName").asText();
                verify(!isNullOrEmpty(physicalName), "physicalName is null or empty");
            }
            case NONE -> {
                physicalName = fieldNode.get("name").asText();
                verify(!isNullOrEmpty(physicalName), "name is null or empty");
            }
            default -> throw new UnsupportedOperationException("Unsupported parameter columnMappingMode");
        }

        return buildType(typeNode, typeManager, OPTIONAL, physicalName, fieldId, columnMappingMode, parent, primitiveTypesBuilder);
    }

    private static org.apache.parquet.schema.Type buildType(
            JsonNode typeNode,
            TypeManager typeManager,
            org.apache.parquet.schema.Type.Repetition repetition,
            String name,
            OptionalInt id,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        if (typeNode.isContainerNode()) {
            return buildContainerType(typeNode, typeManager, repetition, name, id, columnMappingMode, parent, primitiveTypesBuilder);
        }

        String primitiveType = typeNode.asText();
        return buildPrimitiveType(primitiveType, typeManager, repetition, name, id, parent, primitiveTypesBuilder);
    }

    private static org.apache.parquet.schema.Type buildPrimitiveType(
            String primitiveType,
            TypeManager typeManager,
            org.apache.parquet.schema.Type.Repetition repetition,
            String name,
            OptionalInt id,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        Types.PrimitiveBuilder<PrimitiveType> typeBuilder;
        Type trinoType;
        if (primitiveType.startsWith(StandardTypes.DECIMAL)) {
            trinoType = typeManager.fromSqlType(primitiveType);
            verify(trinoType instanceof DecimalType, "type %s does not map to Trino decimal".formatted(primitiveType));
            DecimalType trinoDecimalType = (DecimalType) trinoType;
            if (trinoDecimalType.getPrecision() <= 9) {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition);
            }
            else if (trinoDecimalType.isShort()) {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition);
            }
            else {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                        .length(PRECISION_TO_BYTE_COUNT[trinoDecimalType.getPrecision()]);
            }
            typeBuilder = typeBuilder.as(decimalType(trinoDecimalType.getScale(), trinoDecimalType.getPrecision()));
            return buildType(name, id, parent, typeBuilder, trinoType, primitiveTypesBuilder);
        }
        switch (primitiveType) {
            case "string" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).as(LogicalTypeAnnotation.stringType());
                trinoType = VARCHAR;
            }
            case "byte" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition);
                trinoType = TINYINT;
            }
            case "short" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition);
                trinoType = SMALLINT;
            }
            case "integer" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition);
                trinoType = INTEGER;
            }
            case "long" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition);
                trinoType = BIGINT;
            }
            case "float" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition);
                trinoType = REAL;
            }
            case "double" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition);
                trinoType = DOUBLE;
            }
            case "boolean" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition);
                trinoType = BOOLEAN;
            }
            case "binary" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition);
                trinoType = VARBINARY;
            }
            case "date" -> {
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).as(LogicalTypeAnnotation.dateType());
                trinoType = DATE;
            }
            case "timestamp" -> {
                // Spark / Delta Lake stores timestamps in UTC, but renders them in session time zone.
                typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS));
                trinoType = TIMESTAMP_MILLIS;
            }
            default -> throw new TrinoException(NOT_SUPPORTED, format("Unsupported primitive type: %s", primitiveType));
        }

        return buildType(name, id, parent, typeBuilder, trinoType, primitiveTypesBuilder);
    }

    private static PrimitiveType buildType(String name, OptionalInt id, List<String> parent, Types.PrimitiveBuilder<PrimitiveType> typeBuilder, Type trinoType, ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        if (id.isPresent()) {
            typeBuilder.id(id.getAsInt());
        }

        List<String> fullName = ImmutableList.<String>builder().addAll(parent).add(name).build();
        primitiveTypesBuilder.put(fullName, trinoType);

        return typeBuilder.named(name);
    }

    private static org.apache.parquet.schema.Type buildContainerType(
            JsonNode typeNode,
            TypeManager typeManager,
            org.apache.parquet.schema.Type.Repetition repetition,
            String name,
            OptionalInt id,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        String containerType = typeNode.get("type").asText();
        return switch (containerType) {
            case "array" -> buildArrayType(typeNode, typeManager, repetition, name, id, columnMappingMode, parent, primitiveTypesBuilder);
            case "map" -> buildMapType(typeNode, typeManager, repetition, name, id, columnMappingMode, parent, primitiveTypesBuilder);
            case "struct" -> buildRowType(typeNode, typeManager, repetition, name, id, columnMappingMode, parent, primitiveTypesBuilder);
            default -> throw new TypeNotFoundException(new TypeSignature(containerType));
        };
    }

    private static org.apache.parquet.schema.Type buildArrayType(
            JsonNode typeNode,
            TypeManager typeManager,
            org.apache.parquet.schema.Type.Repetition repetition,
            String name,
            OptionalInt id,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("list").build();

        JsonNode elementTypeNode = typeNode.get("elementType");
        org.apache.parquet.schema.Type elementType;

        if (elementTypeNode.isContainerNode()) {
            elementType = buildContainerType(elementTypeNode, typeManager, OPTIONAL, "element", OptionalInt.empty(), columnMappingMode, parent, primitiveTypesBuilder);
        }
        else {
            elementType = buildType(elementTypeNode, typeManager, OPTIONAL, "element", OptionalInt.empty(), columnMappingMode, parent, primitiveTypesBuilder);
        }

        GroupType arrayType = Types.list(repetition)
                .element(elementType)
                .named(name);
        if (id.isPresent()) {
            arrayType = arrayType.withId(id.getAsInt());
        }
        return arrayType;
    }

    private static org.apache.parquet.schema.Type buildMapType(
            JsonNode typeNode,
            TypeManager typeManager,
            org.apache.parquet.schema.Type.Repetition repetition,
            String name,
            OptionalInt id,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        Types.MapBuilder<GroupType> builder = Types.map(repetition);
        if (id.isPresent()) {
            builder.id(id.getAsInt());
        }

        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("key_value").build();

        JsonNode keyTypeNode = typeNode.get("keyType");
        org.apache.parquet.schema.Type keyType;
        if (keyTypeNode.isContainerNode()) {
            keyType = buildContainerType(keyTypeNode, typeManager, REQUIRED, "key", OptionalInt.empty(), columnMappingMode, parent, primitiveTypesBuilder);
        }
        else {
            keyType = buildType(keyTypeNode, typeManager, REQUIRED, "key", OptionalInt.empty(), columnMappingMode, parent, primitiveTypesBuilder);
        }
        JsonNode valueTypeNode = typeNode.get("valueType");
        org.apache.parquet.schema.Type valueType;
        if (valueTypeNode.isContainerNode()) {
            valueType = buildContainerType(valueTypeNode, typeManager, OPTIONAL, "value", OptionalInt.empty(), columnMappingMode, parent, primitiveTypesBuilder);
        }
        else {
            valueType = buildType(valueTypeNode, typeManager, OPTIONAL, "value", OptionalInt.empty(), columnMappingMode, parent, primitiveTypesBuilder);
        }

        return builder
                .key(keyType)
                .value(valueType)
                .named(name);
    }

    private static org.apache.parquet.schema.Type buildRowType(
            JsonNode typeNode,
            TypeManager typeManager,
            org.apache.parquet.schema.Type.Repetition repetition,
            String name,
            OptionalInt id,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> parent,
            ImmutableMap.Builder<List<String>, Type> primitiveTypesBuilder)
    {
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);
        if (id.isPresent()) {
            builder.id(id.getAsInt());
        }
        List<String> currentParent = ImmutableList.<String>builder().addAll(parent).add(name).build();
        stream(typeNode.get("fields").elements())
                .map(node -> buildType(node, typeManager, columnMappingMode, currentParent, primitiveTypesBuilder))
                .forEach(builder::addField);
        return builder.named(name);
    }

    private static Optional<Location> getLocation(JsonProcessingException e)
    {
        return Optional.ofNullable(e.getLocation()).map(location -> new Location(location.getLineNr(), location.getColumnNr()));
    }
}
