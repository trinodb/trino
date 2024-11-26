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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.util.DefaultLocationProvider;
import io.trino.plugin.iceberg.util.ObjectStoreLocationProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.builderWithExpectedSize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.parquet.writer.ParquetWriter.SUPPORTED_BLOOM_FILTER_TYPES;
import static io.trino.plugin.base.io.ByteBuffers.getWrappedBytes;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnMetadata;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnMetadata;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_PARTITION_VALUE;
import static io.trino.plugin.iceberg.IcebergTableProperties.DATA_LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.OBJECT_STORE_LAYOUT_ENABLED_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_COLUMNS_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_FPP_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PROTECTED_ICEBERG_NATIVE_PROPERTIES;
import static io.trino.plugin.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.SUPPORTED_PROPERTIES;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getSortOrder;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.SortFieldUtils.toSortFields;
import static io.trino.plugin.iceberg.TrinoMetricsReporter.TRINO_METRICS_REPORTER;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_FPP;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;
import static org.apache.iceberg.util.PropertyUtil.propertyAsBoolean;

public final class IcebergUtil
{
    public static final String TRINO_TABLE_METADATA_INFO_VALID_FOR = "trino_table_metadata_info_valid_for";
    public static final String TRINO_TABLE_COMMENT_CACHE_PREVENTED = "trino_table_comment_cache_prevented";
    public static final String COLUMN_TRINO_NOT_NULL_PROPERTY = "trino_not_null";
    public static final String COLUMN_TRINO_TYPE_ID_PROPERTY = "trino_type_id";

    public static final String METADATA_FOLDER_NAME = "metadata";
    public static final String METADATA_FILE_EXTENSION = ".metadata.json";
    public static final String TRINO_QUERY_ID_NAME = "trino_query_id";
    public static final String TRINO_USER_NAME = "trino_user";
    // For backward compatibility only. DO NOT USE.
    private static final String BROKEN_ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";
    // For backward compatibility only. DO NOT USE.
    private static final String BROKEN_ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");
    // Metadata file name examples
    //  - 00001-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json
    //  - 00001-409702ba-4735-4645-8f14-09537cc0b2c8.gz.metadata.json (https://github.com/apache/iceberg/blob/ab398a0d5ff195f763f8c7a4358ac98fa38a8de7/core/src/main/java/org/apache/iceberg/TableMetadataParser.java#L141)
    //  - 00001-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json.gz (https://github.com/apache/iceberg/blob/ab398a0d5ff195f763f8c7a4358ac98fa38a8de7/core/src/main/java/org/apache/iceberg/TableMetadataParser.java#L146)
    private static final Pattern METADATA_FILE_NAME_PATTERN = Pattern.compile("(?<version>\\d+)-(?<uuid>[-a-fA-F0-9]*)(?<compression>\\.[a-zA-Z0-9]+)?" + Pattern.quote(METADATA_FILE_EXTENSION) + "(?<compression2>\\.[a-zA-Z0-9]+)?");
    // Hadoop Generated Metadata file name examples
    //  - v0.metadata.json
    //  - v0.gz.metadata.json
    //  - v0.metadata.json.gz
    private static final Pattern HADOOP_GENERATED_METADATA_FILE_NAME_PATTERN = Pattern.compile("v(?<version>\\d+)(?<compression>\\.[a-zA-Z0-9]+)?" + Pattern.quote(METADATA_FILE_EXTENSION) + "(?<compression2>\\.[a-zA-Z0-9]+)?");

    private IcebergUtil() {}

    public static Table loadIcebergTable(TrinoCatalog catalog, IcebergTableOperationsProvider tableOperationsProvider, ConnectorSession session, SchemaTableName table)
    {
        TableOperations operations = tableOperationsProvider.createTableOperations(
                catalog,
                session,
                table.getSchemaName(),
                table.getTableName(),
                Optional.empty(),
                Optional.empty());
        return new BaseTable(operations, quotedTableName(table), TRINO_METRICS_REPORTER);
    }

    public static Table getIcebergTableWithMetadata(
            TrinoCatalog catalog,
            IcebergTableOperationsProvider tableOperationsProvider,
            ConnectorSession session,
            SchemaTableName table,
            TableMetadata tableMetadata)
    {
        IcebergTableOperations operations = tableOperationsProvider.createTableOperations(
                catalog,
                session,
                table.getSchemaName(),
                table.getTableName(),
                Optional.empty(),
                Optional.empty());
        operations.initializeFromMetadata(tableMetadata);
        return new BaseTable(operations, quotedTableName(table), TRINO_METRICS_REPORTER);
    }

    public static List<IcebergColumnHandle> getProjectedColumns(Schema schema, TypeManager typeManager)
    {
        Map<Integer, NestedField> indexById = TypeUtil.indexById(schema.asStruct());
        return getProjectedColumns(schema, typeManager, indexById, indexById.keySet() /* project all columns */);
    }

    public static List<IcebergColumnHandle> getProjectedColumns(Schema schema, TypeManager typeManager, Set<Integer> fieldIds)
    {
        Map<Integer, NestedField> indexById = TypeUtil.indexById(schema.asStruct());
        return getProjectedColumns(schema, typeManager, indexById, fieldIds /* project selected columns */);
    }

    private static List<IcebergColumnHandle> getProjectedColumns(Schema schema, TypeManager typeManager, Map<Integer, NestedField> indexById, Set<Integer> fieldIds)
    {
        ImmutableList.Builder<IcebergColumnHandle> columns = builderWithExpectedSize(fieldIds.size());
        Map<Integer, Integer> indexParents = TypeUtil.indexParents(schema.asStruct());
        Map<Integer, List<Integer>> indexPaths = indexById.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableList.copyOf(buildPath(indexParents, entry.getKey()))));

        for (int fieldId : fieldIds) {
            columns.add(createColumnHandle(typeManager, fieldId, indexById, indexPaths));
        }
        return columns.build();
    }

    public static IcebergColumnHandle createColumnHandle(TypeManager typeManager, int fieldId, Map<Integer, NestedField> indexById, Map<Integer, List<Integer>> indexPaths)
    {
        NestedField childField = indexById.get(fieldId);
        NestedField baseField = childField;

        List<Integer> path = requireNonNull(indexPaths.get(fieldId));
        if (!path.isEmpty()) {
            baseField = indexById.get(path.getFirst());
            path = ImmutableList.<Integer>builder()
                    .addAll(path.subList(1, path.size())) // Base column id shouldn't exist in IcebergColumnHandle.path
                    .add(fieldId) // Append the leaf field id
                    .build();
        }
        return createColumnHandle(baseField, childField, typeManager, path);
    }

    public static List<Integer> buildPath(Map<Integer, Integer> indexParents, int fieldId)
    {
        List<Integer> path = new ArrayList<>();
        while (indexParents.containsKey(fieldId)) {
            int parentId = indexParents.get(fieldId);
            path.add(parentId);
            fieldId = parentId;
        }
        return ImmutableList.copyOf(path.reversed());
    }

    public static Map<String, Object> getIcebergTableProperties(Table icebergTable)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        SortOrder sortOrder = icebergTable.sortOrder();
        // TODO: Support sort column transforms (https://github.com/trinodb/trino/issues/15088)
        if (sortOrder.isSorted() && sortOrder.fields().stream().allMatch(sortField -> sortField.transform().isIdentity())) {
            List<String> sortColumnNames = toSortFields(sortOrder);
            properties.put(SORTED_BY_PROPERTY, sortColumnNames);
        }

        if (!icebergTable.location().isEmpty()) {
            properties.put(LOCATION_PROPERTY, icebergTable.location());
        }

        int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        properties.put(FORMAT_VERSION_PROPERTY, formatVersion);

        // iceberg ORC format bloom filter properties
        Optional<String> orcBloomFilterColumns = getOrcBloomFilterColumns(icebergTable.properties());
        if (orcBloomFilterColumns.isPresent()) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS_PROPERTY, Splitter.on(',').trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns.get()));
        }
        // iceberg ORC format bloom filter properties
        Optional<String> orcBloomFilterFpp = getOrcBloomFilterFpp(icebergTable.properties());
        if (orcBloomFilterFpp.isPresent()) {
            properties.put(ORC_BLOOM_FILTER_FPP_PROPERTY, Double.parseDouble(orcBloomFilterFpp.get()));
        }

        // iceberg Parquet format bloom filter properties
        Set<String> parquetBloomFilterColumns = getParquetBloomFilterColumns(icebergTable.properties());
        if (!parquetBloomFilterColumns.isEmpty()) {
            properties.put(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY, ImmutableList.copyOf(parquetBloomFilterColumns));
        }

        if (parseBoolean(icebergTable.properties().getOrDefault(OBJECT_STORE_ENABLED, "false"))) {
            properties.put(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY, true);
        }

        Optional<String> dataLocation = Optional.ofNullable(icebergTable.properties().get(WRITE_DATA_LOCATION));
        dataLocation.ifPresent(location -> properties.put(DATA_LOCATION_PROPERTY, location));

        return properties.buildOrThrow();
    }

    // Version 382-438 set incorrect table properties: https://github.com/trinodb/trino/commit/b89aac68c43e5392f23b8d6ba053bbeb6df85028#diff-2af3e19a6b656640a7d0bb73114ef224953a2efa04e569b1fe4da953b2cc6d15R418-R419
    // `orc.bloom.filter.columns` was set instead of `write.orc.bloom.filter.columns`, and `orc.bloom.filter.fpp` instead of `write.orc.bloom.filter.fpp`
    // These methods maintain backward compatibility for existing table.
    public static Optional<String> getOrcBloomFilterColumns(Map<String, String> properties)
    {
        Optional<String> orcBloomFilterColumns = Stream.of(
                        properties.get(ORC_BLOOM_FILTER_COLUMNS),
                        properties.get(BROKEN_ORC_BLOOM_FILTER_COLUMNS_KEY))
                .filter(Objects::nonNull)
                .findFirst();
        return orcBloomFilterColumns;
    }

    public static Set<String> getParquetBloomFilterColumns(Map<String, String> properties)
    {
        return properties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX) && "true".equals(entry.getValue()))
                .map(entry -> entry.getKey().substring(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX.length()))
                .collect(toImmutableSet());
    }

    public static Optional<String> getOrcBloomFilterFpp(Map<String, String> properties)
    {
        return Stream.of(
                        properties.get(ORC_BLOOM_FILTER_FPP),
                        properties.get(BROKEN_ORC_BLOOM_FILTER_FPP_KEY))
                .filter(Objects::nonNull)
                .findFirst();
    }

    public static List<IcebergColumnHandle> getTopLevelColumns(Schema schema, TypeManager typeManager)
    {
        return schema.columns().stream()
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());
    }

    public static List<ColumnMetadata> getColumnMetadatas(Schema schema, TypeManager typeManager)
    {
        List<NestedField> icebergColumns = schema.columns();
        ImmutableList.Builder<ColumnMetadata> columns = builderWithExpectedSize(icebergColumns.size() + 2);

        icebergColumns.stream()
                .map(column ->
                        ColumnMetadata.builder()
                                .setName(column.name())
                                .setType(toTrinoType(column.type(), typeManager))
                                .setNullable(column.isOptional())
                                .setComment(Optional.ofNullable(column.doc()))
                                .build())
                .forEach(columns::add);
        columns.add(pathColumnMetadata());
        columns.add(fileModifiedTimeColumnMetadata());
        return columns.build();
    }

    public static Schema updateColumnComment(Schema schema, String columnName, String comment)
    {
        NestedField fieldToUpdate = schema.findField(columnName);
        checkArgument(fieldToUpdate != null, "Field %s does not exist", columnName);
        NestedField updatedField = NestedField.of(fieldToUpdate.fieldId(), fieldToUpdate.isOptional(), fieldToUpdate.name(), fieldToUpdate.type(), comment);
        List<NestedField> newFields = schema.columns().stream()
                .map(field -> (field.fieldId() == updatedField.fieldId()) ? updatedField : field)
                .toList();

        return new Schema(newFields, schema.getAliases(), schema.identifierFieldIds());
    }

    public static IcebergColumnHandle getColumnHandle(NestedField column, TypeManager typeManager)
    {
        return createColumnHandle(column, column, typeManager, ImmutableList.of());
    }

    private static IcebergColumnHandle createColumnHandle(NestedField baseColumn, NestedField childColumn, TypeManager typeManager, List<Integer> path)
    {
        return new IcebergColumnHandle(
                createColumnIdentity(baseColumn),
                toTrinoType(baseColumn.type(), typeManager),
                path,
                toTrinoType(childColumn.type(), typeManager),
                childColumn.isOptional(),
                Optional.ofNullable(childColumn.doc()));
    }

    public static Schema schemaFromHandles(List<IcebergColumnHandle> columns)
    {
        List<NestedField> icebergColumns = columns.stream()
                .map(column -> NestedField.optional(column.getId(), column.getName(), toIcebergType(column.getType(), column.getColumnIdentity())))
                .collect(toImmutableList());
        return new Schema(StructType.of(icebergColumns).asStructType().fields());
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().isIdentity()) {
                columns.put(field, i);
            }
        }
        return columns.buildOrThrow();
    }

    public static List<Types.NestedField> primitiveFields(Schema schema)
    {
        return primitiveFields(schema.columns())
                .collect(toImmutableList());
    }

    private static Stream<Types.NestedField> primitiveFields(List<NestedField> nestedFields)
    {
        return nestedFields.stream()
                .flatMap(IcebergUtil::primitiveFields);
    }

    private static Stream<Types.NestedField> primitiveFields(NestedField nestedField)
    {
        org.apache.iceberg.types.Type type = nestedField.type();
        if (type.isPrimitiveType()) {
            return Stream.of(nestedField);
        }

        if (type.isNestedType()) {
            return primitiveFields(type.asNestedType().fields())
                    .map(field -> Types.NestedField.of(field.fieldId(), field.isOptional(), nestedField.name() + "." + field.name(), field.type(), field.doc()));
        }

        throw new IllegalStateException("Unsupported field type: " + nestedField);
    }

    public static Map<Integer, PrimitiveType> primitiveFieldTypes(Schema schema)
    {
        return primitiveFieldTypes(schema.columns())
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    private static Stream<Entry<Integer, PrimitiveType>> primitiveFieldTypes(List<NestedField> nestedFields)
    {
        return nestedFields.stream()
                .flatMap(IcebergUtil::primitiveFieldTypes);
    }

    private static Stream<Entry<Integer, PrimitiveType>> primitiveFieldTypes(NestedField nestedField)
    {
        org.apache.iceberg.types.Type fieldType = nestedField.type();
        if (fieldType.isPrimitiveType()) {
            return Stream.of(Map.entry(nestedField.fieldId(), fieldType.asPrimitiveType()));
        }

        if (fieldType.isNestedType()) {
            return primitiveFieldTypes(fieldType.asNestedType().fields());
        }

        throw new IllegalStateException("Unsupported field type: " + nestedField);
    }

    public static IcebergFileFormat getFileFormat(Table table)
    {
        return getFileFormat(table.properties());
    }

    public static IcebergFileFormat getFileFormat(Map<String, String> storageProperties)
    {
        return IcebergFileFormat.fromIceberg(FileFormat.valueOf(storageProperties
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH)));
    }

    public static Optional<String> getTableComment(Table table)
    {
        return Optional.ofNullable(table.properties().get(TABLE_COMMENT));
    }

    public static String quotedTableName(SchemaTableName name)
    {
        return quotedName(name.getSchemaName()) + "." + quotedName(name.getTableName());
    }

    private static String quotedName(String name)
    {
        if (SIMPLE_NAME.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    public static boolean canEnforceColumnConstraintInSpecs(
            TypeOperators typeOperators,
            Table table,
            Set<Integer> partitionSpecIds,
            IcebergColumnHandle columnHandle,
            Domain domain)
    {
        List<PartitionSpec> partitionSpecs = table.specs().values().stream()
                .filter(partitionSpec -> partitionSpecIds.contains(partitionSpec.specId()))
                .collect(toImmutableList());

        if (partitionSpecs.isEmpty()) {
            return false;
        }

        return partitionSpecs.stream().allMatch(spec -> canEnforceConstraintWithinPartitioningSpec(typeOperators, spec, columnHandle, domain));
    }

    private static boolean canEnforceConstraintWithinPartitioningSpec(TypeOperators typeOperators, PartitionSpec spec, IcebergColumnHandle column, Domain domain)
    {
        for (PartitionField field : spec.getFieldsBySourceId(column.getId())) {
            if (canEnforceConstraintWithPartitionField(typeOperators, field, column, domain)) {
                return true;
            }
        }
        return false;
    }

    private static boolean canEnforceConstraintWithPartitionField(TypeOperators typeOperators, PartitionField field, IcebergColumnHandle column, Domain domain)
    {
        if (field.transform().isVoid()) {
            // Useless for filtering.
            return false;
        }
        if (field.transform().isIdentity()) {
            // A predicate on an identity partitioning column can always be enforced.
            return true;
        }

        ColumnTransform transform = PartitionTransforms.getColumnTransform(field, column.getType());
        if (transform.preservesNonNull()) {
            // Partitioning transform must return NULL for NULL input.
            // Below we assume it never returns NULL for non-NULL input,
            // so NULL values and non-NULL values are always segregated.
            // In practice, this condition matches the void transform only,
            // which isn't useful for filtering anyway.
            return false;
        }
        ValueSet valueSet = domain.getValues();

        boolean canEnforce = valueSet.getValuesProcessor().transform(
                ranges -> {
                    MethodHandle targetTypeEqualOperator = typeOperators.getEqualOperator(
                            transform.type(), InvocationConvention.simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
                    for (Range range : ranges.getOrderedRanges()) {
                        if (!canEnforceRangeWithPartitioningField(field, transform, range, targetTypeEqualOperator)) {
                            return false;
                        }
                    }
                    return true;
                },
                discreteValues -> false,
                allOrNone -> true);
        return canEnforce;
    }

    private static boolean canEnforceRangeWithPartitioningField(PartitionField field, ColumnTransform transform, Range range, MethodHandle targetTypeEqualOperator)
    {
        if (!transform.monotonic()) {
            // E.g. bucketing transform
            return false;
        }
        io.trino.spi.type.Type type = range.getType();
        if (!type.isOrderable()) {
            return false;
        }
        if (!range.isLowUnbounded()) {
            Object boundedValue = range.getLowBoundedValue();
            Optional<Object> adjacentValue = range.isLowInclusive() ? type.getPreviousValue(boundedValue) : type.getNextValue(boundedValue);
            if (adjacentValue.isEmpty() || yieldSamePartitioningValue(field, transform, type, boundedValue, adjacentValue.get(), targetTypeEqualOperator)) {
                return false;
            }
        }
        if (!range.isHighUnbounded()) {
            Object boundedValue = range.getHighBoundedValue();
            Optional<Object> adjacentValue = range.isHighInclusive() ? type.getNextValue(boundedValue) : type.getPreviousValue(boundedValue);
            if (adjacentValue.isEmpty() || yieldSamePartitioningValue(field, transform, type, boundedValue, adjacentValue.get(), targetTypeEqualOperator)) {
                return false;
            }
        }
        return true;
    }

    private static boolean yieldSamePartitioningValue(
            PartitionField field,
            ColumnTransform transform,
            io.trino.spi.type.Type sourceType,
            Object first,
            Object second,
            MethodHandle targetTypeEqualOperator)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        Object firstTransformed = transform.valueTransform().apply(nativeValueToBlock(sourceType, first), 0);
        Object secondTransformed = transform.valueTransform().apply(nativeValueToBlock(sourceType, second), 0);
        // The pushdown logic assumes NULLs and non-NULLs are segregated, so that we have to think about non-null values only.
        verify(firstTransformed != null && secondTransformed != null, "Transform for %s returned null for non-null input", field);
        try {
            return (boolean) targetTypeEqualOperator.invoke(firstTransformed, secondTransformed);
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    @Nullable
    public static Object deserializePartitionValue(Type type, String valueString, String name)
    {
        if (valueString == null) {
            return null;
        }

        try {
            if (type.equals(BOOLEAN)) {
                if (valueString.equalsIgnoreCase("true")) {
                    return true;
                }
                if (valueString.equalsIgnoreCase("false")) {
                    return false;
                }
                throw new IllegalArgumentException();
            }
            if (type.equals(INTEGER)) {
                return parseLong(valueString);
            }
            if (type.equals(BIGINT)) {
                return parseLong(valueString);
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(parseFloat(valueString));
            }
            if (type.equals(DOUBLE)) {
                return parseDouble(valueString);
            }
            if (type.equals(DATE)) {
                return parseLong(valueString);
            }
            if (type.equals(TIME_MICROS)) {
                return parseLong(valueString) * PICOSECONDS_PER_MICROSECOND;
            }
            if (type.equals(TIMESTAMP_MICROS)) {
                return parseLong(valueString);
            }
            if (type.equals(TIMESTAMP_TZ_MICROS)) {
                return timestampTzFromMicros(parseLong(valueString));
            }
            if (type instanceof VarcharType varcharType) {
                Slice value = utf8Slice(valueString);
                if (!varcharType.isUnbounded() && SliceUtf8.countCodePoints(value) > varcharType.getBoundedLength()) {
                    throw new IllegalArgumentException();
                }
                return value;
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return Slices.wrappedBuffer(Base64.getDecoder().decode(valueString));
            }
            if (type.equals(UuidType.UUID)) {
                return javaUuidToTrinoUuid(UUID.fromString(valueString));
            }
            if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = new BigDecimal(valueString);
                decimal = decimal.setScale(decimalType.getScale(), UNNECESSARY);
                if (decimal.precision() > decimalType.getPrecision()) {
                    throw new IllegalArgumentException();
                }
                BigInteger unscaledValue = decimal.unscaledValue();
                return decimalType.isShort() ? unscaledValue.longValue() : Int128.valueOf(unscaledValue);
            }
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(ICEBERG_INVALID_PARTITION_VALUE, format(
                    "Invalid partition value '%s' for %s partition key: %s",
                    valueString,
                    type.getDisplayName(),
                    name));
        }
        // Iceberg tables don't partition by non-primitive-type columns.
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid partition type " + type);
    }

    /**
     * Returns a map from fieldId to serialized partition value containing entries for all identity partitions.
     * {@code null} partition values are represented with {@link Optional#empty}.
     */
    public static Map<Integer, Optional<String>> getPartitionKeys(FileScanTask scanTask)
    {
        return getPartitionKeys(scanTask.file().partition(), scanTask.spec());
    }

    public static Map<Integer, Optional<String>> getPartitionKeys(StructLike partition, PartitionSpec spec)
    {
        Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
        ImmutableMap.Builder<Integer, Optional<String>> partitionKeys = ImmutableMap.builder();

        fieldToIndex.forEach((field, index) -> {
            int id = field.sourceId();
            org.apache.iceberg.types.Type type = spec.schema().findType(id);
            Class<?> javaClass = type.typeId().javaClass();
            Object value = partition.get(index, javaClass);

            if (value == null) {
                partitionKeys.put(id, Optional.empty());
            }
            else {
                String partitionValue;
                if (type.typeId() == FIXED || type.typeId() == BINARY) {
                    // this is safe because Iceberg PartitionData directly wraps the byte array
                    partitionValue = Base64.getEncoder().encodeToString(getWrappedBytes(((ByteBuffer) value)));
                }
                else {
                    partitionValue = value.toString();
                }
                partitionKeys.put(id, Optional.of(partitionValue));
            }
        });

        return partitionKeys.buildOrThrow();
    }

    public static Map<ColumnHandle, NullableValue> getPartitionValues(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Map<Integer, Optional<String>> partitionKeys)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> bindings = ImmutableMap.builder();
        for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
            Object partitionValue = deserializePartitionValue(
                    partitionColumn.getType(),
                    partitionKeys.get(partitionColumn.getId()).orElse(null),
                    partitionColumn.getName());
            NullableValue bindingValue = new NullableValue(partitionColumn.getType(), partitionValue);
            bindings.put(partitionColumn, bindingValue);
        }
        return bindings.buildOrThrow();
    }

    public static LocationProvider getLocationProvider(SchemaTableName schemaTableName, String tableLocation, Map<String, String> storageProperties)
    {
        if (storageProperties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + storageProperties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }

        if (propertyAsBoolean(storageProperties, OBJECT_STORE_ENABLED, OBJECT_STORE_ENABLED_DEFAULT)) {
            return new ObjectStoreLocationProvider(tableLocation, storageProperties);
        }

        return new DefaultLocationProvider(tableLocation, storageProperties);
    }

    public static Schema schemaFromMetadata(List<ColumnMetadata> columns)
    {
        List<NestedField> icebergColumns = new ArrayList<>();
        int visibleColumnCount = (int) columns.stream().filter(column -> !column.isHidden()).count();
        AtomicInteger nextFieldId = new AtomicInteger(visibleColumnCount + 1);
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size() + 1;
                org.apache.iceberg.types.Type type = toIcebergTypeForNewColumn(column.getType(), nextFieldId);
                NestedField field = NestedField.of(index, column.isNullable(), column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        org.apache.iceberg.types.Type icebergSchema = StructType.of(icebergColumns);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public static Schema schemaFromViewColumns(TypeManager typeManager, List<ViewColumn> columns)
    {
        List<NestedField> icebergColumns = new ArrayList<>();
        AtomicInteger nextFieldId = new AtomicInteger(1);
        for (ViewColumn column : columns) {
            Type trinoType = typeManager.getType(column.getType());
            org.apache.iceberg.types.Type type = toIcebergTypeForNewColumn(trinoType, nextFieldId);
            NestedField field = NestedField.of(nextFieldId.getAndIncrement(), false, column.getName(), type, column.getComment().orElse(null));
            icebergColumns.add(field);
        }
        org.apache.iceberg.types.Type icebergSchema = StructType.of(icebergColumns);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public static List<ViewColumn> viewColumnsFromSchema(TypeManager typeManager, Schema schema)
    {
        return IcebergUtil.getTopLevelColumns(schema, typeManager).stream()
                .map(column -> new ViewColumn(column.getName(), column.getType().getTypeId(), column.getComment()))
                .toList();
    }

    public static Transaction newCreateTableTransaction(TrinoCatalog catalog, ConnectorTableMetadata tableMetadata, ConnectorSession session, boolean replace, String tableLocation, Predicate<String> allowedExtraProperties)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(tableMetadata.getProperties()));

        if (replace) {
            return catalog.newCreateOrReplaceTableTransaction(session, schemaTableName, schema, partitionSpec, sortOrder, tableLocation, createTableProperties(tableMetadata, allowedExtraProperties));
        }
        return catalog.newCreateTableTransaction(session, schemaTableName, schema, partitionSpec, sortOrder, tableLocation, createTableProperties(tableMetadata, allowedExtraProperties));
    }

    public static Map<String, String> createTableProperties(ConnectorTableMetadata tableMetadata, Predicate<String> allowedExtraProperties)
    {
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        IcebergFileFormat fileFormat = IcebergTableProperties.getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toIceberg().toString());
        propertiesBuilder.put(FORMAT_VERSION, Integer.toString(IcebergTableProperties.getFormatVersion(tableMetadata.getProperties())));

        boolean objectStoreLayoutEnabled = IcebergTableProperties.getObjectStoreLayoutEnabled(tableMetadata.getProperties());
        if (objectStoreLayoutEnabled) {
            propertiesBuilder.put(OBJECT_STORE_ENABLED, "true");
        }
        Optional<String> dataLocation = IcebergTableProperties.getDataLocation(tableMetadata.getProperties());
        dataLocation.ifPresent(location -> {
            if (!objectStoreLayoutEnabled) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Data location can only be set when object store layout is enabled");
            }
            propertiesBuilder.put(WRITE_DATA_LOCATION, location);
        });

        // iceberg ORC format bloom filter properties used by create table
        List<String> orcBloomFilterColumns = IcebergTableProperties.getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (!orcBloomFilterColumns.isEmpty()) {
            checkFormatForProperty(fileFormat.toIceberg(), FileFormat.ORC, ORC_BLOOM_FILTER_COLUMNS_PROPERTY);
            validateOrcBloomFilterColumns(tableMetadata, orcBloomFilterColumns);
            propertiesBuilder.put(ORC_BLOOM_FILTER_COLUMNS, Joiner.on(",").join(orcBloomFilterColumns));
            propertiesBuilder.put(ORC_BLOOM_FILTER_FPP, String.valueOf(IcebergTableProperties.getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        // iceberg Parquet format bloom filter properties used by create table
        List<String> parquetBloomFilterColumns = IcebergTableProperties.getParquetBloomFilterColumns(tableMetadata.getProperties());
        if (!parquetBloomFilterColumns.isEmpty()) {
            checkFormatForProperty(fileFormat.toIceberg(), FileFormat.PARQUET, PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY);
            validateParquetBloomFilterColumns(tableMetadata, parquetBloomFilterColumns);
            for (String column : parquetBloomFilterColumns) {
                propertiesBuilder.put(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + column, "true");
            }
        }

        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        Map<String, String> baseProperties = propertiesBuilder.buildOrThrow();
        Map<String, String> extraProperties = IcebergTableProperties.getExtraProperties(tableMetadata.getProperties()).orElseGet(ImmutableMap::of);

        verifyExtraProperties(baseProperties.keySet(), extraProperties, allowedExtraProperties);

        return ImmutableMap.<String, String>builder()
                .putAll(baseProperties)
                .putAll(extraProperties)
                .buildOrThrow();
    }

    public static void verifyExtraProperties(Set<String> basePropertyKeys, Map<String, String> extraProperties, Predicate<String> allowedExtraProperties)
    {
        Set<String> illegalExtraProperties = ImmutableSet.<String>builder()
                .addAll(Sets.intersection(
                        ImmutableSet.<String>builder()
                                .add(TABLE_COMMENT)
                                .addAll(basePropertyKeys)
                                .addAll(SUPPORTED_PROPERTIES)
                                .addAll(PROTECTED_ICEBERG_NATIVE_PROPERTIES)
                                .build(),
                        extraProperties.keySet()))
                .addAll(extraProperties.keySet().stream()
                        .filter(name -> !allowedExtraProperties.test(name))
                        .collect(toImmutableSet()))
                .build();

        if (!illegalExtraProperties.isEmpty()) {
            throw new TrinoException(
                    INVALID_TABLE_PROPERTY,
                    format("Illegal keys in extra_properties: %s", illegalExtraProperties));
        }
    }

    /**
     * Find the first snapshot in the table. First snapshot is the last snapshot
     * in snapshot parents chain starting at {@link Table#currentSnapshot()}.
     *
     * @return First (oldest) Snapshot reachable from {@link Table#currentSnapshot()} or empty if table history
     * expiration makes it impossible to find the snapshot.
     * @throws IllegalArgumentException when table has no snapshot.
     */
    public static Optional<Snapshot> firstSnapshot(Table table)
    {
        Snapshot current = table.currentSnapshot();
        checkArgument(current != null, "No current snapshot in %s when looking for the first snapshot", table);

        while (true) {
            if (current.parentId() == null) {
                return Optional.of(current);
            }
            current = table.snapshot(current.parentId());
            if (current == null) {
                // History expired
                return Optional.empty();
            }
        }
    }

    /**
     * @return First (oldest) snapshot that is reachable from {@link Table#currentSnapshot()} but is not
     * reachable from snapshot with id {@code baseSnapshotId}. Returns empty if table history
     * expiration makes it impossible to find the snapshot.
     * @throws IllegalArgumentException when table has no snapshot,
     * {@code baseSnapshotId} is not a valid snapshot in the table or
     * the {@code baseSnapshotId} is the current snapshot.
     */
    public static Optional<Snapshot> firstSnapshotAfter(Table table, long baseSnapshotId)
    {
        Snapshot current = table.currentSnapshot();
        checkArgument(current != null, "No current snapshot in %s when looking for the first snapshot after %s", table, baseSnapshotId);
        checkArgument(current.snapshotId() != baseSnapshotId, "No snapshot after %s in %s, current snapshot is %s", baseSnapshotId, table, current);

        while (true) {
            if (current.parentId() == null) {
                // Current is the first snapshot in the table, which means we reached end of table history not finding baseSnapshotId. This is possible
                // when table was rolled back and baseSnapshotId is no longer referenced.
                return Optional.empty();
            }
            if (current.parentId() == baseSnapshotId) {
                return Optional.of(current);
            }
            current = table.snapshot(current.parentId());
            if (current == null) {
                // History expired
                return Optional.empty();
            }
        }
    }

    public static long getSnapshotIdAsOfTime(Table table, long epochMillis)
    {
        // The order of table.history() elements is not guaranteed, refer: https://github.com/apache/iceberg/issues/3891#issuecomment-1012303069
        return table.history().stream()
                .filter(logEntry -> logEntry.timestampMillis() <= epochMillis)
                .max(comparing(HistoryEntry::timestampMillis))
                .orElseThrow(() -> new TrinoException(INVALID_ARGUMENTS, format("No version history table %s at or before %s", table.name(), Instant.ofEpochMilli(epochMillis))))
                .snapshotId();
    }

    private static void checkFormatForProperty(FileFormat actualStorageFormat, FileFormat expectedStorageFormat, String propertyName)
    {
        if (actualStorageFormat != expectedStorageFormat) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", propertyName, actualStorageFormat));
        }
    }

    private static void validateOrcBloomFilterColumns(ConnectorTableMetadata tableMetadata, List<String> orcBloomFilterColumns)
    {
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());
        if (!allColumns.containsAll(orcBloomFilterColumns)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Orc bloom filter columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(orcBloomFilterColumns), allColumns)));
        }
    }

    private static void validateParquetBloomFilterColumns(ConnectorTableMetadata tableMetadata, List<String> parquetBloomFilterColumns)
    {
        Map<String, Type> columnTypes = tableMetadata.getColumns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::getType));
        for (String column : parquetBloomFilterColumns) {
            Type type = columnTypes.get(column);
            if (type == null) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Parquet Bloom filter column %s not present in schema", column));
            }
            if (!SUPPORTED_BLOOM_FILTER_TYPES.contains(type)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Parquet Bloom filter column %s has unsupported type %s", column, type.getDisplayName()));
            }
        }
    }

    public static int parseVersion(String metadataFileName)
            throws TrinoException
    {
        checkArgument(!metadataFileName.contains("/"), "Not a file name: %s", metadataFileName);
        Matcher matcher = METADATA_FILE_NAME_PATTERN.matcher(metadataFileName);
        if (matcher.matches()) {
            return parseInt(matcher.group("version"));
        }
        matcher = HADOOP_GENERATED_METADATA_FILE_NAME_PATTERN.matcher(metadataFileName);
        if (matcher.matches()) {
            return parseInt(matcher.group("version"));
        }
        throw new TrinoException(ICEBERG_BAD_DATA, "Invalid metadata file name: " + metadataFileName);
    }

    public static String fixBrokenMetadataLocation(String location)
    {
        // Version 393-394 stored metadata location with double slash https://github.com/trinodb/trino/commit/e95fdcc7d1ec110b10977d17458e06fc4e6f217d#diff-9bbb7c0b6168f0e6b4732136f9a97f820aa082b04efb5609b6138afc118831d7R46
        // e.g. s3://bucket/db/table//metadata/00001.metadata.json
        // It caused failure when accessing S3 objects https://github.com/trinodb/trino/issues/14299
        // Version 395 fixed the above issue by removing trailing slash https://github.com/trinodb/trino/pull/13984,
        // but the change was insufficient for existing table cases created by 393 and 394. This method covers existing table cases.
        String fileName = fileName(location);
        String correctSuffix = "/metadata/" + fileName;
        String brokenSuffix = "//metadata/" + fileName;
        if (!location.endsWith(brokenSuffix)) {
            return location;
        }
        return location.replaceFirst(Pattern.quote(brokenSuffix) + "$", Matcher.quoteReplacement(correctSuffix));
    }

    public static String fileName(String path)
    {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static void commit(SnapshotUpdate<?> update, ConnectorSession session)
    {
        update.set(TRINO_QUERY_ID_NAME, session.getQueryId());
        update.set(TRINO_USER_NAME, session.getUser());
        update.commit();
    }

    public static String getLatestMetadataLocation(TrinoFileSystem fileSystem, String location)
    {
        List<Location> latestMetadataLocations = new ArrayList<>();
        String metadataDirectoryLocation = format("%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME);
        try {
            int latestMetadataVersion = -1;
            FileIterator fileIterator = fileSystem.listFiles(Location.of(metadataDirectoryLocation));
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                Location fileLocation = fileEntry.location();
                String fileName = fileLocation.fileName();
                if (fileName.endsWith(METADATA_FILE_EXTENSION)) {
                    int versionNumber = parseVersion(fileName);
                    if (versionNumber > latestMetadataVersion) {
                        latestMetadataVersion = versionNumber;
                        latestMetadataLocations.clear();
                        latestMetadataLocations.add(fileLocation);
                    }
                    else if (versionNumber == latestMetadataVersion) {
                        latestMetadataLocations.add(fileLocation);
                    }
                }
            }
            if (latestMetadataLocations.isEmpty()) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, "No versioned metadata file exists at location: " + metadataDirectoryLocation);
            }
            if (latestMetadataLocations.size() > 1) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format(
                        "More than one latest metadata file found at location: %s, latest metadata files are %s",
                        metadataDirectoryLocation,
                        latestMetadataLocations));
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed checking table location: " + location, e);
        }
        return getOnlyElement(latestMetadataLocations).toString();
    }

    public static Domain getPathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle pathColumn = pathColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(pathColumn);
        if (domain == null) {
            return Domain.all(pathColumn.getType());
        }
        return domain;
    }

    public static Domain getFileModifiedTimePathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle fileModifiedTimeColumn = fileModifiedTimeColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(fileModifiedTimeColumn);
        if (domain == null) {
            return Domain.all(fileModifiedTimeColumn.getType());
        }
        return domain;
    }

    public static long getModificationTime(String path, TrinoFileSystem fileSystem)
    {
        try {
            TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path));
            return inputFile.lastModified().toEpochMilli();
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to get file modification time: " + path, e);
        }
    }

    public static Optional<IcebergPartitionColumn> getPartitionColumnType(List<PartitionField> fields, Schema schema, TypeManager typeManager)
    {
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        List<RowType.Field> partitionFields = fields.stream()
                .map(field -> RowType.field(
                        field.name(),
                        toTrinoType(field.transform().getResultType(schema.findType(field.sourceId())), typeManager)))
                .collect(toImmutableList());
        List<Integer> fieldIds = fields.stream()
                .map(PartitionField::fieldId)
                .collect(toImmutableList());
        return Optional.of(new IcebergPartitionColumn(RowType.from(partitionFields), fieldIds));
    }

    public static List<org.apache.iceberg.types.Type> partitionTypes(
            List<PartitionField> partitionFields,
            Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToPrimitiveTypeMapping)
    {
        ImmutableList.Builder<org.apache.iceberg.types.Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            org.apache.iceberg.types.Type.PrimitiveType sourceType = idToPrimitiveTypeMapping.get(partitionField.sourceId());
            org.apache.iceberg.types.Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }
}
