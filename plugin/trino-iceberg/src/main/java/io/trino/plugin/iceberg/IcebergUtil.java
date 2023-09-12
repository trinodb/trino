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
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataTableType;
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
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.io.ByteBuffers.getWrappedBytes;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnMetadata;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnMetadata;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_PARTITION_VALUE;
import static io.trino.plugin.iceberg.IcebergMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.trino.plugin.iceberg.IcebergMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getOrcBloomFilterColumns;
import static io.trino.plugin.iceberg.IcebergTableProperties.getOrcBloomFilterFpp;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getSortOrder;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
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
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.LocationProviders.locationsFor;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;

public final class IcebergUtil
{
    public static final String TRINO_TABLE_METADATA_INFO_VALID_FOR = "trino_table_metadata_info_valid_for";
    public static final String COLUMN_TRINO_NOT_NULL_PROPERTY = "trino_not_null";
    public static final String COLUMN_TRINO_TYPE_ID_PROPERTY = "trino_type_id";

    public static final String METADATA_FOLDER_NAME = "metadata";
    public static final String METADATA_FILE_EXTENSION = ".metadata.json";
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");
    static final String TRINO_QUERY_ID_NAME = "trino_query_id";
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
        String orcBloomFilterColumns = icebergTable.properties().get(ORC_BLOOM_FILTER_COLUMNS_KEY);
        if (orcBloomFilterColumns != null) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS, Splitter.on(',').trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns));
        }
        String orcBloomFilterFpp = icebergTable.properties().get(ORC_BLOOM_FILTER_FPP_KEY);
        if (orcBloomFilterFpp != null) {
            properties.put(ORC_BLOOM_FILTER_FPP, Double.parseDouble(orcBloomFilterFpp));
        }

        return properties.buildOrThrow();
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, TypeManager typeManager)
    {
        return schema.columns().stream()
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());
    }

    public static List<ColumnMetadata> getColumnMetadatas(Schema schema, TypeManager typeManager)
    {
        List<NestedField> icebergColumns = schema.columns();
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builderWithExpectedSize(icebergColumns.size() + 2);

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

    public static IcebergColumnHandle getColumnHandle(NestedField column, TypeManager typeManager)
    {
        Type type = toTrinoType(column.type(), typeManager);
        return new IcebergColumnHandle(
                createColumnIdentity(column),
                type,
                ImmutableList.of(),
                type,
                Optional.ofNullable(column.doc()));
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
        return table.specs().values().stream()
                .filter(partitionSpec -> partitionSpecIds.contains(partitionSpec.specId()))
                .allMatch(spec -> canEnforceConstraintWithinPartitioningSpec(typeOperators, spec, columnHandle, domain));
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
                            transform.getType(), InvocationConvention.simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
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
        if (!transform.isMonotonic()) {
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
        Object firstTransformed = transform.getValueTransform().apply(nativeValueToBlock(sourceType, first), 0);
        Object secondTransformed = transform.getValueTransform().apply(nativeValueToBlock(sourceType, second), 0);
        // The pushdown logic assumes NULLs and non-NULLs are segregated, so that we have to think about non-null values only.
        verify(firstTransformed != null && secondTransformed != null, "Transform for %s returned null for non-null input", field);
        try {
            return (boolean) targetTypeEqualOperator.invoke(firstTransformed, secondTransformed);
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

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

    public static LocationProvider getLocationProvider(SchemaTableName schemaTableName, String tableLocation, Map<String, String> storageProperties)
    {
        if (storageProperties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + storageProperties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }
        return locationsFor(tableLocation, storageProperties);
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

    public static Transaction newCreateTableTransaction(TrinoCatalog catalog, ConnectorTableMetadata tableMetadata, ConnectorSession session)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(tableMetadata.getProperties()));
        String targetPath = getTableLocation(tableMetadata.getProperties())
                .orElseGet(() -> catalog.defaultTableLocation(session, schemaTableName));

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        IcebergFileFormat fileFormat = IcebergTableProperties.getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toIceberg().toString());
        propertiesBuilder.put(FORMAT_VERSION, Integer.toString(IcebergTableProperties.getFormatVersion(tableMetadata.getProperties())));

        // iceberg ORC format bloom filter properties used by create table
        List<String> columns = getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (!columns.isEmpty()) {
            checkFormatForProperty(fileFormat.toIceberg(), FileFormat.ORC, ORC_BLOOM_FILTER_COLUMNS);
            validateOrcBloomFilterColumns(tableMetadata, columns);
            propertiesBuilder.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(columns));
            propertiesBuilder.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        return catalog.newCreateTableTransaction(session, schemaTableName, schema, partitionSpec, sortOrder, targetPath, propertiesBuilder.buildOrThrow());
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

    public static void validateTableCanBeDropped(Table table)
    {
        // TODO: support path override in Iceberg table creation: https://github.com/trinodb/trino/issues/8861
        if (table.properties().containsKey(OBJECT_STORE_PATH) ||
                table.properties().containsKey("write.folder-storage.path") || // Removed from Iceberg as of 0.14.0, but preserved for backward compatibility
                table.properties().containsKey(WRITE_METADATA_LOCATION) ||
                table.properties().containsKey(WRITE_DATA_LOCATION)) {
            throw new TrinoException(NOT_SUPPORTED, "Table contains Iceberg path override properties and cannot be dropped from Trino: " + table.name());
        }
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
        update.commit();
    }

    public static TableScan buildTableScan(Table icebergTable, MetadataTableType metadataTableType)
    {
        return createMetadataTableInstance(icebergTable, metadataTableType).newScan();
    }

    public static Map<String, Integer> columnNameToPositionInSchema(Schema schema)
    {
        return mapWithIndex(schema.columns().stream(),
                (column, position) -> immutableEntry(column.name(), Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }
}
