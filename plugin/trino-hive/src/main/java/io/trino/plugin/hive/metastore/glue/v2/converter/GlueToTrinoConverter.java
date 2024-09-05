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
package io.trino.plugin.hive.metastore.glue.v2.converter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.ResourceType;
import io.trino.hive.thrift.metastore.ResourceUri;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.SortingColumn.Order;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.security.PrincipalType;
import jakarta.annotation.Nullable;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.glue.v2.converter.Memoizers.memoizeLast;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.decodeFunction;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GlueToTrinoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private GlueToTrinoConverter() {}

    @SuppressModernizer // Usage of `Column.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getColumnParameters(software.amazon.awssdk.services.glue.model.Column glueColumn)
    {
        return firstNonNull(glueColumn.parameters(), ImmutableMap.of());
    }

    public static String getTableType(software.amazon.awssdk.services.glue.model.Table glueTable)
    {
        // Athena treats missing table type as EXTERNAL_TABLE.
        return firstNonNull(getTableTypeNullable(glueTable), EXTERNAL_TABLE.name());
    }

    @Nullable
    @SuppressModernizer // Usage of `Table.getTableType` is not allowed. Only this method can call that.
    public static String getTableTypeNullable(software.amazon.awssdk.services.glue.model.Table glueTable)
    {
        return glueTable.tableType();
    }

    @SuppressModernizer // Usage of `Table.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getTableParameters(software.amazon.awssdk.services.glue.model.Table glueTable)
    {
        return firstNonNull(glueTable.parameters(), ImmutableMap.of());
    }

    @SuppressModernizer // Usage of `Partition.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getPartitionParameters(software.amazon.awssdk.services.glue.model.Partition gluePartition)
    {
        return firstNonNull(gluePartition.parameters(), ImmutableMap.of());
    }

    @SuppressModernizer // Usage of `SerDeInfo.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getSerDeInfoParameters(software.amazon.awssdk.services.glue.model.SerDeInfo glueSerDeInfo)
    {
        return firstNonNull(glueSerDeInfo.parameters(), ImmutableMap.of());
    }

    public static Database convertDatabase(software.amazon.awssdk.services.glue.model.Database glueDb)
    {
        return Database.builder()
                .setDatabaseName(glueDb.name())
                // Currently it's not possible to create a Glue database with empty location string ""
                // (validation error detected: Value '' at 'database.locationUri' failed to satisfy constraint: Member must have length greater than or equal to 1)
                // However, it has been observed that Glue databases with empty location do exist in the wild.
                .setLocation(Optional.ofNullable(emptyToNull(glueDb.locationUri())))
                .setComment(Optional.ofNullable(glueDb.description()))
                .setParameters(firstNonNull(glueDb.parameters(), ImmutableMap.of()))
                .setOwnerName(Optional.of(PUBLIC_OWNER))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    public static Table convertTable(software.amazon.awssdk.services.glue.model.Table glueTable, String dbName)
    {
        SchemaTableName table = new SchemaTableName(dbName, glueTable.name());

        String tableType = getTableType(glueTable);

        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        Optional<String> description = Optional.ofNullable(glueTable.description());
        description.ifPresent(comment -> parameters.put(TABLE_COMMENT, comment));
        getTableParameters(glueTable).entrySet().stream()
                // If the description was set we may have two "comment"s, prefer the description field
                .filter(entry -> description.isEmpty() || !entry.getKey().equals(TABLE_COMMENT))
                .forEach(parameters::put);
        Map<String, String> tableParameters = parameters.buildOrThrow();

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getSchemaName())
                .setTableName(table.getTableName())
                .setOwner(Optional.ofNullable(glueTable.owner()))
                .setTableType(tableType)
                .setParameters(tableParameters)
                .setViewOriginalText(Optional.ofNullable(glueTable.viewOriginalText()))
                .setViewExpandedText(Optional.ofNullable(glueTable.viewExpandedText()));

        StorageDescriptor sd = glueTable.storageDescriptor();

        if (isIcebergTable(tableParameters) ||
                (sd == null && isDeltaLakeTable(tableParameters)) ||
                (sd == null && isTrinoMaterializedView(tableType, tableParameters))) {
            // Iceberg tables do not need to read the StorageDescriptor field, but we still need to return dummy properties for compatibility
            // Delta Lake tables only need to provide a dummy properties if a StorageDescriptor was not explicitly configured.
            // Materialized views do not need to read the StorageDescriptor, but we still need to return dummy properties for compatibility
            tableBuilder.setDataColumns(ImmutableList.of(new Column("dummy", HIVE_INT, Optional.empty(), ImmutableMap.of())));
            tableBuilder.getStorageBuilder().setStorageFormat(HiveStorageFormat.PARQUET.toStorageFormat());
        }
        else {
            if (sd == null) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table StorageDescriptor is null for table '%s' %s".formatted(table, glueTable));
            }
            if (sd.serdeInfo() == null) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table SerdeInfo is null for table '%s' %s".formatted(table, glueTable));
            }
            boolean isCsv = HiveStorageFormat.CSV.getSerde().equals(sd.serdeInfo().serializationLibrary());
            tableBuilder.setDataColumns(convertColumns(table, sd.columns(), ColumnType.DATA, isCsv));
            if (glueTable.partitionKeys() != null) {
                tableBuilder.setPartitionColumns(convertColumns(table, glueTable.partitionKeys(), ColumnType.PARTITION, isCsv));
            }
            else {
                tableBuilder.setPartitionColumns(ImmutableList.of());
            }
            // No benefit to memoizing here, just reusing the implementation
            new StorageConverter().setStorageBuilder(sd, tableBuilder.getStorageBuilder());
        }

        return tableBuilder.build();
    }

    private static Column convertColumn(SchemaTableName table, software.amazon.awssdk.services.glue.model.Column glueColumn, ColumnType columnType, boolean isCsv)
    {
        // OpenCSVSerde deserializes columns from csv file into strings, so we set the column type from the metastore
        // to string to avoid cast exceptions.
        if (columnType == ColumnType.DATA && isCsv) {
            //TODO(https://github.com/trinodb/trino/issues/7240) Add tests
            return new Column(glueColumn.name(), HiveType.HIVE_STRING, Optional.ofNullable(glueColumn.comment()), getColumnParameters(glueColumn));
        }
        return new Column(glueColumn.name(), convertType(table, glueColumn), Optional.ofNullable(glueColumn.comment()), getColumnParameters(glueColumn));
    }

    private static HiveType convertType(SchemaTableName table, software.amazon.awssdk.services.glue.model.Column column)
    {
        try {
            return HiveType.valueOf(column.type().toLowerCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Glue table '%s' column '%s' has invalid data type: %s".formatted(table, column.name(), column.type()), e);
        }
    }

    private static List<Column> convertColumns(SchemaTableName table, List<software.amazon.awssdk.services.glue.model.Column> glueColumns, ColumnType columnType, boolean isCsv)
    {
        return mappedCopy(glueColumns, glueColumn -> convertColumn(table, glueColumn, columnType, isCsv));
    }

    private static Function<Map<String, String>, Map<String, String>> parametersConverter()
    {
        return memoizeLast(ImmutableMap::copyOf);
    }

    private static boolean isNullOrEmpty(List<?> list)
    {
        return list == null || list.isEmpty();
    }

    public static final class GluePartitionConverter
            implements Function<software.amazon.awssdk.services.glue.model.Partition, Partition>
    {
        private final BiFunction<List<software.amazon.awssdk.services.glue.model.Column>, Boolean, List<Column>> dataColumnsConverter;
        private final Function<Map<String, String>, Map<String, String>> parametersConverter = parametersConverter();
        private final StorageConverter storageConverter = new StorageConverter();
        private final String databaseName;
        private final String tableName;

        public GluePartitionConverter(String databaseName, String tableName)
        {
            this.databaseName = requireNonNull(databaseName, "databaseName is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.dataColumnsConverter = memoizeLast((glueColumns, isCsv) -> convertColumns(new SchemaTableName(databaseName, tableName), glueColumns, ColumnType.DATA, isCsv));
        }

        @Override
        public Partition apply(software.amazon.awssdk.services.glue.model.Partition gluePartition)
        {
            requireNonNull(gluePartition.storageDescriptor(), "Partition StorageDescriptor is null");
            StorageDescriptor sd = gluePartition.storageDescriptor();

            if (!databaseName.equals(gluePartition.databaseName())) {
                throw new IllegalArgumentException(format("Unexpected databaseName, expected: %s, but found: %s", databaseName, gluePartition.databaseName()));
            }
            if (!tableName.equals(gluePartition.tableName())) {
                throw new IllegalArgumentException(format("Unexpected tableName, expected: %s, but found: %s", tableName, gluePartition.tableName()));
            }
            boolean isCsv = sd.serdeInfo() != null && HiveStorageFormat.CSV.getSerde().equals(sd.serdeInfo().serializationLibrary());
            Partition.Builder partitionBuilder = Partition.builder()
                    .setDatabaseName(databaseName)
                    .setTableName(tableName)
                    .setValues(gluePartition.values()) // No memoization benefit
                    .setColumns(dataColumnsConverter.apply(sd.columns(), isCsv))
                    .setParameters(parametersConverter.apply(getPartitionParameters(gluePartition)));

            storageConverter.setStorageBuilder(sd, partitionBuilder.getStorageBuilder());

            return partitionBuilder.build();
        }
    }

    private static final class StorageConverter
    {
        private final Function<List<String>, List<String>> bucketColumns = memoizeLast(ImmutableList::copyOf);
        private final Function<List<software.amazon.awssdk.services.glue.model.Order>, List<SortingColumn>> sortColumns = memoizeLast(StorageConverter::createSortingColumns);
        private final UnaryOperator<Optional<HiveBucketProperty>> bucketProperty = memoizeLast();
        private final Function<Map<String, String>, Map<String, String>> serdeParametersConverter = parametersConverter();
        private final StorageFormatConverter storageFormatConverter = new StorageFormatConverter();

        public void setStorageBuilder(StorageDescriptor sd, Storage.Builder storageBuilder)
        {
            requireNonNull(sd.serdeInfo(), "StorageDescriptor SerDeInfo is null");
            SerDeInfo serdeInfo = sd.serdeInfo();

            storageBuilder.setStorageFormat(storageFormatConverter.createStorageFormat(serdeInfo, sd))
                    .setLocation(nullToEmpty(sd.location()))
                    .setBucketProperty(convertToBucketProperty(sd))
                    .setSkewed(sd.skewedInfo() != null && !isNullOrEmpty(sd.skewedInfo().skewedColumnNames()))
                    .setSerdeParameters(serdeParametersConverter.apply(getSerDeInfoParameters(serdeInfo)))
                    .build();
        }

        private Optional<HiveBucketProperty> convertToBucketProperty(StorageDescriptor sd)
        {
            if (sd.numberOfBuckets() > 0) {
                if (isNullOrEmpty(sd.bucketColumns())) {
                    throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set");
                }
                List<String> bucketColumns = this.bucketColumns.apply(sd.bucketColumns());
                List<SortingColumn> sortedBy = this.sortColumns.apply(sd.sortColumns());
                return bucketProperty.apply(Optional.of(new HiveBucketProperty(bucketColumns, sd.numberOfBuckets(), sortedBy)));
            }
            return Optional.empty();
        }

        private static List<SortingColumn> createSortingColumns(List<software.amazon.awssdk.services.glue.model.Order> sortColumns)
        {
            if (isNullOrEmpty(sortColumns)) {
                return ImmutableList.of();
            }
            return mappedCopy(sortColumns, column -> new SortingColumn(column.column(), Order.fromMetastoreApiOrder(column.sortOrder(), "unknown")));
        }
    }

    private static final class StorageFormatConverter
    {
        private static final StorageFormat ALL_NULLS = StorageFormat.createNullable(null, null, null);
        private final UnaryOperator<String> serializationLib = memoizeLast();
        private final UnaryOperator<String> inputFormat = memoizeLast();
        private final UnaryOperator<String> outputFormat = memoizeLast();
        // Second phase to attempt memoization on the entire instance beyond just the fields
        private final UnaryOperator<StorageFormat> storageFormat = memoizeLast();

        public StorageFormat createStorageFormat(SerDeInfo serdeInfo, StorageDescriptor storageDescriptor)
        {
            String serializationLib = this.serializationLib.apply(serdeInfo.serializationLibrary());
            String inputFormat = this.inputFormat.apply(storageDescriptor.inputFormat());
            String outputFormat = this.outputFormat.apply(storageDescriptor.outputFormat());
            if (serializationLib == null && inputFormat == null && outputFormat == null) {
                return ALL_NULLS;
            }
            return this.storageFormat.apply(StorageFormat.createNullable(serializationLib, inputFormat, outputFormat));
        }
    }

    public static LanguageFunction convertFunction(UserDefinedFunction function)
    {
        List<ResourceUri> uris = mappedCopy(function.resourceUris(), uri -> new ResourceUri(ResourceType.FILE, uri.uri()));

        LanguageFunction result = decodeFunction(function.functionName(), uris);

        return new LanguageFunction(
                result.signatureToken(),
                result.sql(),
                result.path(),
                Optional.ofNullable(function.ownerName()));
    }

    public static <T, R> List<R> mappedCopy(List<T> list, Function<T, R> mapper)
    {
        requireNonNull(list, "list is null");
        requireNonNull(mapper, "mapper is null");
        //  Uses a pre-sized builder to avoid intermediate allocations and copies, which is especially significant when the
        //  number of elements is large and the size of the resulting list can be known in advance
        ImmutableList.Builder<R> builder = ImmutableList.builderWithExpectedSize(list.size());
        for (T item : list) {
            builder.add(mapper.apply(item));
        }
        return builder.build();
    }

    private enum ColumnType
    {
        DATA,
        PARTITION,
    }
}
