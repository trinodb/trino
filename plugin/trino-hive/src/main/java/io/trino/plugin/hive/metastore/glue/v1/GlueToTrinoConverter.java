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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.UserDefinedFunction;
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
import static io.trino.plugin.hive.metastore.glue.v1.Memoizers.memoizeLast;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.decodeFunction;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GlueToTrinoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private GlueToTrinoConverter() {}

    @SuppressModernizer // Usage of `Table.getStorageDescriptor` is not allowed. Only this method can call that.
    public static Optional<StorageDescriptor> getStorageDescriptor(com.amazonaws.services.glue.model.Table glueTable)
    {
        return Optional.ofNullable(glueTable.getStorageDescriptor());
    }

    @SuppressModernizer // Usage of `Column.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getColumnParameters(com.amazonaws.services.glue.model.Column glueColumn)
    {
        return firstNonNull(glueColumn.getParameters(), ImmutableMap.of());
    }

    public static String getTableType(com.amazonaws.services.glue.model.Table glueTable)
    {
        // Athena treats missing table type as EXTERNAL_TABLE.
        return firstNonNull(getTableTypeNullable(glueTable), EXTERNAL_TABLE.name());
    }

    @Nullable
    @SuppressModernizer // Usage of `Table.getTableType` is not allowed. Only this method can call that.
    public static String getTableTypeNullable(com.amazonaws.services.glue.model.Table glueTable)
    {
        return glueTable.getTableType();
    }

    @SuppressModernizer // Usage of `Table.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getTableParameters(com.amazonaws.services.glue.model.Table glueTable)
    {
        return firstNonNull(glueTable.getParameters(), ImmutableMap.of());
    }

    @SuppressModernizer // Usage of `Partition.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getPartitionParameters(com.amazonaws.services.glue.model.Partition gluePartition)
    {
        return firstNonNull(gluePartition.getParameters(), ImmutableMap.of());
    }

    @SuppressModernizer // Usage of `SerDeInfo.getParameters` is not allowed. Only this method can call that.
    public static Map<String, String> getSerDeInfoParameters(com.amazonaws.services.glue.model.SerDeInfo glueSerDeInfo)
    {
        return firstNonNull(glueSerDeInfo.getParameters(), ImmutableMap.of());
    }

    public static Database convertDatabase(com.amazonaws.services.glue.model.Database glueDb)
    {
        return Database.builder()
                .setDatabaseName(glueDb.getName())
                // Currently it's not possible to create a Glue database with empty location string ""
                // (validation error detected: Value '' at 'database.locationUri' failed to satisfy constraint: Member must have length greater than or equal to 1)
                // However, it has been observed that Glue databases with empty location do exist in the wild.
                .setLocation(Optional.ofNullable(emptyToNull(glueDb.getLocationUri())))
                .setComment(Optional.ofNullable(glueDb.getDescription()))
                .setParameters(firstNonNull(glueDb.getParameters(), ImmutableMap.of()))
                .setOwnerName(Optional.of(PUBLIC_OWNER))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    public static Table convertTable(com.amazonaws.services.glue.model.Table glueTable, String dbName)
    {
        SchemaTableName table = new SchemaTableName(dbName, glueTable.getName());

        String tableType = getTableType(glueTable);

        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        Optional<String> description = Optional.ofNullable(glueTable.getDescription());
        description.ifPresent(comment -> parameters.put(TABLE_COMMENT, comment));
        getTableParameters(glueTable).entrySet().stream()
                // If the description was set we may have two "comment"s, prefer the description field
                .filter(entry -> description.isEmpty() || !entry.getKey().equals(TABLE_COMMENT))
                .forEach(parameters::put);
        Map<String, String> tableParameters = parameters.buildOrThrow();

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getSchemaName())
                .setTableName(table.getTableName())
                .setOwner(Optional.ofNullable(glueTable.getOwner()))
                .setTableType(tableType)
                .setParameters(tableParameters)
                .setViewOriginalText(Optional.ofNullable(glueTable.getViewOriginalText()))
                .setViewExpandedText(Optional.ofNullable(glueTable.getViewExpandedText()));

        Optional<StorageDescriptor> storageDescriptor = getStorageDescriptor(glueTable);

        if (isIcebergTable(tableParameters) ||
                (storageDescriptor.isEmpty() && isTrinoMaterializedView(tableType, tableParameters))) {
            // Iceberg tables do not need to read the StorageDescriptor field, but we still need to return dummy properties for compatibility
            // Materialized views do not need to read the StorageDescriptor, but we still need to return dummy properties for compatibility
            tableBuilder.setDataColumns(ImmutableList.of(new Column("dummy", HIVE_INT, Optional.empty(), ImmutableMap.of())));
            tableBuilder.getStorageBuilder().setStorageFormat(HiveStorageFormat.PARQUET.toStorageFormat());
        }
        else if (isDeltaLakeTable(tableParameters)) {
            tableBuilder.setDataColumns(ImmutableList.of(new Column("dummy", HIVE_INT, Optional.empty(), ImmutableMap.of())));
            tableBuilder.setPartitionColumns(ImmutableList.of());
            if (storageDescriptor.isEmpty()) {
                tableBuilder.getStorageBuilder().setStorageFormat(HiveStorageFormat.PARQUET.toStorageFormat());
            }
            else {
                StorageDescriptor sd = storageDescriptor.get();
                if (sd.getSerdeInfo() == null) {
                    throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table SerdeInfo is null for table '%s' %s".formatted(table, glueTable));
                }
                new StorageConverter().setStorageBuilder(sd, tableBuilder.getStorageBuilder());
            }
        }
        else {
            if (storageDescriptor.isEmpty()) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table StorageDescriptor is null for table '%s' %s".formatted(table, glueTable));
            }
            StorageDescriptor sd = storageDescriptor.get();
            if (sd.getSerdeInfo() == null) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table SerdeInfo is null for table '%s' %s".formatted(table, glueTable));
            }
            boolean isCsv = HiveStorageFormat.CSV.getSerde().equals(sd.getSerdeInfo().getSerializationLibrary());
            tableBuilder.setDataColumns(convertColumns(table, sd.getColumns(), ColumnType.DATA, isCsv));
            if (glueTable.getPartitionKeys() != null) {
                tableBuilder.setPartitionColumns(convertColumns(table, glueTable.getPartitionKeys(), ColumnType.PARTITION, isCsv));
            }
            else {
                tableBuilder.setPartitionColumns(ImmutableList.of());
            }
            // No benefit to memoizing here, just reusing the implementation
            new StorageConverter().setStorageBuilder(sd, tableBuilder.getStorageBuilder());
        }

        return tableBuilder.build();
    }

    private static Column convertColumn(SchemaTableName table, com.amazonaws.services.glue.model.Column glueColumn, ColumnType columnType, boolean isCsv)
    {
        // OpenCSVSerde deserializes columns from csv file into strings, so we set the column type from the metastore
        // to string to avoid cast exceptions.
        if (columnType == ColumnType.DATA && isCsv) {
            //TODO(https://github.com/trinodb/trino/issues/7240) Add tests
            return new Column(glueColumn.getName(), HiveType.HIVE_STRING, Optional.ofNullable(glueColumn.getComment()), getColumnParameters(glueColumn));
        }
        return new Column(glueColumn.getName(), convertType(table, glueColumn), Optional.ofNullable(glueColumn.getComment()), getColumnParameters(glueColumn));
    }

    private static HiveType convertType(SchemaTableName table, com.amazonaws.services.glue.model.Column column)
    {
        try {
            return HiveType.valueOf(column.getType().toLowerCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Glue table '%s' column '%s' has invalid data type: %s".formatted(table, column.getName(), column.getType()), e);
        }
    }

    private static List<Column> convertColumns(SchemaTableName table, List<com.amazonaws.services.glue.model.Column> glueColumns, ColumnType columnType, boolean isCsv)
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
            implements Function<com.amazonaws.services.glue.model.Partition, Partition>
    {
        private final BiFunction<List<com.amazonaws.services.glue.model.Column>, Boolean, List<Column>> dataColumnsConverter;
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
        public Partition apply(com.amazonaws.services.glue.model.Partition gluePartition)
        {
            requireNonNull(gluePartition.getStorageDescriptor(), "Partition StorageDescriptor is null");
            StorageDescriptor sd = gluePartition.getStorageDescriptor();

            if (!databaseName.equals(gluePartition.getDatabaseName())) {
                throw new IllegalArgumentException(format("Unexpected databaseName, expected: %s, but found: %s", databaseName, gluePartition.getDatabaseName()));
            }
            if (!tableName.equals(gluePartition.getTableName())) {
                throw new IllegalArgumentException(format("Unexpected tableName, expected: %s, but found: %s", tableName, gluePartition.getTableName()));
            }
            boolean isCsv = sd.getSerdeInfo() != null && HiveStorageFormat.CSV.getSerde().equals(sd.getSerdeInfo().getSerializationLibrary());
            Partition.Builder partitionBuilder = Partition.builder()
                    .setDatabaseName(databaseName)
                    .setTableName(tableName)
                    .setValues(gluePartition.getValues()) // No memoization benefit
                    .setColumns(dataColumnsConverter.apply(sd.getColumns(), isCsv))
                    .setParameters(parametersConverter.apply(getPartitionParameters(gluePartition)));

            storageConverter.setStorageBuilder(sd, partitionBuilder.getStorageBuilder());

            return partitionBuilder.build();
        }
    }

    private static final class StorageConverter
    {
        private final Function<List<String>, List<String>> bucketColumns = memoizeLast(ImmutableList::copyOf);
        private final Function<List<com.amazonaws.services.glue.model.Order>, List<SortingColumn>> sortColumns = memoizeLast(StorageConverter::createSortingColumns);
        private final UnaryOperator<Optional<HiveBucketProperty>> bucketProperty = memoizeLast();
        private final Function<Map<String, String>, Map<String, String>> serdeParametersConverter = parametersConverter();
        private final StorageFormatConverter storageFormatConverter = new StorageFormatConverter();

        public void setStorageBuilder(StorageDescriptor sd, Storage.Builder storageBuilder)
        {
            requireNonNull(sd.getSerdeInfo(), "StorageDescriptor SerDeInfo is null");
            SerDeInfo serdeInfo = sd.getSerdeInfo();

            storageBuilder.setStorageFormat(storageFormatConverter.createStorageFormat(serdeInfo, sd))
                    .setLocation(nullToEmpty(sd.getLocation()))
                    .setBucketProperty(convertToBucketProperty(sd))
                    .setSkewed(sd.getSkewedInfo() != null && !isNullOrEmpty(sd.getSkewedInfo().getSkewedColumnNames()))
                    .setSerdeParameters(serdeParametersConverter.apply(getSerDeInfoParameters(serdeInfo)))
                    .build();
        }

        private Optional<HiveBucketProperty> convertToBucketProperty(StorageDescriptor sd)
        {
            if (sd.getNumberOfBuckets() > 0) {
                if (isNullOrEmpty(sd.getBucketColumns())) {
                    throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set");
                }
                List<String> bucketColumns = this.bucketColumns.apply(sd.getBucketColumns());
                List<SortingColumn> sortedBy = this.sortColumns.apply(sd.getSortColumns());
                return bucketProperty.apply(Optional.of(new HiveBucketProperty(bucketColumns, sd.getNumberOfBuckets(), sortedBy)));
            }
            return Optional.empty();
        }

        private static List<SortingColumn> createSortingColumns(List<com.amazonaws.services.glue.model.Order> sortColumns)
        {
            if (isNullOrEmpty(sortColumns)) {
                return ImmutableList.of();
            }
            return mappedCopy(sortColumns, column -> new SortingColumn(column.getColumn(), Order.fromMetastoreApiOrder(column.getSortOrder(), "unknown")));
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
            String serializationLib = this.serializationLib.apply(serdeInfo.getSerializationLibrary());
            String inputFormat = this.inputFormat.apply(storageDescriptor.getInputFormat());
            String outputFormat = this.outputFormat.apply(storageDescriptor.getOutputFormat());
            if (serializationLib == null && inputFormat == null && outputFormat == null) {
                return ALL_NULLS;
            }
            return this.storageFormat.apply(StorageFormat.createNullable(serializationLib, inputFormat, outputFormat));
        }
    }

    public static LanguageFunction convertFunction(UserDefinedFunction function)
    {
        List<ResourceUri> uris = mappedCopy(function.getResourceUris(), uri -> new ResourceUri(ResourceType.FILE, uri.getUri()));

        LanguageFunction result = decodeFunction(function.getFunctionName(), uris);

        return new LanguageFunction(
                result.signatureToken(),
                result.sql(),
                result.path(),
                Optional.ofNullable(function.getOwnerName()));
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
