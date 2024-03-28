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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.hive.thrift.metastore.ResourceType;
import io.trino.hive.thrift.metastore.ResourceUri;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.type.PrimitiveCategory;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.security.PrincipalType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalNumber;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.lenientFormat;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static io.trino.plugin.hive.metastore.MetastoreUtil.metastoreFunctionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toResourceUris;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.decodeFunction;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreNullsCount;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static java.util.Objects.requireNonNull;

final class GlueConverter
{
    static final String PUBLIC_OWNER = "PUBLIC";
    private static final Storage FAKE_PARQUET_STORAGE = new Storage(
            StorageFormat.create(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
            Optional.empty(),
            Optional.empty(),
            false,
            ImmutableMap.of());
    private static final Column FAKE_COLUMN = new Column("ignored", HiveType.HIVE_INT, Optional.empty(), ImmutableMap.of());
    private static final long SECONDS_PER_DAY = TimeUnit.DAYS.toSeconds(1);

    private static final JsonCodec<LanguageFunction> LANGUAGE_FUNCTION_CODEC = JsonCodec.jsonCodec(LanguageFunction.class);

    private GlueConverter() {}

    public static Database fromGlueDatabase(software.amazon.awssdk.services.glue.model.Database glueDb)
    {
        return new Database(
                glueDb.name(),
                Optional.ofNullable(emptyToNull(glueDb.locationUri())),
                Optional.of(PUBLIC_OWNER),
                Optional.of(PrincipalType.ROLE),
                Optional.ofNullable(glueDb.description()),
                glueDb.parameters());
    }

    public static DatabaseInput toGlueDatabaseInput(Database database)
    {
        return DatabaseInput.builder()
                .name(database.getDatabaseName())
                .parameters(database.getParameters())
                .description(database.getComment().orElse(null))
                .locationUri(database.getLocation().orElse(null))
                .build();
    }

    public static Table fromGlueTable(software.amazon.awssdk.services.glue.model.Table glueTable, String databaseName)
    {
        // Athena treats a missing table type as EXTERNAL_TABLE.
        String tableType = firstNonNull(glueTable.tableType(), "EXTERNAL_TABLE");

        Map<String, String> tableParameters = glueTable.parameters();
        if (glueTable.description() != null) {
            // Glue description overrides the comment field in the parameters
            tableParameters = new LinkedHashMap<>(tableParameters);
            tableParameters.put(TABLE_COMMENT, glueTable.description());
        }

        Storage storage;
        List<Column> partitionColumns;
        List<Column> dataColumns;
        StorageDescriptor sd = glueTable.storageDescriptor();
        if (sd == null) {
            if (!isIcebergTable(tableParameters) && !isDeltaLakeTable(tableParameters) && !isTrinoMaterializedView(tableType, tableParameters)) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table StorageDescriptor is null for table '%s' %s".formatted(databaseName, glueTable.name()));
            }

            dataColumns = ImmutableList.of(FAKE_COLUMN);
            partitionColumns = ImmutableList.of();
            storage = FAKE_PARQUET_STORAGE;
        }
        else if (isIcebergTable(tableParameters)) {
            // todo: any reason to not do this for delta and trino mv?
            if (sd.columns() == null) {
                dataColumns = ImmutableList.of(FAKE_COLUMN);
            }
            else {
                dataColumns = fromGlueColumns(sd.columns(), ColumnType.DATA, false);
            }
            partitionColumns = ImmutableList.of();
            storage = FAKE_PARQUET_STORAGE;
        }
        else {
            boolean isCsv = sd.serdeInfo() != null && HiveStorageFormat.CSV.getSerde().equals(sd.serdeInfo().serializationLibrary());
            dataColumns = fromGlueColumns(sd.columns(), ColumnType.DATA, isCsv);
            if (glueTable.partitionKeys() != null) {
                partitionColumns = fromGlueColumns(glueTable.partitionKeys(), ColumnType.PARTITION, isCsv);
            }
            else {
                partitionColumns = ImmutableList.of();
            }
            storage = fromGlueStorage(sd, databaseName + "." + glueTable.name());
        }

        return new Table(
                databaseName,
                glueTable.name(),
                Optional.ofNullable(glueTable.owner()),
                tableType,
                storage,
                dataColumns,
                partitionColumns,
                tableParameters,
                Optional.ofNullable(glueTable.viewOriginalText()),
                Optional.ofNullable(glueTable.viewExpandedText()),
                OptionalLong.empty());
    }

    public static TableInput toGlueTableInput(Table table)
    {
        Map<String, String> tableParameters = table.getParameters();
        Optional<String> comment = Optional.empty();
        if (!isTrinoView(table.getTableType(), table.getParameters()) && !isTrinoMaterializedView(table.getTableType(), table.getParameters())) {
            comment = Optional.ofNullable(tableParameters.get(TABLE_COMMENT));
            tableParameters = tableParameters.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(TABLE_COMMENT))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        return TableInput.builder()
                .name(table.getTableName())
                .owner(table.getOwner().orElse(null))
                .tableType(table.getTableType())
                .storageDescriptor(toGlueStorage(table.getStorage(), table.getDataColumns()))
                .partitionKeys(table.getPartitionColumns().stream().map(GlueConverter::toGlueColumn).collect(toImmutableList()))
                .parameters(tableParameters)
                .viewOriginalText(table.getViewOriginalText().orElse(null))
                .viewExpandedText(table.getViewExpandedText().orElse(null))
                .description(comment.orElse(null))
                .build();
    }

    public static Partition fromGluePartition(String databaseName, String tableName, software.amazon.awssdk.services.glue.model.Partition gluePartition)
    {
        requireNonNull(gluePartition.storageDescriptor(), "Partition StorageDescriptor is null");

        if (!databaseName.equals(gluePartition.databaseName())) {
            throw new IllegalArgumentException("Unexpected databaseName, expected: %s, but found: %s".formatted(databaseName, gluePartition.databaseName()));
        }
        if (!tableName.equals(gluePartition.tableName())) {
            throw new IllegalArgumentException("Unexpected tableName, expected: %s, but found: %s".formatted(tableName, gluePartition.tableName()));
        }

        StorageDescriptor sd = gluePartition.storageDescriptor();
        boolean isCsv = sd.serdeInfo() != null && HiveStorageFormat.CSV.getSerde().equals(sd.serdeInfo().serializationLibrary());
        List<String> partitionName = gluePartition.values();
        return new Partition(
                databaseName,
                tableName,
                partitionName,
                fromGlueStorage(sd, databaseName + "." + tableName + "/" + partitionName),
                fromGlueColumns(sd.columns(), ColumnType.DATA, isCsv),
                gluePartition.parameters());
    }

    public static PartitionInput toGluePartitionInput(Partition partition)
    {
        return PartitionInput.builder()
                .values(partition.getValues())
                .storageDescriptor(toGlueStorage(partition.getStorage(), partition.getColumns()))
                .parameters(partition.getParameters())
                .build();
    }

    private static List<Column> fromGlueColumns(List<software.amazon.awssdk.services.glue.model.Column> glueColumns, ColumnType columnType, boolean isCsv)
    {
        return glueColumns.stream()
                .map(glueColumn -> fromGlueColumn(glueColumn, columnType, isCsv))
                .collect(toImmutableList());
    }

    private static Column fromGlueColumn(software.amazon.awssdk.services.glue.model.Column glueColumn, ColumnType columnType, boolean isCsv)
    {
        // OpenCSVSerde deserializes columns from csv file into strings, so we set the column type from the metastore
        // to string to avoid cast exceptions.
        if (columnType == ColumnType.DATA && isCsv) {
            //TODO(https://github.com/trinodb/trino/issues/7240) Add tests
            return new Column(glueColumn.name(), HiveType.HIVE_STRING, Optional.ofNullable(glueColumn.comment()), glueColumn.parameters());
        }
        return new Column(glueColumn.name(), HiveType.valueOf(glueColumn.type().toLowerCase(Locale.ROOT)), Optional.ofNullable(glueColumn.comment()), glueColumn.parameters());
    }

    private static software.amazon.awssdk.services.glue.model.Column toGlueColumn(Column trinoColumn)
    {
        return software.amazon.awssdk.services.glue.model.Column.builder()
                .name(trinoColumn.getName())
                .type(trinoColumn.getType().toString())
                .comment(trinoColumn.getComment().orElse(null))
                .parameters(trinoColumn.getProperties())
                .build();
    }

    private static Storage fromGlueStorage(StorageDescriptor sd, String tablePartitionName)
    {
        Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        if (sd.numberOfBuckets() > 0) {
            if (sd.bucketColumns().isEmpty()) {
                throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: " + tablePartitionName);
            }

            List<SortingColumn> sortBy = ImmutableList.of();
            if (!sd.sortColumns().isEmpty()) {
                sortBy = sd.sortColumns().stream()
                        .map(order -> new SortingColumn(order.column(), fromGlueSortOrder(order.sortOrder(), tablePartitionName)))
                        .collect(toImmutableList());
            }

            bucketProperty = Optional.of(new HiveBucketProperty(sd.bucketColumns(), sd.numberOfBuckets(), sortBy));
        }

        SerDeInfo serdeInfo = requireNonNull(sd.serdeInfo(), () -> "StorageDescriptor SerDeInfo is null: " + tablePartitionName);
        return new Storage(
                StorageFormat.createNullable(serdeInfo.serializationLibrary(), sd.inputFormat(), sd.outputFormat()),
                Optional.ofNullable(sd.location()),
                bucketProperty,
                sd.skewedInfo() != null && !sd.skewedInfo().skewedColumnNames().isEmpty(),
                serdeInfo.parameters());
    }

    private static StorageDescriptor toGlueStorage(Storage storage, List<Column> columns)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = SerDeInfo.builder()
                .serializationLibrary(storage.getStorageFormat().getSerDeNullable())
                .parameters(storage.getSerdeParameters())
                .build();

        StorageDescriptor.Builder builder = StorageDescriptor.builder()
                .location(storage.getLocation())
                .columns(columns.stream().map(GlueConverter::toGlueColumn).collect(toImmutableList()))
                .serdeInfo(serdeInfo)
                .inputFormat(storage.getStorageFormat().getInputFormatNullable())
                .outputFormat(storage.getStorageFormat().getOutputFormatNullable())
                .parameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            builder.numberOfBuckets(bucketProperty.get().getBucketCount());
            builder.bucketColumns(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                builder.sortColumns(bucketProperty.get().getSortedBy().stream()
                        .map(GlueConverter::toGlueSortOrder)
                        .collect(toImmutableList()));
            }
        }

        return builder.build();
    }

    private static SortingColumn.Order fromGlueSortOrder(Integer value, String tablePartitionName)
    {
        if (value == 0) {
            return SortingColumn.Order.DESCENDING;
        }
        if (value == 1) {
            return SortingColumn.Order.ASCENDING;
        }
        throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has invalid sorting order: " + tablePartitionName);
    }

    private static Order toGlueSortOrder(SortingColumn column)
    {
        return Order.builder()
                .column(column.getColumnName())
                .sortOrder(switch (column.getOrder()) {
                    case ASCENDING -> 1;
                    case DESCENDING -> 0;
                })
                .build();
    }

    public static Map<String, HiveColumnStatistics> fromGlueStatistics(List<List<software.amazon.awssdk.services.glue.model.ColumnStatistics>> glueColumnStatistics)
    {
        ImmutableMap.Builder<String, HiveColumnStatistics> columnStatistics = ImmutableMap.builder();
        for (var columns : glueColumnStatistics) {
            for (var column : columns) {
                fromGlueColumnStatistics(column.statisticsData())
                        .ifPresent(stats -> columnStatistics.put(column.columnName(), stats));
            }
        }
        return columnStatistics.buildOrThrow();
    }

    private static Optional<HiveColumnStatistics> fromGlueColumnStatistics(ColumnStatisticsData catalogColumnStatisticsData)
    {
        return switch (catalogColumnStatisticsData.type()) {
            case BINARY -> {
                BinaryColumnStatisticsData data = catalogColumnStatisticsData.binaryColumnStatisticsData();
                yield Optional.of(createBinaryColumnStatistics(
                        OptionalLong.of(data.maximumLength()),
                        OptionalDouble.of(data.averageLength()),
                        fromMetastoreNullsCount(data.numberOfNulls())));
            }
            case BOOLEAN -> {
                BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.booleanColumnStatisticsData();
                yield Optional.of(createBooleanColumnStatistics(
                        OptionalLong.of(catalogBooleanData.numberOfTrues()),
                        OptionalLong.of(catalogBooleanData.numberOfFalses()),
                        fromMetastoreNullsCount(catalogBooleanData.numberOfNulls())));
            }
            case DATE -> {
                DateColumnStatisticsData data = catalogColumnStatisticsData.dateColumnStatisticsData();
                yield Optional.of(createDateColumnStatistics(
                        dateToLocalDate(data.minimumValue()),
                        dateToLocalDate(data.maximumValue()),
                        fromMetastoreNullsCount(data.numberOfNulls()),
                        OptionalLong.of(data.numberOfDistinctValues())));
            }
            case DECIMAL -> {
                DecimalColumnStatisticsData data = catalogColumnStatisticsData.decimalColumnStatisticsData();
                yield Optional.of(createDecimalColumnStatistics(
                        fromGlueDecimal(data.minimumValue()),
                        fromGlueDecimal(data.maximumValue()),
                        fromMetastoreNullsCount(data.numberOfNulls()),
                        OptionalLong.of(data.numberOfDistinctValues())));
            }
            case DOUBLE -> {
                DoubleColumnStatisticsData data = catalogColumnStatisticsData.doubleColumnStatisticsData();
                yield Optional.of(createDoubleColumnStatistics(
                        OptionalDouble.of(data.minimumValue()),
                        OptionalDouble.of(data.maximumValue()),
                        fromMetastoreNullsCount(data.numberOfNulls()),
                        OptionalLong.of(data.numberOfDistinctValues())));
            }
            case LONG -> {
                LongColumnStatisticsData data = catalogColumnStatisticsData.longColumnStatisticsData();
                yield Optional.of(createIntegerColumnStatistics(
                        OptionalLong.of(data.minimumValue()),
                        OptionalLong.of(data.maximumValue()),
                        fromMetastoreNullsCount(data.numberOfNulls()),
                        OptionalLong.of(data.numberOfDistinctValues())));
            }
            case STRING -> {
                StringColumnStatisticsData data = catalogColumnStatisticsData.stringColumnStatisticsData();
                yield Optional.of(createStringColumnStatistics(
                        OptionalLong.of(data.maximumLength()),
                        OptionalDouble.of(data.averageLength()),
                        fromMetastoreNullsCount(data.numberOfNulls()),
                        OptionalLong.of(data.numberOfDistinctValues())));
            }
            case UNKNOWN_TO_SDK_VERSION -> Optional.empty();
        };
    }

    public static List<software.amazon.awssdk.services.glue.model.ColumnStatistics> toGlueColumnStatistics(Map<Column, HiveColumnStatistics> columnStatistics)
    {
        return columnStatistics.entrySet().stream()
                .map(e -> toGlueColumnStatistics(e.getKey(), e.getValue()))
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    private static Optional<software.amazon.awssdk.services.glue.model.ColumnStatistics> toGlueColumnStatistics(Column column, HiveColumnStatistics statistics)
    {
        return toGlueColumnStatisticsData(statistics, column.getType())
                .map(columnStatisticsData -> software.amazon.awssdk.services.glue.model.ColumnStatistics.builder()
                        .columnName(column.getName())
                        .columnType(column.getType().toString())
                        .statisticsData(columnStatisticsData)
                        .analyzedTime(Instant.now())
                        .build());
    }

    private static Optional<ColumnStatisticsData> toGlueColumnStatisticsData(HiveColumnStatistics statistics, HiveType columnType)
    {
        if (!isGlueWritable(statistics)) {
            return Optional.empty();
        }

        if (statistics.getBooleanStatistics().isPresent()) {
            BooleanStatistics booleanStatistics = statistics.getBooleanStatistics().get();
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.BOOLEAN)
                    .booleanColumnStatisticsData(builder -> builder
                            .numberOfTrues(boxedValue(booleanStatistics.getTrueCount()))
                            .numberOfFalses(boxedValue(booleanStatistics.getFalseCount()))
                            .numberOfNulls(boxedValue(statistics.getNullsCount())))
                    .build());
        }
        if (statistics.getDateStatistics().isPresent()) {
            DateStatistics dateStatistics = statistics.getDateStatistics().get();
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.DATE)
                    .dateColumnStatisticsData(builder -> builder
                            .minimumValue(dateStatistics.getMin().map(GlueConverter::localDateToDate).orElse(null))
                            .maximumValue(dateStatistics.getMax().map(GlueConverter::localDateToDate).orElse(null))
                            .numberOfNulls(boxedValue(statistics.getNullsCount()))
                            .numberOfDistinctValues(boxedValue(statistics.getDistinctValuesWithNullCount())))
                    .build());
        }
        if (statistics.getDecimalStatistics().isPresent()) {
            DecimalStatistics decimalStatistics = statistics.getDecimalStatistics().get();
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.DECIMAL)
                    .decimalColumnStatisticsData(builder -> builder
                            .minimumValue(toGlueDecimal(decimalStatistics.getMin()))
                            .maximumValue(toGlueDecimal(decimalStatistics.getMax()))
                            .numberOfNulls(boxedValue(statistics.getNullsCount()))
                            .numberOfDistinctValues(boxedValue(statistics.getDistinctValuesWithNullCount())))
                    .build());
        }
        if (statistics.getDoubleStatistics().isPresent()) {
            DoubleStatistics doubleStatistics = statistics.getDoubleStatistics().get();
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.DOUBLE)
                    .doubleColumnStatisticsData(builder -> builder
                            .minimumValue(boxedValue(doubleStatistics.getMin()))
                            .maximumValue(boxedValue(doubleStatistics.getMax()))
                            .numberOfNulls(boxedValue(statistics.getNullsCount()))
                            .numberOfDistinctValues(boxedValue(statistics.getDistinctValuesWithNullCount())))
                    .build());
        }
        if (statistics.getIntegerStatistics().isPresent()) {
            IntegerStatistics integerStatistics = statistics.getIntegerStatistics().get();
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.LONG)
                    .longColumnStatisticsData(builder -> builder
                            .minimumValue(boxedValue(integerStatistics.getMin()))
                            .maximumValue(boxedValue(integerStatistics.getMax()))
                            .numberOfNulls(boxedValue(statistics.getNullsCount()))
                            .numberOfDistinctValues(boxedValue(statistics.getDistinctValuesWithNullCount())))
                    .build());
        }

        TypeInfo typeInfo = columnType.getTypeInfo();
        if (!(typeInfo instanceof PrimitiveTypeInfo primitiveTypeInfo)) {
            throw new IllegalArgumentException(lenientFormat("Unsupported statistics type: %s", columnType));
        }
        PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();

        if (PrimitiveCategory.BINARY == primitiveCategory) {
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.BINARY)
                    .binaryColumnStatisticsData(builder -> builder
                            .maximumLength(statistics.getMaxValueSizeInBytes().orElse(0))
                            .numberOfNulls(boxedValue(statistics.getNullsCount()))
                            .averageLength(boxedValue(statistics.getAverageColumnLength())))
                    .build());
        }
        if (PrimitiveCategory.VARCHAR == primitiveCategory || (PrimitiveCategory.CHAR == primitiveCategory) || (PrimitiveCategory.STRING == primitiveCategory)) {
            OptionalLong distinctValuesCount = statistics.getDistinctValuesWithNullCount();
            return Optional.ofNullable(ColumnStatisticsData.builder()
                    .type(ColumnStatisticsType.STRING)
                    .stringColumnStatisticsData(builder -> builder
                            .numberOfNulls(boxedValue(statistics.getNullsCount()))
                            .numberOfDistinctValues(boxedValue(distinctValuesCount))
                            .maximumLength(statistics.getMaxValueSizeInBytes().orElse(0))
                            .averageLength(boxedValue(statistics.getAverageColumnLength())))
                    .build());
        }
        return Optional.empty();
    }

    // Glue will accept null as min/max values, but return 0 when reading
    // to avoid incorrect statistics we skip writes for columns that have min/max null
    // this can be removed once Glue fixes this behavior
    private static boolean isGlueWritable(HiveColumnStatistics columnStatistics)
    {
        if (columnStatistics.getDateStatistics().isPresent()) {
            DateStatistics dateStatistics = columnStatistics.getDateStatistics().get();
            return dateStatistics.getMin().isPresent() && dateStatistics.getMax().isPresent();
        }
        if (columnStatistics.getDecimalStatistics().isPresent()) {
            DecimalStatistics decimalStatistics = columnStatistics.getDecimalStatistics().get();
            return decimalStatistics.getMin().isPresent() && decimalStatistics.getMax().isPresent();
        }
        if (columnStatistics.getDoubleStatistics().isPresent()) {
            DoubleStatistics doubleStatistics = columnStatistics.getDoubleStatistics().get();
            return doubleStatistics.getMin().isPresent() && doubleStatistics.getMax().isPresent();
        }
        if (columnStatistics.getIntegerStatistics().isPresent()) {
            IntegerStatistics integerStatistics = columnStatistics.getIntegerStatistics().get();
            return integerStatistics.getMin().isPresent() && integerStatistics.getMax().isPresent();
        }
        return true;
    }

    private static Long boxedValue(OptionalLong optionalLong)
    {
        return optionalLong.isPresent() ? optionalLong.getAsLong() : null;
    }

    private static Double boxedValue(OptionalDouble optionalDouble)
    {
        return optionalDouble.isPresent() ? optionalDouble.getAsDouble() : null;
    }

    private static Optional<BigDecimal> fromGlueDecimal(DecimalNumber number)
    {
        if (number == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(number.unscaledValue().asByteArray()), number.scale()));
    }

    private static DecimalNumber toGlueDecimal(Optional<BigDecimal> optionalDecimal)
    {
        if (optionalDecimal.isEmpty()) {
            return null;
        }
        BigDecimal decimal = optionalDecimal.get();
        return DecimalNumber.builder()
                .unscaledValue(SdkBytes.fromByteArray(decimal.unscaledValue().toByteArray()))
                .scale(decimal.scale())
                .build();
    }

    private static Optional<LocalDate> dateToLocalDate(Instant date)
    {
        if (date == null) {
            return Optional.empty();
        }
        long daysSinceEpoch = date.getEpochSecond() / SECONDS_PER_DAY;
        return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch));
    }

    private static Instant localDateToDate(LocalDate date)
    {
        long secondsSinceEpoch = date.toEpochDay() * SECONDS_PER_DAY;
        return Instant.ofEpochSecond(secondsSinceEpoch);
    }

    public static LanguageFunction fromGlueFunction(UserDefinedFunction function)
    {
        List<ResourceUri> uris = function.resourceUris().stream()
                .map(uri -> new ResourceUri(ResourceType.FILE, uri.uri()))
                .collect(toImmutableList());

        LanguageFunction result = decodeFunction(function.functionName(), uris);

        return new LanguageFunction(
                result.signatureToken(),
                result.sql(),
                result.path(),
                Optional.ofNullable(function.ownerName()));
    }

    public static UserDefinedFunctionInput toGlueFunctionInput(String functionName, LanguageFunction function)
    {
        return UserDefinedFunctionInput.builder()
                .functionName(metastoreFunctionName(functionName, function.signatureToken()))
                .className("TrinoFunction")
                .ownerType(software.amazon.awssdk.services.glue.model.PrincipalType.USER)
                .ownerName(function.owner().orElse(null))
                .resourceUris(toResourceUris(LANGUAGE_FUNCTION_CODEC.toJsonBytes(function)).stream()
                        .map(uri -> software.amazon.awssdk.services.glue.model.ResourceUri.builder()
                                .resourceType(software.amazon.awssdk.services.glue.model.ResourceType.FILE)
                                .uri(uri.getUri())
                                .build())
                        .toList())
                .build();
    }

    private enum ColumnType
    {
        DATA,
        PARTITION,
    }
}
