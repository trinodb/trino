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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_BINARY;
import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_BYTE;
import static io.trino.metastore.HiveType.HIVE_DATE;
import static io.trino.metastore.HiveType.HIVE_DOUBLE;
import static io.trino.metastore.HiveType.HIVE_FLOAT;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_SHORT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.HiveType.HIVE_TIMESTAMP;
import static io.trino.metastore.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.ARRAY_BOOLEAN_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.ARRAY_DOUBLE_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.ARRAY_INT_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.ARRAY_STRING_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.BOOLEAN_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.DATE_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.DOUBLE_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.INT_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.MAP_STRING_DATE_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.MAP_STRING_INT_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.MAP_STRING_LONG_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.STRING_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.TIMESTAMP_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.charHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.decimalHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.listHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.mapHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.structHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.varcharHiveType;

public class ResourceHudiTablesInitializer
        implements HudiTablesInitializer
{
    private static final String TEST_RESOURCE_NAME = "hudi-testing-data";

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        HudiTableUnzipper.unzipAllItemsInResource(TEST_RESOURCE_NAME);
        TrinoFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location baseLocation = externalLocation.appendSuffix(schemaName);
        copyDir(Path.of(getResource(TEST_RESOURCE_NAME).toURI()), fileSystem, baseLocation);

        for (TestingTable table : TestingTable.values()) {
            String tableName = table.getTableName();
            Location tablePath = baseLocation.appendPath(tableName);

            // Always create ro table by default
            createTable(
                    queryRunner,
                    schemaName,
                    tablePath,
                    tableName,
                    table.getDataColumns(),
                    table.getPartitionColumns(),
                    table.getPartitions(),
                    false);

            if (table.isCreateRtTable()) {
                createTable(
                        queryRunner,
                        schemaName,
                        tablePath,
                        table.getRtTableName(),
                        table.getDataColumns(),
                        table.getPartitionColumns(),
                        table.getPartitions(),
                        true);
            }
        }
    }

    /**
     * Deletes the test resource directory specified by the {@code TEST_RESOURCE_NAME} constant.
     *
     * @throws IOException if an I/O error occurs during the deletion.
     * @throws URISyntaxException if the resource URI, derived from the lookup, is invalid.
     */
    public void deleteTestResources()
            throws IOException, URISyntaxException
    {
        HudiTableUnzipper.deleteInflatedFiles(TEST_RESOURCE_NAME);
    }

    private void createTable(
            QueryRunner queryRunner,
            String schemaName,
            Location tablePath,
            String tableName,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            Map<String, String> partitions,
            boolean isRtTable)
    {
        StorageFormat roStorageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        StorageFormat rtStorageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_REALTIME_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(isRtTable ? rtStorageFormat : roStorageFormat)
                        .setLocation(tablePath.toString()))
                .build();
        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);

        List<PartitionWithStatistics> partitionsToAdd = new ArrayList<>();
        partitions.forEach((partitionName, partitionPath) -> {
            Partition partition = Partition.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setValues(extractPartitionValues(partitionName))
                    .withStorage(storageBuilder -> storageBuilder
                            .setStorageFormat(isRtTable ? rtStorageFormat : roStorageFormat)
                            .setLocation(tablePath.appendPath(partitionPath).toString()))
                    .setColumns(dataColumns)
                    .build();
            partitionsToAdd.add(new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty()));
        });
        metastore.addPartitions(schemaName, tableName, partitionsToAdd);
    }

    private static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Map.of());
    }

    public static void copyDir(Path sourceDirectory, TrinoFileSystem fileSystem, Location destinationDirectory)
            throws IOException
    {
        try (Stream<Path> paths = Files.walk(sourceDirectory)) {
            for (Iterator<Path> iterator = paths.iterator(); iterator.hasNext(); ) {
                Path path = iterator.next();
                if (path.toFile().isDirectory()) {
                    continue;
                }

                // hudi blows up if crc files are present
                if (path.toString().endsWith(".crc")) {
                    continue;
                }

                Location location = destinationDirectory.appendPath(sourceDirectory.relativize(path).toString());
                fileSystem.createDirectory(location.parentDirectory());
                try (OutputStream out = fileSystem.newOutputFile(location).create()) {
                    Files.copy(path, out);
                }
            }
        }
    }

    public enum TestingTable
    {
        HUDI_NON_PART_COW(nonPartitionRegularColumns()),
        HUDI_COW_PT_TBL(multiPartitionRegularColumns(), multiPartitionColumns(), multiPartitions(), false),
        STOCK_TICKS_COW(stockTicksRegularColumns(), stockTicksPartitionColumns(), stockTicksPartitions(), false),
        STOCK_TICKS_MOR(stockTicksRegularColumns(), stockTicksPartitionColumns(), stockTicksPartitions(), false),
        HUDI_STOCK_TICKS_COW(hudiStockTicksRegularColumns(), hudiStockTicksPartitionColumns(), hudiStockTicksPartitions(), false),
        HUDI_STOCK_TICKS_MOR(hudiStockTicksRegularColumns(), hudiStockTicksPartitionColumns(), hudiStockTicksPartitions(), false),
        HUDI_MULTI_FG_PT_MOR(hudiMultiFgRegularColumns(), hudiMultiFgPartitionsColumn(), hudiMultiFgPartitions(), false),
        HUDI_COMPREHENSIVE_TYPES_MOR(hudiComprehensiveTypesColumns(), hudiComprehensiveTypesPartitionColumns(), hudiComprehensiveTypesPartitions(), true),
        /**/;

        private static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
                new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()));

        private final List<Column> regularColumns;
        private final List<Column> partitionColumns;
        private final Map<String, String> partitions;
        private final boolean isCreateRtTable;

        TestingTable(
                List<Column> regularColumns,
                List<Column> partitionColumns,
                Map<String, String> partitions,
                boolean isCreateRtTable)
        {
            this.regularColumns = regularColumns;
            this.partitionColumns = partitionColumns;
            this.partitions = partitions;
            this.isCreateRtTable = isCreateRtTable;
        }

        TestingTable(List<Column> regularColumns)
        {
            this(regularColumns, ImmutableList.of(), ImmutableMap.of(), false);
        }

        public String getTableName()
        {
            return name().toLowerCase(Locale.ROOT);
        }

        public String getRtTableName()
        {
            return name().toLowerCase(Locale.ROOT) + "_rt";
        }

        public String getRoTableName()
        {
            // ro tables do not have suffix
            return getTableName();
        }

        public List<Column> getDataColumns()
        {
            return Stream.of(HUDI_META_COLUMNS, regularColumns)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toUnmodifiableList());
        }

        public List<Column> getPartitionColumns()
        {
            return partitionColumns;
        }

        public Map<String, String> getPartitions()
        {
            return partitions;
        }

        public boolean isCreateRtTable()
        {
            return isCreateRtTable;
        }

        private static List<Column> nonPartitionRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_LONG),
                    column("name", HIVE_STRING),
                    column("ts", HIVE_LONG),
                    column("dt", HIVE_STRING),
                    column("hh", HIVE_STRING));
        }

        private static List<Column> stockTicksRegularColumns()
        {
            return ImmutableList.of(
                    column("volume", HIVE_LONG),
                    column("ts", HIVE_STRING),
                    column("symbol", HIVE_STRING),
                    column("year", HIVE_INT),
                    column("month", HIVE_STRING),
                    column("high", HIVE_DOUBLE),
                    column("low", HIVE_DOUBLE),
                    column("key", HIVE_STRING),
                    column("date", HIVE_STRING),
                    column("close", HIVE_DOUBLE),
                    column("open", HIVE_DOUBLE),
                    column("day", HIVE_STRING));
        }

        private static List<Column> stockTicksPartitionColumns()
        {
            return ImmutableList.of(column("dt", HIVE_STRING));
        }

        private static Map<String, String> stockTicksPartitions()
        {
            return ImmutableMap.of("dt=2018-08-31", "2018/08/31");
        }

        private static List<Column> hudiStockTicksRegularColumns()
        {
            return ImmutableList.of(
                    column("volume", HIVE_LONG),
                    column("ts", HIVE_STRING),
                    column("symbol", HIVE_STRING),
                    column("year", HIVE_INT),
                    column("month", HIVE_STRING),
                    column("high", HIVE_DOUBLE),
                    column("low", HIVE_DOUBLE),
                    column("key", HIVE_STRING),
                    column("close", HIVE_DOUBLE),
                    column("open", HIVE_DOUBLE),
                    column("day", HIVE_STRING));
        }

        private static List<Column> hudiStockTicksPartitionColumns()
        {
            return ImmutableList.of(column("date", HIVE_STRING));
        }

        private static Map<String, String> hudiStockTicksPartitions()
        {
            return ImmutableMap.of("date=2018-08-31", "2018/08/31");
        }

        private static List<Column> multiPartitionRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_LONG),
                    column("name", HIVE_STRING),
                    column("ts", HIVE_LONG));
        }

        private static List<Column> multiPartitionColumns()
        {
            return ImmutableList.of(
                    column("dt", HIVE_STRING),
                    column("hh", HIVE_STRING));
        }

        private static Map<String, String> multiPartitions()
        {
            return ImmutableMap.of(
                    "dt=2021-12-09/hh=10", "dt=2021-12-09/hh=10",
                    "dt=2021-12-09/hh=11", "dt=2021-12-09/hh=11");
        }

        private static List<Column> hudiMultiFgRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_INT),
                    column("name", HIVE_STRING),
                    column("price", HIVE_DOUBLE),
                    column("ts", HIVE_LONG));
        }

        private static List<Column> hudiMultiFgPartitionsColumn()
        {
            return ImmutableList.of(
                    column("country", HIVE_STRING));
        }

        private static Map<String, String> hudiMultiFgPartitions()
        {
            return ImmutableMap.of(
                    "country=SG", "country=SG",
                    "country=US", "country=US");
        }

        private static List<Column> hudiComprehensiveTypesColumns()
        {
            return ImmutableList.of(
                    // ----- Primary Key & Precombine -----
                    column("uuid", HIVE_STRING),
                    column("precombine_field", HIVE_LONG),

                    // ----- Numeric Types -----
                    column("col_boolean", HIVE_BOOLEAN),
                    column("col_tinyint", HIVE_BYTE),
                    column("col_smallint", HIVE_SHORT),
                    column("col_int", HIVE_INT),
                    column("col_bigint", HIVE_LONG),
                    column("col_float", HIVE_FLOAT),
                    column("col_double", HIVE_DOUBLE),
                    column("col_decimal", decimalHiveType(10, 2)),

                    // ----- String Types -----
                    column("col_string", HIVE_STRING),
                    column("col_varchar", varcharHiveType(50)),
                    column("col_char", charHiveType(10)),

                    // ----- Binary Type -----
                    column("col_binary", HIVE_BINARY),

                    // ----- Datetime Types -----
                    column("col_date", HIVE_DATE),
                    column("col_timestamp", HIVE_TIMESTAMP),

                    // ----- Complex Types -----
                    // ARRAY<INT>
                    column("col_array_int", listHiveType(INT_TYPE_INFO)),
                    // ARRAY<STRING>
                    column("col_array_string", listHiveType(STRING_TYPE_INFO)),
                    // MAP<STRING, INT>
                    column("col_map_string_int", mapHiveType(STRING_TYPE_INFO, INT_TYPE_INFO)),
                    // STRUCT<f1: STRING, f2: INT, f3: BOOLEAN>
                    column("col_struct", structHiveType(
                            ImmutableList.of("f1", "f2", "f3"),
                            ImmutableList.of(STRING_TYPE_INFO, INT_TYPE_INFO, BOOLEAN_TYPE_INFO))),
                    // ARRAY<STRUCT<nested_f1: DOUBLE, nested_f2: ARRAY<STRING>>>
                    column("col_array_struct", listHiveType(
                            getStructTypeInfo(
                                    ImmutableList.of("nested_f1", "nested_f2"),
                                    ImmutableList.of(DOUBLE_TYPE_INFO, ARRAY_STRING_TYPE_INFO)))),
                    // MAP<STRING, STRUCT<nested_f3: DATE, nested_f4: DECIMAL(5,2)>>
                    column("col_map_string_struct", mapHiveType(
                            STRING_TYPE_INFO,
                            getStructTypeInfo(
                                    ImmutableList.of("nested_f3", "nested_f4"),
                                    ImmutableList.of(DATE_TYPE_INFO, getDecimalTypeInfo(5, 2))))),
                    // ARRAY<STRUCT<f_arr_struct_str: STRING, f_arr_struct_map: MAP<STRING, INT>>>
                    column("col_array_struct_with_map", listHiveType(
                            getStructTypeInfo(
                                    ImmutableList.of("f_arr_struct_str", "f_arr_struct_map"),
                                    ImmutableList.of(STRING_TYPE_INFO, MAP_STRING_INT_TYPE_INFO)))),
                    // MAP<STRING, STRUCT<f_map_struct_arr: ARRAY<BOOLEAN>, f_map_struct_ts: TIMESTAMP>>
                    column("col_map_struct_with_array", mapHiveType(
                            STRING_TYPE_INFO,
                            getStructTypeInfo(
                                    ImmutableList.of("f_map_struct_arr", "f_map_struct_ts"),
                                    ImmutableList.of(ARRAY_BOOLEAN_TYPE_INFO, TIMESTAMP_TYPE_INFO)))),
                    // STRUCT<outer_f1: INT, nested_struct: STRUCT<inner_f1: STRING, inner_f2: BOOLEAN>>
                    column("col_struct_nested_struct", structHiveType(
                            ImmutableList.of("outer_f1", "nested_struct"),
                            ImmutableList.of(
                                    INT_TYPE_INFO,
                                    getStructTypeInfo(
                                            ImmutableList.of("inner_f1", "inner_f2"),
                                            ImmutableList.of(STRING_TYPE_INFO, BOOLEAN_TYPE_INFO))))),
                    // ARRAY<ARRAY<INT>>
                    column("col_array_array_int", listHiveType(ARRAY_INT_TYPE_INFO)),
                    // MAP<STRING, ARRAY<DOUBLE>>
                    column("col_map_string_array_double", mapHiveType(STRING_TYPE_INFO, ARRAY_DOUBLE_TYPE_INFO)),
                    // MAP<STRING, MAP<STRING, DATE>>
                    column("col_map_string_map_string_date", mapHiveType(STRING_TYPE_INFO, MAP_STRING_DATE_TYPE_INFO)),
                    // STRUCT<outer_f2: STRING, struct_array: ARRAY<STRUCT<inner_f3: TIMESTAMP, inner_f4: STRING>>>
                    column("col_struct_array_struct", structHiveType(
                            ImmutableList.of("outer_f2", "struct_array"),
                            ImmutableList.of(
                                    STRING_TYPE_INFO,
                                    getListTypeInfo(getStructTypeInfo(
                                            ImmutableList.of("inner_f3", "inner_f4"),
                                            ImmutableList.of(TIMESTAMP_TYPE_INFO, STRING_TYPE_INFO)))))),
                    // STRUCT<outer_f3: BOOLEAN, struct_map: MAP<STRING, BIGINT>>
                    column("col_struct_map", structHiveType(
                            ImmutableList.of("outer_f3", "struct_map"),
                            ImmutableList.of(BOOLEAN_TYPE_INFO, MAP_STRING_LONG_TYPE_INFO))));
        }

        private static List<Column> hudiComprehensiveTypesPartitionColumns()
        {
            return ImmutableList.of(column("part_col", HIVE_STRING));
        }

        private static Map<String, String> hudiComprehensiveTypesPartitions()
        {
            return ImmutableMap.of(
                    "part_col=A", "part_col=A",
                    "part_col=B", "part_col=B");
        }
    }
}
