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
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
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
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.util.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;

public class ResourceHudiTablesInitializer
        implements HudiTablesInitializer
{
    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        TrinoFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location baseLocation = externalLocation.appendSuffix(schemaName);
        copyDir(new File(getResource("hudi-testing-data").toURI()).toPath(), fileSystem, baseLocation);

        for (TestingTable table : TestingTable.values()) {
            String tableName = table.getTableName();
            Location tablePath = baseLocation.appendPath(tableName);
            createTable(
                    queryRunner,
                    schemaName,
                    tablePath,
                    tableName,
                    table.getDataColumns(),
                    table.getPartitionColumns(),
                    table.getPartitions());
        }
    }

    private void createTable(
            QueryRunner queryRunner,
            String schemaName,
            Location tablePath,
            String tableName,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            Map<String, String> partitions)
    {
        StorageFormat storageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
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
                        .setStorageFormat(storageFormat)
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
                            .setStorageFormat(storageFormat)
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
        HUDI_COW_PT_TBL(multiPartitionRegularColumns(), multiPartitionColumns(), multiPartitions()),
        STOCK_TICKS_COW(stockTicksRegularColumns(), stockTicksPartitionColumns(), stockTicksPartitions()),
        STOCK_TICKS_MOR(stockTicksRegularColumns(), stockTicksPartitionColumns(), stockTicksPartitions()),
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

        TestingTable(
                List<Column> regularColumns,
                List<Column> partitionColumns,
                Map<String, String> partitions)
        {
            this.regularColumns = regularColumns;
            this.partitionColumns = partitionColumns;
            this.partitions = partitions;
        }

        TestingTable(List<Column> regularColumns)
        {
            this(regularColumns, ImmutableList.of(), ImmutableMap.of());
        }

        public String getTableName()
        {
            return name().toLowerCase(Locale.ROOT);
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
    }
}
