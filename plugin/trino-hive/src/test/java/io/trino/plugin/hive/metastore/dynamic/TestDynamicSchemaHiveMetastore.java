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
package io.trino.plugin.hive.metastore.dynamic;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.hive.formats.line.protobuf.ProtobufDeserializerFactory;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE_PROTOBUF;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicSchemaHiveMetastore
{
    private static String databaseName = "test_database_" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    private static String tableName = "test_table";
    private static Path tempDir;
    private static HiveMetastore metastore;

    @BeforeAll
    static void beforeAll()
            throws Exception
    {
        tempDir = createTempDirectory("test");
        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(tempDir);

        HiveMetastore delegate = new FileHiveMetastore(
                new NodeVersion("testversion"),
                fileSystemFactory,
                false,
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local:///")
                        .setMetastoreUser("test")
                        .setDisableLocationChecks(true));

        ProtobufDeserializerFactory protobufDeserializerFactory = new ProtobufDeserializerFactory(Path.of(TestDynamicSchemaHiveMetastore.class.getResource("/protobuf/descriptors").toURI()), Duration.valueOf("1h"), 10);
        Duration dynamicSchemaCacheExpiration = Duration.valueOf("1h");
        metastore = new DynamicSchemaHiveMetastore(delegate, protobufDeserializerFactory, dynamicSchemaCacheExpiration);

        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        metastore.createDatabase(database.build());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setTableType(MANAGED_TABLE.name())
                .setDataColumns(List.of()) // No columns needed, they are inferred from the protobuf
                .setOwner(Optional.empty())
                .setPartitionColumns(ImmutableList.of(new Column("year", HIVE_STRING, Optional.empty(), Map.of())));

        tableBuilder.getStorageBuilder()
                //.setLocation(Optional.of("/tmp/location"))
                .setStorageFormat(SEQUENCEFILE_PROTOBUF.toStorageFormat())
                .setSerdeParameters(Map.of("serialization.class", "com.example.tutorial.protos.AddressBookProtos$Person"));

        metastore.createTable(tableBuilder.build(), NO_PRIVILEGES);
        Path tablePath = tempDir.resolve(databaseName, tableName);
        Path partitionPath = tablePath.resolve("year=2025");

        Partition partition = Partition.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setValues(List.of("2025"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(SEQUENCEFILE_PROTOBUF.toStorageFormat())
                        .setLocation(partitionPath.toString()))
                .setColumns(List.of())
                .build();
        metastore.addPartitions(databaseName, tableName, List.of(new PartitionWithStatistics(partition, "year=2025", PartitionStatistics.empty())));
    }

    @AfterAll
    static void tearDown()
            throws IOException
    {
        metastore.dropTable(databaseName, tableName, false);
        metastore.dropDatabase(databaseName, false);
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    void testTableWithDynamicSchema()
    {
        Table table = metastore.getTable(databaseName, tableName).orElseThrow(NullPointerException::new);
        assertColumns(table.getDataColumns());
    }

    @Test
    void testDynamicSchemaPartitions()
    {
        Table table = metastore.getTable(databaseName, tableName).orElseThrow(NullPointerException::new);
        Partition partition = metastore.getPartition(table, List.of("2025")).orElseThrow();
        assertColumns(partition.getColumns());
    }

    @Test
    void testDynamicSchemaPartitionsByNames()
    {
        Table table = metastore.getTable(databaseName, tableName).orElseThrow(NullPointerException::new);
        Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(table, List.of("year=2025"));
        assertThat(partitions).hasSize(1);
        assertThat(partitions.get("year=2025")).isNotNull();
        assertThat(partitions.get("year=2025").isPresent()).isTrue();
        assertColumns(partitions.get("year=2025").get().getColumns());
    }

    private static void assertColumns(List<Column> columns)
    {
        assertThat(columns.get(0).getName()).isEqualTo("name");
        assertThat(columns.get(0).getType()).isEqualTo(HIVE_STRING);

        assertThat(columns.get(1).getName()).isEqualTo("id");
        assertThat(columns.get(1).getType()).isEqualTo(HIVE_INT);

        assertThat(columns.get(2).getName()).isEqualTo("email");
        assertThat(columns.get(2).getType()).isEqualTo(HIVE_STRING);

        assertThat(columns.get(3).getName()).isEqualTo("phones");
        assertThat(columns.get(3).getType().getTypeInfo()).isEqualTo(getListTypeInfo(
                getStructTypeInfo(List.of("number", "type"), List.of(HIVE_STRING.getTypeInfo(), HIVE_STRING.getTypeInfo()))));
    }
}
