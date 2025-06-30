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
package io.trino.plugin.hive.metastore.polaris;

import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveType;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hive.TableType;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestPolarisHiveMetastoreOperations
{
    private static final String TEST_SCHEMA = "test_schema_" + randomNameSuffix();

    private TestingPolarisHiveMetastore polarisContainer;
    private PolarisHiveMetastore hiveMetastore;
    private Path warehouseLocation;

    @BeforeAll
    void setUp()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory("polaris-test-warehouse");
        polarisContainer = TestingPolarisHiveMetastore.create(warehouseLocation, resource -> {});
        hiveMetastore = (PolarisHiveMetastore) polarisContainer.createHiveMetastore();

        // Create test schema
        Database testDatabase = Database.builder()
                .setDatabaseName(TEST_SCHEMA)
                .setLocation(Optional.of("file://" + polarisContainer.getWarehousePath() + "/" + TEST_SCHEMA))
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
        hiveMetastore.createDatabase(testDatabase);
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        if (polarisContainer != null) {
            polarisContainer.close();
        }
    }

    @Test
    void testCreateAndGetDatabase()
    {
        // Test that we can retrieve the database we created
        Optional<Database> database = hiveMetastore.getDatabase(TEST_SCHEMA);

        assertThat(database).isPresent();
        assertThat(database.get().getDatabaseName()).isEqualTo(TEST_SCHEMA);
        assertThat(database.get().getLocation()).isPresent();
        assertThat(database.get().getLocation().get()).contains(TEST_SCHEMA);
    }

    @Test
    void testListDatabases()
    {
        List<String> databases = hiveMetastore.getAllDatabases();

        assertThat(databases).contains(TEST_SCHEMA);
        // Should also contain the tpch database created by TestingPolarisHiveMetastore
        assertThat(databases).contains("tpch");
    }

    @Test
    void testCreateTableAndVerifyInPolaris()
    {
        String tableName = "test_metadata_creation_" + randomNameSuffix();

        // Check tables before creation
        List<String> tablesBeforeCreation = hiveMetastore.getAllTables(TEST_SCHEMA);

        // Create a minimal table - just enough to trigger Polaris metadata creation
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(TEST_SCHEMA)
                .setTableName(tableName)
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setOwner(Optional.of("test"))
                .setDataColumns(List.of(
                        new Column("id", HiveType.HIVE_LONG, Optional.empty(), Map.of()),
                        new Column("name", HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                .setParameters(emptyMap());

        // Add minimal storage format (required for Table validation)
        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.create(
                        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "org.apache.hadoop.mapred.TextInputFormat",
                        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
                .setLocation("file://" + polarisContainer.getWarehousePath() + "/" + TEST_SCHEMA + "/" + tableName);

        Table table = tableBuilder.build();

        // Create table - this should create metadata in Polaris's internal database
        hiveMetastore.createTable(table, NO_PRIVILEGES);

        // Check tables after creation
        List<String> tablesAfterCreation = hiveMetastore.getAllTables(TEST_SCHEMA);
        assertThat(tablesAfterCreation).hasSize(tablesBeforeCreation.size() + 1);
        assertThat(tablesAfterCreation).containsAll(tablesBeforeCreation);
        assertThat(tablesAfterCreation).contains(tableName);

        // Verify 1: Table can be retrieved by name
        Optional<Table> retrievedTable = hiveMetastore.getTable(TEST_SCHEMA, tableName);
        assertThat(retrievedTable).isPresent();
        assertThat(retrievedTable.get().getTableName()).isEqualTo(tableName);
        assertThat(retrievedTable.get().getDatabaseName()).isEqualTo(TEST_SCHEMA);

        // Verify 2: Table has correct schema information
        assertThat(retrievedTable.get().getDataColumns()).hasSize(2);
        assertThat(retrievedTable.get().getDataColumns().get(0).getName()).isEqualTo("id");
        assertThat(retrievedTable.get().getDataColumns().get(1).getName()).isEqualTo("name");
    }

    @Test
    void testListTables()
    {
        String tableName1 = "test_list_table1_" + randomNameSuffix();
        String tableName2 = "test_list_table2_" + randomNameSuffix();

        // Check initial table count
        List<String> tablesInitially = hiveMetastore.getAllTables(TEST_SCHEMA);

        // Create first table
        Table.Builder tableBuilder1 = Table.builder()
                .setDatabaseName(TEST_SCHEMA)
                .setTableName(tableName1)
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setOwner(Optional.of("test"))
                .setDataColumns(List.of(new Column("id", HiveType.HIVE_LONG, Optional.empty(), Map.of())))
                .setParameters(emptyMap());

        tableBuilder1.getStorageBuilder()
                .setStorageFormat(StorageFormat.create(
                        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "org.apache.hadoop.mapred.TextInputFormat",
                        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
                .setLocation("file://" + polarisContainer.getWarehousePath() + "/" + TEST_SCHEMA + "/" + tableName1);

        hiveMetastore.createTable(tableBuilder1.build(), NO_PRIVILEGES);

        // Check after first table
        List<String> tablesAfterFirst = hiveMetastore.getAllTables(TEST_SCHEMA);
        assertThat(tablesAfterFirst).hasSize(tablesInitially.size() + 1);
        assertThat(tablesAfterFirst).contains(tableName1);

        // Create second table
        Table.Builder tableBuilder2 = Table.builder()
                .setDatabaseName(TEST_SCHEMA)
                .setTableName(tableName2)
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setOwner(Optional.of("test"))
                .setDataColumns(List.of(new Column("name", HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                .setParameters(emptyMap());

        tableBuilder2.getStorageBuilder()
                .setStorageFormat(StorageFormat.create(
                        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "org.apache.hadoop.mapred.TextInputFormat",
                        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
                .setLocation("file://" + polarisContainer.getWarehousePath() + "/" + TEST_SCHEMA + "/" + tableName2);

        hiveMetastore.createTable(tableBuilder2.build(), NO_PRIVILEGES);

        // Check final table count
        List<String> tablesAfterBoth = hiveMetastore.getAllTables(TEST_SCHEMA);
        assertThat(tablesAfterBoth).hasSize(tablesInitially.size() + 2);
        assertThat(tablesAfterBoth).contains(tableName1);
        assertThat(tablesAfterBoth).contains(tableName2);
    }

    @Test
    void testDropTable()
    {
        String tableName = "test_drop_table_" + randomNameSuffix();

        // Create a simple table first
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(TEST_SCHEMA)
                .setTableName(tableName)
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setOwner(Optional.of("test"))
                .setDataColumns(List.of(new Column("id", HiveType.HIVE_LONG, Optional.empty(), Map.of())))
                .setParameters(emptyMap());

        // Add minimal storage format
        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.create(
                        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "org.apache.hadoop.mapred.TextInputFormat",
                        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
                .setLocation("file://" + polarisContainer.getWarehousePath() + "/" + TEST_SCHEMA + "/" + tableName);

        Table table = tableBuilder.build();
        hiveMetastore.createTable(table, NO_PRIVILEGES);

        // Verify table exists
        Optional<Table> retrievedTable = hiveMetastore.getTable(TEST_SCHEMA, tableName);
        assertThat(retrievedTable).isPresent();

        List<String> tablesBeforeDrop = hiveMetastore.getAllTables(TEST_SCHEMA);
        assertThat(tablesBeforeDrop).contains(tableName);

        // Drop table
        hiveMetastore.dropTable(TEST_SCHEMA, tableName, false);

        // Verify table metadata is removed from Polaris
        Optional<Table> tableAfterDrop = hiveMetastore.getTable(TEST_SCHEMA, tableName);
        assertThat(tableAfterDrop).isEmpty();

        List<String> tablesAfterDrop = hiveMetastore.getAllTables(TEST_SCHEMA);
        assertThat(tablesAfterDrop).doesNotContain(tableName);
        assertThat(tablesAfterDrop).hasSize(tablesBeforeDrop.size() - 1);
    }

    @Test
    void testTableNotFound()
    {
        // Test getting a non-existent table
        Optional<Table> table = hiveMetastore.getTable(TEST_SCHEMA, "non_existent_table");
        assertThat(table).isEmpty();
    }

    @Test
    void testDatabaseNotFound()
    {
        // Test getting a non-existent database
        Optional<Database> database = hiveMetastore.getDatabase("non_existent_database");
        assertThat(database).isEmpty();
    }
}
