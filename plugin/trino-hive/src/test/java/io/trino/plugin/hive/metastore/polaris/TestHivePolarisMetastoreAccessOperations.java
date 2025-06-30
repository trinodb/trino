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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.log.Logger;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveType;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.CREATE_DATABASE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.CREATE_GENERIC_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.CREATE_ICEBERG_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.DROP_GENERIC_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.DROP_ICEBERG_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.DROP_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.GET_ALL_TABLES;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.GET_DATABASE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.LIST_GENERIC_TABLES;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.LIST_ICEBERG_TABLES;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.LOAD_GENERIC_TABLE;
import static io.trino.plugin.hive.metastore.polaris.PolarisMetastoreMethod.LOAD_ICEBERG_TABLE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Integration test for Polaris Hive Metastore operations using the official Apache Polaris Docker image.
 * This test verifies that metastore operations result in the correct Polaris API calls for both Iceberg and generic tables,
 * following the same pattern as TestHiveGlueMetastoreAccessOperations.
 */
@TestInstance(PER_CLASS)
final class TestHivePolarisMetastoreAccessOperations
{
    private static final Logger log = Logger.get(TestHivePolarisMetastoreAccessOperations.class);

    private final String testSchema = "test_schema_" + randomNameSuffix();

    private TestingPolarisHiveMetastore polarisContainer;
    private PolarisHiveMetastore polarisMetastore;
    private PolarisMetastoreStats polarisStats;
    private Path warehouseLocation;

    @BeforeAll
    void setUp()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory("polaris-integration-warehouse");
        polarisContainer = TestingPolarisHiveMetastore.create(warehouseLocation, closer -> {});
        polarisMetastore = polarisContainer.createHiveMetastore();
        polarisStats = polarisMetastore.getStats();

        // Create test schema
        Database testDatabase = Database.builder()
                .setDatabaseName(testSchema)
                .setOwnerName(Optional.of("integration-test"))
                .setOwnerType(Optional.of(io.trino.spi.security.PrincipalType.USER))
                .setComment(Optional.of("Integration test database"))
                .setLocation(Optional.of("file://" + polarisContainer.getWarehousePath() + "/" + testSchema))
                .setParameters(Map.of("test", "true"))
                .build();
        polarisMetastore.createDatabase(testDatabase);
    }

    @AfterAll
    void cleanUp()
            throws Exception
    {
        if (polarisContainer != null) {
            polarisContainer.close();
        }
    }

    @Test
    void testCreateIcebergTable()
    {
        log.info("=== RUNNING TEST: testCreateIcebergTable ===");
        String tableName = "test_create_iceberg_" + randomNameSuffix();

        Table icebergTable = createTestTable(tableName, "ICEBERG");

        assertInvocations(() -> polarisMetastore.createTable(icebergTable, PrincipalPrivileges.NO_PRIVILEGES),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(CREATE_ICEBERG_TABLE, 1)
                        .build());

        // Verify table was created successfully
        Optional<Table> retrievedTable = polarisMetastore.getTable(testSchema, tableName);
        assertThat(retrievedTable).isPresent();
        assertThat(retrievedTable.get().getTableName()).isEqualTo(tableName);
        log.info("=== COMPLETED TEST: testCreateIcebergTable ===");
    }

    @Test
    void testCreateDeltaTable()
    {
        log.info("=== RUNNING TEST: testCreateDeltaTable ===");
        String tableName = "test_create_delta_" + randomNameSuffix();

        Table deltaTable = createTestTable(tableName, "DELTA");

        assertInvocations(() -> polarisMetastore.createTable(deltaTable, PrincipalPrivileges.NO_PRIVILEGES),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(CREATE_GENERIC_TABLE, 1)
                        .build());

        // Verify table was created successfully
        Optional<Table> retrievedTable = polarisMetastore.getTable(testSchema, tableName);
        assertThat(retrievedTable).isPresent();
        assertThat(retrievedTable.get().getTableName()).isEqualTo(tableName);
        log.info("=== COMPLETED TEST: testCreateDeltaTable ===");
    }

    @Test
    void testGetTable()
    {
        log.info("=== RUNNING TEST: testGetTable ===");
        // Test loading Iceberg table
        String icebergTableName = "test_load_iceberg_" + randomNameSuffix();
        Table icebergTable = createTestTable(icebergTableName, "ICEBERG");
        polarisMetastore.createTable(icebergTable, PrincipalPrivileges.NO_PRIVILEGES);

        assertInvocations(() -> polarisMetastore.getTable(testSchema, icebergTableName),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .addCopies(LOAD_ICEBERG_TABLE, 1)
                        .build());

        String deltaTableName = "test_load_delta_" + randomNameSuffix();
        Table deltaTable = createTestTable(deltaTableName, "DELTA");
        polarisMetastore.createTable(deltaTable, PrincipalPrivileges.NO_PRIVILEGES);

        // For Delta tables: first tries Iceberg (fails), then generic (succeeds)
        assertInvocations(() -> polarisMetastore.getTable(testSchema, deltaTableName),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .addCopies(LOAD_ICEBERG_TABLE, 1) // Tries Iceberg first (fails)
                        .addCopies(LOAD_GENERIC_TABLE, 1) // Then tries generic (succeeds)
                        .build());
        log.info("=== COMPLETED TEST: testGetTable ===");
    }

    @Test
    void testGetAllTables()
    {
        log.info("=== RUNNING TEST: testGetAllTables ===");
        assertInvocations(() -> polarisMetastore.getAllTables(testSchema),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(GET_ALL_TABLES, 1)
                        .addCopies(LIST_ICEBERG_TABLES, 1)
                        .addCopies(LIST_GENERIC_TABLES, 1)
                        .build());
        log.info("=== COMPLETED TEST: testGetAllTables ===");
    }

    @Test
    void testDropIcebergTable()
    {
        log.info("=== RUNNING TEST: testDropIcebergTable ===");
        String tableName = "test_drop_iceberg_" + randomNameSuffix();
        Table icebergTable = createTestTable(tableName, "ICEBERG");
        polarisMetastore.createTable(icebergTable, PrincipalPrivileges.NO_PRIVILEGES);

        assertInvocations(() -> polarisMetastore.dropTable(testSchema, tableName, false),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(DROP_TABLE, 1)
                        .addCopies(GET_TABLE, 1)
                        .addCopies(LOAD_ICEBERG_TABLE, 1)
                        .addCopies(DROP_ICEBERG_TABLE, 1)
                        .build());

        // Verify table is gone
        assertThat(polarisMetastore.getTable(testSchema, tableName)).isEmpty();
        log.info("=== COMPLETED TEST: testDropIcebergTable ===");
    }

    @Test
    void testDropDeltaTable()
    {
        log.info("=== RUNNING TEST: testDropDeltaTable ===");
        String tableName = "test_drop_delta_" + randomNameSuffix();
        Table deltaTable = createTestTable(tableName, "DELTA");
        polarisMetastore.createTable(deltaTable, PrincipalPrivileges.NO_PRIVILEGES);

        // dropTable() calls getTable() to determine format, then uses appropriate drop method
        // For Delta: getTable() tries Iceberg (fails), then generic (succeeds), then drops via generic
        assertInvocations(() -> polarisMetastore.dropTable(testSchema, tableName, false),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(DROP_TABLE, 1)
                        .addCopies(GET_TABLE, 1) // To determine format
                        .addCopies(LOAD_ICEBERG_TABLE, 1) // Tries Iceberg first (fails)
                        .addCopies(LOAD_GENERIC_TABLE, 1) // Then tries generic (succeeds)
                        .addCopies(DROP_GENERIC_TABLE, 1) // Uses generic drop method
                        .build());

        // Verify table is gone
        assertThat(polarisMetastore.getTable(testSchema, tableName)).isEmpty();
        log.info("=== COMPLETED TEST: testDropDeltaTable ===");
    }

    @Test
    void testGetDatabase()
    {
        log.info("=== RUNNING TEST: testGetDatabase ===");
        assertInvocations(() -> polarisMetastore.getDatabase(testSchema),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(GET_DATABASE, 1)
                        .build());
        log.info("=== COMPLETED TEST: testGetDatabase ===");
    }

    @Test
    void testGetAllDatabases()
    {
        log.info("=== RUNNING TEST: testGetAllDatabases ===");
        assertInvocations(() -> polarisMetastore.getAllDatabases(),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(GET_ALL_DATABASES, 1)
                        .build());

        // Verify our test schema appears in the results
        List<String> databases = polarisMetastore.getAllDatabases();
        assertThat(databases).contains(testSchema);
        log.info("=== COMPLETED TEST: testGetAllDatabases ===");
    }

    @Test
    void testCreateDatabase()
    {
        log.info("=== RUNNING TEST: testCreateDatabase ===");
        String databaseName = "test_db_" + randomNameSuffix();
        Database database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(io.trino.spi.security.PrincipalType.USER))
                .setComment(Optional.of("Test database"))
                .setLocation(Optional.of("file://" + polarisContainer.getWarehousePath() + "/" + databaseName))
                .build();

        assertInvocations(() -> polarisMetastore.createDatabase(database),
                ImmutableMultiset.<PolarisMetastoreMethod>builder()
                        .addCopies(CREATE_DATABASE, 1)
                        .build());

        // Verify database was created
        Optional<Database> retrievedDb = polarisMetastore.getDatabase(databaseName);
        assertThat(retrievedDb).isPresent();
        assertThat(retrievedDb.get().getDatabaseName()).isEqualTo(databaseName);
        log.info("=== COMPLETED TEST: testCreateDatabase ===");
    }

    // Helper methods
    private Table createTestTable(String tableName, String tableType)
    {
        Map<String, String> parameters;

        if ("DELTA".equals(tableType)) {
            // Use standard Trino parameter conventions
            parameters = Map.of("spark.sql.sources.provider", "delta");
        }
        else {
            // Use standard Trino parameter for Iceberg tables
            parameters = Map.of("table_type", "iceberg");
        }

        Table table = Table.builder()
                .setDatabaseName(testSchema)
                .setTableName(tableName)
                .setTableType("EXTERNAL_TABLE")
                .setOwner(Optional.of("integration-test"))
                .setDataColumns(ImmutableList.of(
                        new Column("id", HiveType.HIVE_LONG, Optional.of("ID column"), Map.of()),
                        new Column("name", HiveType.HIVE_STRING, Optional.of("Name column"), Map.of())))
                .setParameters(parameters)
                .withStorage(storage -> storage
                        .setLocation("file://" + polarisContainer.getWarehousePath() + "/" + testSchema + "/" + tableName)
                        .setStorageFormat(StorageFormat.create(
                                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")))
                .build();

        return table;
    }

    private void assertInvocations(Runnable operation, Multiset<PolarisMetastoreMethod> expectedInvocations)
    {
        Map<PolarisMetastoreMethod, Long> countsBefore = Arrays.stream(PolarisMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(polarisStats)));

        operation.run();

        Map<PolarisMetastoreMethod, Long> countsAfter = Arrays.stream(PolarisMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(polarisStats)));

        Multiset<PolarisMetastoreMethod> actualInvocations = Arrays.stream(PolarisMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> toIntExact(requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method)))));

        assertMultisetsEqual(actualInvocations, expectedInvocations);
    }
}
