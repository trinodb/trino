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
package io.trino.plugin.ducklake;

import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Ducklake catalog reading from DuckDB-generated SQLite database.
 */
public class TestDucklakeCatalog
{
    private static Path catalogPath;
    private DucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        // Look for test catalog
        catalogPath = Path.of("target/test-catalog/catalog.db");

        // Generate test catalog once for all tests
        if (!Files.exists(catalogPath)) {
            synchronized (TestDucklakeCatalog.class) {
                if (!Files.exists(catalogPath)) {
                    System.out.println("Test catalog not found, generating with DuckDB...");
                    DucklakeCatalogGenerator.generateTestCatalog();
                }
            }
        }
    }

    @BeforeEach
    public void setUp()
    {
        // Create catalog instance for each test
        DucklakeConfig config = new DucklakeConfig();
        config.setCatalogDatabaseUrl("jdbc:sqlite:" + catalogPath.toAbsolutePath());
        config.setDataPath(catalogPath.getParent().toAbsolutePath().toString());
        config.setMaxCatalogConnections(5);

        catalog = new SqliteDucklakeCatalog(config);
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testGetCurrentSnapshot()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        assertThat(snapshotId).isGreaterThan(0);
    }

    @Test
    public void testListSchemas()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        var schemas = catalog.listSchemas(snapshotId);

        assertThat(schemas)
                .isNotEmpty()
                .anySatisfy(schema ->
                        assertThat(schema.schemaName()).isEqualTo("test_schema"));
    }

    @Test
    public void testListTables()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        var schemas = catalog.listSchemas(snapshotId);

        // Find test_schema
        var testSchema = schemas.stream()
                .filter(s -> s.schemaName().equals("test_schema"))
                .findFirst();

        if (testSchema.isPresent()) {
            var tables = catalog.listTables(testSchema.get().schemaId(), snapshotId);

            assertThat(tables)
                    .isNotEmpty()
                    .hasSize(2)
                    .anySatisfy(table ->
                            assertThat(table.tableName()).isEqualTo("simple_table"))
                    .anySatisfy(table ->
                            assertThat(table.tableName()).isEqualTo("array_table"));
        }
    }

    @Test
    public void testGetTable()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        var schemas = catalog.listSchemas(snapshotId);
        var testSchema = schemas.stream()
                .filter(s -> s.schemaName().equals("test_schema"))
                .findFirst();

        if (testSchema.isPresent()) {
            var tables = catalog.listTables(testSchema.get().schemaId(), snapshotId);

            // Get first table
            if (!tables.isEmpty()) {
                var table = tables.get(0);
                var retrievedTable = catalog.getTableById(table.tableId(), snapshotId);

                assertThat(retrievedTable)
                        .isPresent()
                        .get()
                        .satisfies(t -> {
                            assertThat(t.tableId()).isEqualTo(table.tableId());
                            assertThat(t.tableName()).isEqualTo(table.tableName());
                        });
            }
        }
    }

    @Test
    public void testGetDataFiles()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        var schemas = catalog.listSchemas(snapshotId);
        var testSchema = schemas.stream()
                .filter(s -> s.schemaName().equals("test_schema"))
                .findFirst();

        if (testSchema.isPresent()) {
            var tables = catalog.listTables(testSchema.get().schemaId(), snapshotId);

            if (!tables.isEmpty()) {
                var table = tables.get(0);
                var dataFiles = catalog.getDataFiles(table.tableId(), snapshotId);

                assertThat(dataFiles)
                        .isNotEmpty()
                        .allSatisfy(file -> {
                            assertThat(file.path()).isNotBlank();
                            assertThat(file.fileFormat()).isEqualTo("parquet");
                            assertThat(file.recordCount()).isGreaterThan(0);
                            assertThat(file.fileSizeBytes()).isGreaterThan(0);
                        });
            }
        }
    }
}
