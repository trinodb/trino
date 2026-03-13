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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Utility to generate a test Ducklake catalog using DuckDB's embedded JDBC driver.
 * This creates the SQLite catalog database and Parquet data files that the connector reads.
 */
public final class DucklakeCatalogGenerator
{
    private static final Path TARGET_DIR = Path.of("target");
    private static final Path CATALOG_DIR = TARGET_DIR.resolve("test-catalog");
    private static final Path TEMP_DB = TARGET_DIR.resolve("temp-setup.duckdb");

    private DucklakeCatalogGenerator() {}

    public static void main(String[] args)
            throws Exception
    {
        generateTestCatalog();
    }

    public static void generateTestCatalog()
            throws Exception
    {
        System.out.println("==========================================");
        System.out.println("Ducklake Test Catalog Generator");
        System.out.println("==========================================");
        System.out.println();

        // Create target directory
        Files.createDirectories(TARGET_DIR);

        // Remove old catalog if exists
        if (Files.exists(CATALOG_DIR)) {
            System.out.println("Removing existing test catalog...");
            deleteDirectory(CATALOG_DIR);
        }

        // Remove old temp DB if exists
        Files.deleteIfExists(TEMP_DB);

        System.out.println("Creating test catalog with DuckDB 1.5 + Ducklake extension...");
        System.out.println();

        // Create catalog directory and data directory
        Files.createDirectories(CATALOG_DIR);
        Path dataDir = CATALOG_DIR.resolve("data");
        Files.createDirectories(dataDir);

        String catalogDbPath = CATALOG_DIR.resolve("catalog.db").toAbsolutePath().toString();

        // Connect to DuckDB in-memory
        String jdbcUrl = "jdbc:duckdb:";

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            // Install and load extensions
            System.out.println("Installing extensions...");
            stmt.execute("INSTALL ducklake");
            stmt.execute("INSTALL sqlite");
            System.out.println("Loading extensions...");
            stmt.execute("LOAD ducklake");
            stmt.execute("LOAD sqlite");

            // Attach Ducklake catalog pointing to SQLite DB
            System.out.println("Attaching Ducklake catalog...");
            stmt.execute(String.format(
                    "ATTACH 'ducklake:sqlite:%s' AS ducklake_db (DATA_PATH '%s')",
                    catalogDbPath,
                    dataDir.toAbsolutePath()));

            // Create test schema in Ducklake
            System.out.println("Creating test schema...");
            stmt.execute("CREATE SCHEMA ducklake_db.test_schema");

            // Table 1: Simple primitives only
            System.out.println("Creating simple_table (primitives only)...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.simple_table (
                        id INTEGER,
                        name VARCHAR,
                        price DOUBLE,
                        active BOOLEAN,
                        created_date DATE
                    )
                    """);

            System.out.println("Inserting data into simple_table...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.simple_table VALUES
                        (1, 'Product A', 19.99, true, '2024-01-15'),
                        (2, 'Product B', 29.99, true, '2024-02-20'),
                        (3, 'Product C', 39.99, false, '2024-03-10'),
                        (4, 'Product D', 49.99, true, '2024-01-05'),
                        (5, 'Product E', 59.99, false, '2024-02-28')
                    """);

            // Table 2: Primitives + array(varchar)
            System.out.println("Creating array_table (primitives + array)...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.array_table (
                        id INTEGER,
                        product_name VARCHAR,
                        tags VARCHAR[],
                        quantity INTEGER
                    )
                    """);

            System.out.println("Inserting data into array_table...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.array_table VALUES
                        (1, 'Widget', ['electronics', 'gadgets', 'sale'], 100),
                        (2, 'Gizmo', ['tools', 'hardware'], 50),
                        (3, 'Doohickey', ['accessories', 'premium', 'new'], 25),
                        (4, 'Thingamajig', ['clearance'], 200),
                        (5, 'Whatchamacallit', ['featured', 'bestseller', 'trending'], 75)
                    """);

            // Force checkpoint to write data to Parquet files
            System.out.println("Forcing checkpoint to write Parquet files...");
            stmt.execute("CHECKPOINT ducklake_db");

            // Detach Ducklake catalog (commits everything to SQLite)
            System.out.println("Detaching Ducklake catalog...");
            stmt.execute("DETACH ducklake_db");

            System.out.println();
            System.out.println("✓ Test catalog created successfully!");
            System.out.println();
            System.out.println("Catalog location: " + CATALOG_DIR.toAbsolutePath());
            System.out.println("Catalog database: " + catalogDbPath);
            System.out.println("Data directory: " + dataDir.toAbsolutePath());
            System.out.println();
            System.out.println("Tables created:");
            System.out.println("  - test_schema.simple_table (5 rows, primitives only)");
            System.out.println("  - test_schema.array_table (5 rows, with array(varchar))");
        }

        System.out.println();
        System.out.println("You can now run tests with:");
        System.out.println("  mvn test -Dtest=TestDucklakeCatalog");
    }

    private static void deleteDirectory(Path directory)
            throws Exception
    {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted((a, b) -> -a.compareTo(b)) // Reverse order to delete files before directories
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        }
                        catch (Exception e) {
                            throw new RuntimeException("Failed to delete: " + path, e);
                        }
                    });
        }
    }
}
