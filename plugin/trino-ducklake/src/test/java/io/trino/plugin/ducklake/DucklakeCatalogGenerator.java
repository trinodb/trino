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

            // Table 3: Partitioned by region (identity transform)
            System.out.println("Creating partitioned_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.partitioned_table (
                        id INTEGER,
                        name VARCHAR,
                        region VARCHAR,
                        amount DOUBLE
                    )
                    """);

            // Set partition by region using ALTER (CREATE TABLE ... PARTITION BY not yet supported)
            System.out.println("Setting partition by region...");
            stmt.execute("ALTER TABLE ducklake_db.test_schema.partitioned_table SET PARTITIONED BY (region)");

            // Insert data per region in separate statements so DuckDB writes separate files
            System.out.println("Inserting partitioned data...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (1, 'Alice', 'US', 100.0),
                        (2, 'Bob', 'US', 200.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (3, 'Charlie', 'EU', 150.0),
                        (4, 'Diana', 'EU', 250.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (5, 'Emi', 'APAC', 300.0)
                    """);

            // Table 4: Partitioned by temporal transforms (year, month, day)
            System.out.println("Creating temporal_partitioned_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.temporal_partitioned_table (
                        id INTEGER,
                        event_name VARCHAR,
                        event_date DATE,
                        amount DOUBLE
                    )
                    """);

            // Partition by year(event_date) and month(event_date)
            System.out.println("Setting temporal partition by year(event_date), month(event_date)...");
            stmt.execute("ALTER TABLE ducklake_db.test_schema.temporal_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date))");

            // Insert data across different years/months so DuckDB writes separate files per partition
            System.out.println("Inserting temporally partitioned data...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                        (1, 'Jan Event', '2023-01-15', 100.0),
                        (2, 'Jan Meeting', '2023-01-20', 150.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                        (3, 'Jun Event', '2023-06-10', 200.0),
                        (4, 'Jun Meeting', '2023-06-25', 250.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                        (5, 'Next Year', '2024-03-05', 300.0),
                        (6, 'Next Year Too', '2024-03-20', 350.0)
                    """);

            // Table 5: Partitioned down to day level
            System.out.println("Creating daily_partitioned_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.daily_partitioned_table (
                        id INTEGER,
                        event_name VARCHAR,
                        event_date DATE,
                        amount DOUBLE
                    )
                    """);

            System.out.println("Setting partition by year/month/day...");
            stmt.execute("ALTER TABLE ducklake_db.test_schema.daily_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date), day(event_date))");

            System.out.println("Inserting daily partitioned data...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (1, 'Morning standup', '2023-06-15', 10.0),
                        (2, 'Afternoon review', '2023-06-15', 20.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (3, 'Sprint planning', '2023-06-20', 30.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (4, 'July kickoff', '2023-07-01', 40.0)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (5, 'New year event', '2024-01-10', 50.0)
                    """);

            // Table 6: Nested types (struct, map, nested arrays)
            System.out.println("Creating nested_table (struct, map, nested arrays)...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.nested_table (
                        id INTEGER,
                        metadata STRUCT(key VARCHAR, value VARCHAR),
                        tags MAP(VARCHAR, INTEGER),
                        nested_list INTEGER[][],
                        complex_struct STRUCT(name VARCHAR, scores INTEGER[], attrs MAP(VARCHAR, VARCHAR))
                    )
                    """);

            System.out.println("Inserting data into nested_table...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.nested_table VALUES
                        (1,
                         {'key': 'color', 'value': 'red'},
                         MAP {'priority': 1, 'severity': 3},
                         [[1, 2], [3, 4]],
                         {'name': 'Alice', 'scores': [90, 85, 92], 'attrs': MAP {'dept': 'eng', 'level': 'senior'}}
                        ),
                        (2,
                         {'key': 'size', 'value': 'large'},
                         MAP {'priority': 2, 'severity': 1},
                         [[10, 20, 30], [40]],
                         {'name': 'Bob', 'scores': [75, 88], 'attrs': MAP {'dept': 'sales', 'level': 'junior'}}
                        ),
                        (3,
                         {'key': 'shape', 'value': 'round'},
                         MAP {'priority': 3},
                         [[100]],
                         {'name': 'Carol', 'scores': [95, 97, 99, 100], 'attrs': MAP {'dept': 'eng', 'level': 'lead'}}
                        )
                    """);

            // Table 7: Wide types table (covers many primitive types)
            System.out.println("Creating wide_types_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.wide_types_table (
                        col_tinyint TINYINT,
                        col_smallint SMALLINT,
                        col_integer INTEGER,
                        col_bigint BIGINT,
                        col_float FLOAT,
                        col_double DOUBLE,
                        col_decimal DECIMAL(10, 2),
                        col_boolean BOOLEAN,
                        col_varchar VARCHAR,
                        col_date DATE,
                        col_timestamp TIMESTAMP,
                        col_blob BLOB
                    )
                    """);

            System.out.println("Inserting data into wide_types_table...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.wide_types_table VALUES
                        (1, 100, 10000, 1000000000, 1.5, 2.5, 123.45, true, 'hello', '2024-01-01', '2024-01-01 12:00:00', '\\x48454C4C4F'),
                        (2, 200, 20000, 2000000000, 2.5, 3.5, 678.90, false, 'world', '2024-06-15', '2024-06-15 18:30:00', '\\x574F524C44'),
                        (-1, -100, -10000, -1000000000, -1.5, -2.5, -123.45, true, '', '1970-01-01', '1970-01-01 00:00:00', '\\x00')
                    """);

            // Table 8: Nullable table (NULLs in every column type)
            System.out.println("Creating nullable_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.nullable_table (
                        id INTEGER,
                        name VARCHAR,
                        price DOUBLE,
                        active BOOLEAN,
                        created_date DATE,
                        tags VARCHAR[],
                        metadata STRUCT(key VARCHAR, value VARCHAR)
                    )
                    """);

            System.out.println("Inserting data into nullable_table (with NULLs)...");
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.nullable_table VALUES
                        (1, 'Present', 10.0, true, '2024-01-01', ['a', 'b'], {'key': 'k1', 'value': 'v1'}),
                        (2, NULL, NULL, NULL, NULL, NULL, NULL),
                        (NULL, 'NoId', 20.0, false, '2024-06-01', ['c'], {'key': 'k2', 'value': 'v2'}),
                        (4, NULL, NULL, true, NULL, NULL, {'key': 'k3', 'value': NULL})
                    """);

            // Table 9: Empty table (no rows, tests empty result handling)
            System.out.println("Creating empty_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.empty_table (
                        id INTEGER,
                        name VARCHAR,
                        value DOUBLE
                    )
                    """);

            // Table 10: Schema evolution table
            // Create with original columns, insert data, then add a column and insert more
            System.out.println("Creating schema_evolution_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.schema_evolution_table (
                        id INTEGER,
                        original_col VARCHAR
                    )
                    """);

            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.schema_evolution_table VALUES
                        (1, 'row1'),
                        (2, 'row2')
                    """);

            // Force a checkpoint so the first batch is in its own Parquet file
            stmt.execute("CHECKPOINT ducklake_db");

            // Now add a new column and insert more data
            System.out.println("Adding column to schema_evolution_table...");
            stmt.execute("ALTER TABLE ducklake_db.test_schema.schema_evolution_table ADD COLUMN added_col INTEGER");

            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.schema_evolution_table VALUES
                        (3, 'row3', 300),
                        (4, 'row4', 400)
                    """);

            // Table 11: Large-ish table for aggregation/stats tests
            System.out.println("Creating aggregation_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.aggregation_table (
                        id INTEGER,
                        category VARCHAR,
                        amount DOUBLE,
                        quantity INTEGER
                    )
                    """);

            StringBuilder insertSql = new StringBuilder(
                    "INSERT INTO ducklake_db.test_schema.aggregation_table VALUES ");
            String[] categories = {"A", "B", "C"};
            for (int i = 1; i <= 30; i++) {
                if (i > 1) {
                    insertSql.append(", ");
                }
                String category = categories[(i - 1) % 3];
                double amount = 10.0 * i + (i % 7);
                int quantity = i * 5;
                insertSql.append(String.format("(%d, '%s', %.1f, %d)", i, category, amount, quantity));
            }
            stmt.execute(insertSql.toString());

            // Table 12: Table with deleted rows (tests delete file / merge-on-read handling)
            System.out.println("Creating deleted_rows_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.deleted_rows_table (
                        id INTEGER,
                        name VARCHAR,
                        value DOUBLE
                    )
                    """);

            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.deleted_rows_table VALUES
                        (1, 'keep', 10.0),
                        (2, 'delete', 20.0),
                        (3, 'keep', 30.0),
                        (4, 'delete', 40.0),
                        (5, 'keep', 50.0),
                        (6, 'delete', 60.0)
                    """);

            // Flush before delete so data is in Parquet files
            stmt.execute("CALL ducklake_flush_inlined_data('ducklake_db')");

            System.out.println("Deleting rows from deleted_rows_table...");
            stmt.execute("DELETE FROM ducklake_db.test_schema.deleted_rows_table WHERE name = 'delete'");

            // Table 13: Simple struct with full-NULL rows and variable-length arrays with null elements
            System.out.println("Creating complex_nulls_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.complex_nulls_table (
                        id INTEGER,
                        pair STRUCT(a INTEGER, b INTEGER),
                        items INTEGER[]
                    )
                    """);

            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.complex_nulls_table VALUES
                        (1, {'a': 10, 'b': 20}, [1, 2, 3]),
                        (2, NULL, [NULL]),
                        (3, {'a': NULL, 'b': 40}, NULL),
                        (4, NULL, [4, 5]),
                        (5, {'a': 50, 'b': 60}, [])
                    """);

            // Table 14: Multi-file table (separate INSERTs produce separate Parquet files)
            System.out.println("Creating multi_file_table...");
            stmt.execute("""
                    CREATE TABLE ducklake_db.test_schema.multi_file_table (
                        id INTEGER,
                        value VARCHAR
                    )
                    """);

            // Disable inlining so each INSERT creates a separate Parquet file
            stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => 'multi_file_table')");

            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                        (1, 'file1_row1'),
                        (2, 'file1_row2')
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                        (3, 'file2_row1'),
                        (NULL, NULL)
                    """);
            stmt.execute("""
                    INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                        (5, 'file3_row1')
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
            System.out.println("  - test_schema.partitioned_table (5 rows, partitioned by region)");
            System.out.println("  - test_schema.temporal_partitioned_table (6 rows, partitioned by year/month)");
            System.out.println("  - test_schema.daily_partitioned_table (5 rows, partitioned by year/month/day)");
            System.out.println("  - test_schema.nested_table (3 rows, struct/map/nested arrays)");
            System.out.println("  - test_schema.wide_types_table (3 rows, many primitive types)");
            System.out.println("  - test_schema.nullable_table (4 rows, NULLs in every column type)");
            System.out.println("  - test_schema.empty_table (0 rows, empty result set testing)");
            System.out.println("  - test_schema.schema_evolution_table (4 rows, column added after initial write)");
            System.out.println("  - test_schema.aggregation_table (30 rows, aggregation/stats testing)");
            System.out.println("  - test_schema.deleted_rows_table (3 surviving rows, delete file handling)");
            System.out.println("  - test_schema.complex_nulls_table (5 rows, full-NULL structs/arrays with null elements)");
            System.out.println("  - test_schema.multi_file_table (5 rows across 3 Parquet files, multi-file scan)");
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
