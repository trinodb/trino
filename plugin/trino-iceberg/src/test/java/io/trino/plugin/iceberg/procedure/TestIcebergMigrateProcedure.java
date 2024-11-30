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

package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMigrateProcedure
        extends AbstractTestQueryFramework
{
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("_test_hidden");
        QueryRunner queryRunner = IcebergQueryRunner.builder().setMetastoreDirectory(dataDirectory.toFile()).build();
        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .buildOrThrow());
        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    public void testMigrateTable(IcebergFileFormat fileFormat)
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format='" + fileFormat + "') AS SELECT 1 x", 1);
        assertUpdate("INSERT INTO " + hiveTableName + " VALUES NULL", 1);
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat((String) computeScalar("SHOW CREATE TABLE " + icebergTableName))
                .contains("format = '%s'".formatted(fileFormat));

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1, NULL");
        assertQuery("SELECT count(*) FROM " + icebergTableName, "VALUES 2");
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1, NULL");
        assertQuery("SELECT count(*) FROM " + icebergTableName, "VALUES 2");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES 2, NULL", 2);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1, NULL, 2, NULL");
        assertQuery("SELECT count(*) FROM " + icebergTableName, "VALUES 4");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    public void testMigrateTableWithTinyintType(IcebergFileFormat fileFormat)
    {
        String tableName = "test_migrate_tinyint" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        String createTable = "CREATE TABLE " + hiveTableName + "(col TINYINT) WITH (format = '" + fileFormat + "')";
        if (fileFormat == AVRO) {
            assertQueryFails(createTable, "Column 'col' is tinyint, which is not supported by Avro. Use integer instead.");
            return;
        }

        assertUpdate(createTable);
        assertUpdate("INSERT INTO " + hiveTableName + " VALUES NULL, -128, 127", 3);

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat(getColumnType(tableName, "col")).isEqualTo("integer");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (NULL), (-128), (127)");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES -2147483648, 2147483647", 2);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (NULL), (-2147483648), (-128), (127), (2147483647)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    public void testMigrateTableWithSmallintType(IcebergFileFormat fileFormat)
    {
        String tableName = "test_migrate_smallint" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        String createTable = "CREATE TABLE " + hiveTableName + "(col SMALLINT) WITH (format = '" + fileFormat + "')";
        if (fileFormat == AVRO) {
            assertQueryFails(createTable, "Column 'col' is smallint, which is not supported by Avro. Use integer instead.");
            return;
        }

        assertUpdate(createTable);
        assertUpdate("INSERT INTO " + hiveTableName + " VALUES NULL, -32768, 32767", 3);

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat(getColumnType(tableName, "col")).isEqualTo("integer");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (NULL), (-32768), (32767)");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES -2147483648, 2147483647", 2);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (NULL), (-2147483648), (-32768), (32767), (2147483647)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    public void testMigrateTableWithComplexType(IcebergFileFormat fileFormat)
    {
        String tableName = "test_migrate_complex_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format='" + fileFormat + "') AS " +
                        "SELECT 1 x, array[2, 3] a, " +
                        "CAST(map(array['key1'], array['value1']) AS map(varchar, varchar)) b, " +
                        "CAST(row(1) AS row(d integer)) c",
                1);
        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        String firstRow = "VALUES (" +
                "1, " +
                "ARRAY[2, 3], " +
                "CAST(map(ARRAY['key1'], ARRAY['value1']) AS map(varchar, varchar)), " +
                "CAST(row(1) AS row(d integer)))";
        assertThat(query("SELECT * FROM " + icebergTableName))
                .matches(firstRow);

        String secondRow = " VALUES (" +
                "2, " +
                "ARRAY[4, 5], " +
                "CAST(map(ARRAY['key2'], ARRAY['value2']) AS map(varchar, varchar)), " +
                "CAST(row(2) AS row(d integer)))";
        assertUpdate("INSERT INTO " + icebergTableName + secondRow, 1);
        assertThat(query("SELECT * FROM " + icebergTableName))
                .matches(firstRow + " UNION ALL " + secondRow);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMigrateTimestampHiveTableInComplexType()
    {
        String inputValue = "2021-01-01 10:11:12.123"; // Tests are run with America/Bahia_Banderas timezone
        String expectedValue = "2021-01-01 16:11:12.123000 UTC";

        String tableName = "test_migrate_timestamp_complex_type_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format='PARQUET') AS " +
                     "SELECT CAST(row(timestamp '" + inputValue + "') AS row(t timestamp(3))) r," +
                     "array[timestamp '" + inputValue + "'] a, " +
                     "CAST(map(array[1], array[timestamp '" + inputValue + "']) AS map(int, timestamp(3))) m", 1);

        assertThat(query("SELECT a, m, r.t FROM " + hiveTableName))
                .matches("VALUES (" +
                         " ARRAY[timestamp '" + inputValue + "'], " +
                         " CAST(map(ARRAY[1], ARRAY[timestamp '" + inputValue + "']) AS map(int, timestamp(3)))," +
                         " timestamp '" + inputValue + "')");

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat(query("SELECT a, m, r.t FROM " + icebergTableName))
                .matches("VALUES (" +
                         " ARRAY[timestamp '" + expectedValue + "'], " +
                         " CAST(map(ARRAY[1], ARRAY[timestamp '" + expectedValue + "']) AS map(int, timestamp(6) with time zone))," +
                         " timestamp '" + expectedValue + "')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    public void testMigrateTableSchemaEvolution(IcebergFileFormat fileFormat)
            throws Exception
    {
        String randomNameSuffix = randomNameSuffix();
        String tableNameOneColumn = "test_migrate_one_column_" + randomNameSuffix;
        String tableNameTwoColumns = "test_migrate_two_columns_" + randomNameSuffix;
        String hiveTableNameOneColumn = "hive.tpch." + tableNameOneColumn;
        String hiveTableNameTwoColumns = "hive.tpch." + tableNameTwoColumns;
        String icebergTableNameTwoColumns = "iceberg.tpch." + tableNameTwoColumns;

        assertUpdate("CREATE TABLE " + hiveTableNameOneColumn + " WITH (format='" + fileFormat + "') AS SELECT 1 col1", 1);
        assertUpdate("CREATE TABLE " + hiveTableNameTwoColumns + " WITH (format='" + fileFormat + "') AS SELECT 2 col1, CAST(row(10, 20) AS row(x integer, y integer)) AS nested", 1);

        // Copy the parquet file containing only one column to the table with two columns
        Path tableNameOneColumnLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableNameOneColumn));
        Path tableNameTwoColumnsLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableNameTwoColumns));
        try (Stream<Path> files = Files.list(tableNameOneColumnLocation)) {
            Path file = files.filter(path -> !path.getFileName().toString().startsWith(".")).collect(onlyElement());
            Files.copy(file, tableNameTwoColumnsLocation.resolve(file.getFileName()));
        }

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableNameTwoColumns + "')");

        assertThat(query("SELECT * FROM " + icebergTableNameTwoColumns))
                .skippingTypesCheck()
                .matches("VALUES (1, CAST(null AS row(x integer, y integer))), (2, row(10, 20))");

        assertUpdate("INSERT INTO " + icebergTableNameTwoColumns + " VALUES (3, row(100, 200))", 1);
        assertThat(query("SELECT * FROM " + icebergTableNameTwoColumns))
                .skippingTypesCheck()
                .matches("VALUES (1, CAST(null AS row(x integer, y integer))), (2, row(10, 20)), (3, row(100, 200))");

        assertUpdate("DROP TABLE " + icebergTableNameTwoColumns);
        assertUpdate("DROP TABLE " + hiveTableNameOneColumn);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    public void testMigrateTableRowColumnSchemaEvolution(IcebergFileFormat fileFormat)
            throws Exception
    {
        String randomNameSuffix = randomNameSuffix();
        String tableNameRowOneField = "test_migrate_row_one_field_" + randomNameSuffix;
        String tableNameRowTwoFields = "test_migrate_row_two_fields_" + randomNameSuffix;
        String hiveTableNameRowOneField = "hive.tpch." + tableNameRowOneField;
        String hiveTableNameRowTwoFields = "hive.tpch." + tableNameRowTwoFields;
        String icebergTableNameRowTwoFields = "iceberg.tpch." + tableNameRowTwoFields;

        assertUpdate("CREATE TABLE " + hiveTableNameRowOneField + " WITH (format='" + fileFormat + "') AS SELECT CAST(row(1) AS row(x integer)) as nested", 1);
        assertUpdate("CREATE TABLE " + hiveTableNameRowTwoFields + " WITH (format='" + fileFormat + "') AS SELECT CAST(row(10, 20) AS row(x integer, y integer)) AS nested", 1);

        Path tableNameRowOneFieldLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableNameRowOneField));
        Path tableNameRowTwoFieldsLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableNameRowTwoFields));
        try (Stream<Path> files = Files.list(tableNameRowOneFieldLocation)) {
            Path file = files.filter(path -> !path.getFileName().toString().startsWith(".")).collect(onlyElement());
            Files.copy(file, tableNameRowTwoFieldsLocation.resolve(file.getFileName()));
        }

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableNameRowTwoFields + "')");

        assertThat(query("SELECT * FROM " + icebergTableNameRowTwoFields))
                .skippingTypesCheck()
                .matches("VALUES row(CAST((1,null) AS row(x integer, y integer))), row(row(10, 20))");

        assertUpdate("INSERT INTO " + icebergTableNameRowTwoFields + " VALUES (row(row(100, 200)))", 1);
        assertThat(query("SELECT * FROM " + icebergTableNameRowTwoFields))
                .skippingTypesCheck()
                .matches("VALUES row(CAST((1, null) AS row(x integer, y integer))), row(row(10, 20)), row(row(100, 200))");

        assertUpdate("DROP TABLE " + icebergTableNameRowTwoFields);
        assertUpdate("DROP TABLE " + hiveTableNameRowOneField);
    }

    public static Object[][] fileFormats()
    {
        return Stream.of(IcebergFileFormat.values())
                .map(fileFormat -> new Object[] {fileFormat})
                .toArray(Object[][]::new);
    }

    @Test
    public void testMigratePartitionedTable()
    {
        String tableName = "test_migrate_partitioned_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part_col']) AS SELECT 1 id, 'part1' part_col", 1);
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'part1')");

        // Make sure partition column is preserved
        assertThat(query("SELECT partition FROM iceberg.tpch.\"" + tableName + "$partitions\""))
                .skippingTypesCheck()
                .matches("SELECT CAST(row('part1') AS row(part_col varchar))");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2, 'part2')", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'part1'), (2, 'part2')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMigrateBucketedTable()
    {
        String tableName = "test_migrate_bucketed_table_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket'], bucket_count = 10) AS SELECT 1 bucket, 'part1' part", 1);

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        // Make sure partition column is preserved, but it's migrated as a non-bucketed table
        assertThat(query("SELECT partition FROM iceberg.tpch.\"" + tableName + "$partitions\""))
                .skippingTypesCheck()
                .matches("SELECT CAST(row('part1') AS row(part_col varchar))");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + icebergTableName))
                .contains("partitioning = ARRAY['part']");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2, 'part2')", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'part1'), (2, 'part2')");

        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    public void testMigrateTableWithRecursiveDirectory()
            throws Exception
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 x", 1);

        // Copy a file to nested directory
        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path nestedDirectory = tableLocation.resolve("nested");
        try (Stream<Path> files = Files.list(tableLocation)) {
            Path file = files.filter(path -> !path.getFileName().toString().startsWith(".")).collect(onlyElement());
            Files.createDirectory(nestedDirectory);
            Files.copy(file, nestedDirectory.resolve(file.getFileName()));
        }

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "', 'true')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (1)");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2)", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (1), (2)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMigrateTableWithoutRecursiveDirectory()
            throws Exception
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 x", 1);

        // Copy a file to nested directory
        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path nestedDirectory = tableLocation.resolve("nested");
        try (Stream<Path> files = Files.list(tableLocation)) {
            Path file = files.filter(path -> !path.getFileName().toString().startsWith(".")).collect(onlyElement());
            Files.createDirectory(nestedDirectory);
            Files.copy(file, nestedDirectory.resolve(file.getFileName()));
        }

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "', 'false')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1)");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2)", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (2)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMigrateTableFailRecursiveDirectory()
            throws Exception
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 x", 1);

        // Copy a file to nested directory
        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path nestedDirectory = tableLocation.resolve("nested");
        try (Stream<Path> files = Files.list(tableLocation)) {
            Path file = files.filter(path -> !path.getFileName().toString().startsWith(".")).collect(onlyElement());
            Files.createDirectory(nestedDirectory);
            Files.copy(file, nestedDirectory.resolve(file.getFileName()));
        }

        // The default and explicit 'fail' mode should throw an exception when nested directory exists
        assertQueryFails("CALL iceberg.system.migrate('tpch', '" + tableName + "')", "Failed to migrate table");
        assertQueryFails("CALL iceberg.system.migrate('tpch', '" + tableName + "', 'fail')", "Failed to migrate table");

        assertQuery("SELECT * FROM " + hiveTableName, "VALUES (1)");

        assertUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    public void testMigrateTablePreserveComments()
    {
        String tableName = "test_migrate_comments_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + "(col int COMMENT 'column comment') COMMENT 'table comment'");
        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat(getTableComment(tableName)).isEqualTo("table comment");
        assertThat(getColumnComment(tableName, "col")).isEqualTo("column comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'iceberg' AND schema_name = 'tpch' AND table_name = '" + tableName + "'");
    }

    private String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar("SELECT comment FROM information_schema.columns WHERE table_catalog = 'iceberg' AND table_schema = 'tpch' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
    }

    @Test
    public void testMigrateTimestampMillisTypeWithAvro()
    {
        String tableName = "test_migrate_timestamp_millis_avro" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format='AVRO') AS SELECT timestamp '2021-01-01 00:00:00.000' x", 1);
        assertQuery("SELECT * FROM " + hiveTableName, "VALUES timestamp '2021-01-01 00:00:00.000'");

        assertQueryFails("CALL iceberg.system.migrate('tpch', '" + tableName + "')", "Migrating timestamp type with Avro format is not supported.");

        assertUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    public void testMigrateUnsupportedTableFormat()
    {
        String tableName = "test_migrate_unsupported_table_format_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format = 'RCBINARY') AS SELECT 1 x", 1);

        assertThat(query("CALL iceberg.system.migrate('tpch', '" + tableName + "')"))
                .failure().hasStackTraceContaining("Unsupported storage format: RCBINARY");

        assertQuery("SELECT * FROM " + hiveTableName, "VALUES 1");
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    public void testMigrateUnsupportedTableType()
    {
        String viewName = "test_migrate_unsupported_table_type_" + randomNameSuffix();
        String trinoViewInHive = "hive.tpch." + viewName;
        String trinoViewInIceberg = "iceberg.tpch." + viewName;

        assertUpdate("CREATE VIEW " + trinoViewInHive + " AS SELECT 1 x");

        assertQueryFails(
                "CALL iceberg.system.migrate('tpch', '" + viewName + "')",
                "The procedure doesn't support migrating VIRTUAL_VIEW table type");

        assertQuery("SELECT * FROM " + trinoViewInHive, "VALUES 1");
        assertQuery("SELECT * FROM " + trinoViewInIceberg, "VALUES 1");

        assertUpdate("DROP VIEW " + trinoViewInHive);
    }

    @Test
    public void testMigrateEmptyTable()
    {
        String tableName = "test_migrate_empty_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " (col int)");

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertQuery("DESCRIBE " + icebergTableName, "VALUES ('col', 'integer', '', '')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + icebergTableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMigrateTableWithHiddenFiles()
            throws Exception
    {
        String tableName = "test_migrate_hidden_files_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 x", 1);

        // Copy a file to hidden directory
        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path hiddenDirectory = tableLocation.resolve(".hidden");
        Path nestedDirectory = tableLocation.resolve("nested");
        try (Stream<Path> files = Files.list(tableLocation)) {
            Path file = files.filter(path -> !path.getFileName().toString().startsWith(".")).collect(onlyElement());
            Files.createDirectory(hiddenDirectory);
            Files.createDirectory(nestedDirectory);
            Files.copy(file, hiddenDirectory.resolve(file.getFileName()));
            Files.copy(file, nestedDirectory.resolve(file.getFileName()));
        }

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "', 'true')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (1)");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2)", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (1), (2)");

        assertUpdate("DROP TABLE " + tableName);
    }

    private String getColumnType(String tableName, String columnName)
    {
        return (String) computeScalar(format("SELECT data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = '%s' AND column_name = '%s'",
                tableName,
                columnName));
    }
}
