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

package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestIcebergMigrateProcedure
        extends AbstractTestQueryFramework
{
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("_test_hidden");
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder().setMetastoreDirectory(dataDirectory.toFile()).build();
        queryRunner.installPlugin(new TestingHivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", dataDirectory.toString())
                        .put("hive.security", "allow-all")
                .buildOrThrow());
        return queryRunner;
    }

    @Test(dataProvider = "fileFormats")
    public void testMigrateTable(IcebergFileFormat fileFormat)
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format='" + fileFormat + "') AS SELECT 1 x", 1);
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "')");

        assertThat((String) computeScalar("SHOW CREATE TABLE " + icebergTableName))
                .contains("format = '%s'".formatted(fileFormat));

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1");
        assertQuery("SELECT count(*) FROM " + icebergTableName, "VALUES 1");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2)", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (2)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "fileFormats")
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

    @Test(dataProvider = "fileFormats")
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

    @DataProvider
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

        assertEquals(getTableComment(tableName), "table comment");
        assertEquals(getColumnComment(tableName, "col"), "column comment");

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
    public void testMigrateUnsupportedColumnType()
    {
        String tableName = "test_migrate_unsupported_column_type_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT timestamp '2021-01-01 00:00:00.000' x", 1);

        assertQueryFails(
                "CALL iceberg.system.migrate('tpch', '" + tableName + "')",
                "\\QTimestamp precision (3) not supported for Iceberg. Use \"timestamp(6)\" instead.");

        assertQuery("SELECT * FROM " + hiveTableName, "VALUES timestamp '2021-01-01 00:00:00.000'");
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    public void testMigrateUnsupportedComplexColumnType()
    {
        // TODO https://github.com/trinodb/trino/issues/17583 Add support for these complex types
        String tableName = "test_migrate_unsupported_complex_column_type_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT array[1] x", 1);
        assertQueryFails("CALL iceberg.system.migrate('tpch', '" + tableName + "')", "\\QMigrating array(integer) type is not supported");
        assertUpdate("DROP TABLE " + hiveTableName);

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT map(array['key'], array[2]) x", 1);
        assertQueryFails("CALL iceberg.system.migrate('tpch', '" + tableName + "')", "\\QMigrating map(varchar(3), integer) type is not supported");
        assertUpdate("DROP TABLE " + hiveTableName);

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT CAST(row(1) AS row(y integer)) x", 1);
        assertQueryFails("CALL iceberg.system.migrate('tpch', '" + tableName + "')", "\\QMigrating row(y integer) type is not supported");
        assertUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    public void testMigrateUnsupportedTableFormat()
    {
        String tableName = "test_migrate_unsupported_table_format_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (format = 'RCBINARY') AS SELECT 1 x", 1);

        assertThatThrownBy(() -> query("CALL iceberg.system.migrate('tpch', '" + tableName + "')"))
                .hasStackTraceContaining("Unsupported storage format: RCBINARY");

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

    private String getColumnType(String tableName, String columnName)
    {
        return (String) computeScalar(format("SELECT data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = '%s' AND column_name = '%s'",
                tableName,
                columnName));
    }
}
