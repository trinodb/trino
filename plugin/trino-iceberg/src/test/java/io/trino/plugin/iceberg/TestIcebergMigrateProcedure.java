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
import static io.trino.testing.TestingNames.randomNameSuffix;
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
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder().build();
        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
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

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1");
        assertQuery("SELECT count(*) FROM " + icebergTableName, "VALUES 1");

        assertUpdate("INSERT INTO " + icebergTableName + " VALUES (2)", 1);
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1), (2)");

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
    public void testMigrateUnsupportedBucketedTable()
    {
        String tableName = "test_migrate_unsupported_bucketed_table_" + randomNameSuffix();
        String hiveTableName = "hive.tpch." + tableName;
        String icebergTableName = "iceberg.tpch." + tableName;

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket'], bucket_count = 10) AS SELECT 1 bucket, 'test' part", 1);

        assertThatThrownBy(() -> query("CALL iceberg.system.migrate('tpch', '" + tableName + "')"))
                .hasStackTraceContaining("Cannot migrate bucketed table: [bucket]");

        assertQuery("SELECT * FROM " + hiveTableName, "VALUES (1, 'test')");
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
                "The procedure supports migrating only managed tables: .*");

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
}
