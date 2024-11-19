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
import io.trino.filesystem.Location;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;

final class TestIcebergAddFilesProcedure
        extends AbstractTestQueryFramework
{
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("_test_hidden");
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setMetastoreDirectory(dataDirectory.toFile())
                .addIcebergProperty("iceberg.add-files-procedure.enabled", "true")
                .build();

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .buildOrThrow());

        return queryRunner;
    }

    @Test
    void testAddFilesFomTable()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertQuery("SELECT * FROM hive.tpch." + hiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesNotNull()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int NOT NULL)");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");
        assertQuery("SELECT * FROM hive.tpch." + hiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesNotNullViolation()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT CAST(NULL AS int) x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int NOT NULL)");

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                ".*NULL value not allowed for NOT NULL column: x");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                ".*NULL value not allowed for NOT NULL column: x");

        assertQueryReturnsEmptyResult("SELECT * FROM iceberg.tpch." + icebergTableName);

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesDifferentFileFormat()
    {
        testAddFilesDifferentFileFormat("PARQUET", "ORC");
        testAddFilesDifferentFileFormat("PARQUET", "AVRO");
        testAddFilesDifferentFileFormat("ORC", "PARQUET");
        testAddFilesDifferentFileFormat("ORC", "AVRO");
        testAddFilesDifferentFileFormat("AVRO", "PARQUET");
        testAddFilesDifferentFileFormat("AVRO", "ORC");
    }

    private void testAddFilesDifferentFileFormat(String hiveFormat, String icebergFormat)
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + icebergFormat + "') AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + hiveFormat + "') AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesAcrossSchema()
    {
        String hiveSchemaName = "test_schema" + randomNameSuffix();
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA hive." + hiveSchemaName);

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = 'PARQUET') AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive." + hiveSchemaName + "." + hiveTableName + " WITH (format = 'ORC') AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE tpch." + icebergTableName + " EXECUTE add_files_from_table('" + hiveSchemaName + "', '" + hiveTableName + "')");

        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        assertUpdate("DROP SCHEMA hive." + hiveSchemaName + " CASCADE ");
    }

    @Test
    void testAddFilesTypeMismatch()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = 'ORC') AS SELECT '1' x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'ORC') AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "Target 'x' column is 'string' type, but got source 'int' type");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromLessColumnTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 x, 20 y", 1);

            assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");
            assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES (1, NULL), (2, 20)");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesFromLessColumnTableNotNull()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int, y int NOT NULL) WITH (format = '" + format + "')");

            assertQueryFails(
                    "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                    ".*NULL value not allowed for NOT NULL column: y");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesFromMoreColumnTable()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x, 'extra' y", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 x", 1);

            assertQueryFails(
                    "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                    "Target table should have at least 2 columns but got 1");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesDifferentAllDataColumnDefinitions()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 y", 1);

            assertQueryFails(
                    "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                    "All columns in the source table do not exist in the target table");

            assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
            assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
        }
    }

    @Test
    void testAddFilesDifferentPartitionColumnDefinitions()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (partitioned_by = ARRAY['hive_part']) AS SELECT 1 x, 10 hive_part", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['iceberg_part']) AS SELECT 2 x, 20 iceberg_part", 1);

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['hive_part'], ARRAY['10']))",
                "Partition column 'hive_part' does not exist");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFromNonPartitionTable()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['iceberg_part']) AS SELECT 2 x, 20 iceberg_part", 1);

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "Numbers of partition columns should be equivalent. target: 1, source: 0");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesPartitionFilter()
    {
        String hiveTableName = "test_add_files_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, part varchar) WITH (partitioning = ARRAY['part'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int, part varchar) WITH (partitioned_by = ARRAY['part'])");
        assertUpdate("INSERT INTO hive.tpch." + hiveTableName + " VALUES (1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')", 4);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['test1']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1')");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['test2']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        // no-partition filter on the partitioned table
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                "partition_filter argument must be provided for partitioned tables");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        // empty partition filter on the partitioned table
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map())",
                ".* partition value count must match partition column count");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void tetAddFilesNonPartitionTableWithPartitionFilter()
    {
        String hiveTableName = "test_add_files_non_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, part varchar) WITH (partitioning = ARRAY['part'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int)");

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['test1']))",
                "Numbers of partition columns should be equivalent. target: 1, source: 0");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void tetAddFilesInvalidPartitionFilter()
    {
        String hiveTableName = "test_add_files_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, part varchar) WITH (partitioning = ARRAY['part'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int, part varchar) WITH (partitioned_by = ARRAY['part'])");

        // Invalid partition key
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['invalid_part'], ARRAY['test1']))",
                ".*Invalid partition: invalid_part=test1");
        // Invalid partition value
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['part'], ARRAY['invalid_value']))",
                ".*Invalid partition: part=invalid_value");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesNestedPartitionFilter()
    {
        String hiveTableName = "test_add_files_nested_partition_" + randomNameSuffix();
        String icebergTableName = "test_add_files_nested_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(id int, parent varchar, child varchar) WITH (partitioning = ARRAY['parent', 'child'])");
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(id int, parent varchar, child varchar) WITH (partitioned_by = ARRAY['parent', 'child'])");
        assertUpdate("INSERT INTO hive.tpch." + hiveTableName + " VALUES (1, 'parent1', 'child1'), (2, 'parent1', 'child2'), (3, 'parent2', 'child3'), (4, 'parent2', 'child4')", 4);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['parent', 'child'], ARRAY['parent1', 'child1']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'parent1', 'child1')");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['parent', 'child'], ARRAY['parent1', 'child2']))");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'parent1', 'child1'), (2, 'parent1', 'child2')");

        // TODO: Add support for partial partition filters
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "', map(ARRAY['parent'], ARRAY['parent2']))",
                ".*partition value count must match partition column count.*");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesWithRecursiveDirectory()
            throws Exception
    {
        String hiveTableName = "test_migrate_recursive_directory_" + randomNameSuffix();
        String icebergTableName = "test_migrate_recursive_directory_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'ORC') AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int)");

        // Move a file to the nested directory
        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String fileName = Location.of(path).fileName();
        Path tableLocation = dataDirectory.resolve("tpch").resolve(hiveTableName);
        Files.createDirectory(tableLocation.resolve("nested"));
        Files.move(tableLocation.resolve(fileName), tableLocation.resolve("nested").resolve(fileName));

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC')",
                ".*Recursive directory must not exist when recursive_directory argument is 'fail'.*");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC', 'fail')",
                ".*Recursive directory must not exist when recursive_directory argument is 'fail'.*");

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC', 'false')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + icebergTableName);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + tableLocation + "', 'ORC', 'true')");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesDirectoryLocation()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesFileLocation()
    {
        // Spark allows adding files from specific file location
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')");

        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesToPartitionTabtestAddFilesToPartitionTableWithLocationleWithLocation()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['part']) AS SELECT 1 x, 'test' part", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + directory + "', 'ORC')",
                ".*The procedure does not support partitioned tables");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesLocationWithWrongFormat()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'ORC') AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String location = Location.of(path).parentDirectory().toString();

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + location + "', 'TEXTFILE')",
                ".* The procedure does not support storage format: TEXTFILE");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files(location=>'" + location + "', format=>'PARQUET')",
                ".*Failed to read file footer.*");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesUnsupportedFileFormat()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT '1' x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = 'TEXTFILE') AS SELECT '2' x", 1);

        assertQueryFails("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')", ".*Unsupported storage format: TEXTFILE.*");

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES '1'");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesToNonIcebergTable()
    {
        String sourceHiveTableName = "test_add_files_" + randomNameSuffix();
        String targetHiveTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + sourceHiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE hive.tpch." + targetHiveTableName + " AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + targetHiveTableName + " EXECUTE add_files_from_table('tpch', '" + sourceHiveTableName + "')",
                "Not an Iceberg table: .*");

        assertQuery("SELECT * FROM hive.tpch." + sourceHiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM hive.tpch." + targetHiveTableName, "VALUES 2");

        assertUpdate("DROP TABLE hive.tpch." + sourceHiveTableName);
        assertUpdate("DROP TABLE hive.tpch." + targetHiveTableName);
    }

    @Test
    void testAddFilesToView()
    {
        String sourceViewName = "test_add_files_" + randomNameSuffix();
        String targetIcebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE VIEW iceberg.tpch." + sourceViewName + " AS SELECT 1 x");
        assertUpdate("CREATE TABLE iceberg.tpch." + targetIcebergTableName + " AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + targetIcebergTableName + " EXECUTE add_files_from_table('tpch', '" + sourceViewName + "')",
                "The procedure doesn't support adding files from VIRTUAL_VIEW table type");

        assertQuery("SELECT * FROM iceberg.tpch." + sourceViewName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + targetIcebergTableName, "VALUES 2");

        assertUpdate("DROP VIEW iceberg.tpch." + sourceViewName);
        assertUpdate("DROP TABLE iceberg.tpch." + targetIcebergTableName);
    }

    @Test
    void testAddFilesFromIcebergTable()
    {
        String sourceIcebergTableName = "test_add_files_" + randomNameSuffix();
        String targetIcebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + sourceIcebergTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + targetIcebergTableName + " AS SELECT 2 x", 1);

        assertQueryFails(
                "ALTER TABLE " + targetIcebergTableName + " EXECUTE add_files_from_table('tpch', '" + sourceIcebergTableName + "')",
                "Adding files from non-Hive tables is unsupported");

        assertQuery("SELECT * FROM iceberg.tpch." + sourceIcebergTableName, "VALUES 1");
        assertQuery("SELECT * FROM iceberg.tpch." + targetIcebergTableName, "VALUES 2");

        assertUpdate("DROP TABLE iceberg.tpch." + sourceIcebergTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + targetIcebergTableName);
    }

    @Test
    void testAddDuplicatedFiles()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);
        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')");

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files('" + path + "', 'ORC')",
                ".*File already exists.*");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files(location=>'" + path + "', format=>'ORC')",
                ".*File already exists.*");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddDuplicatedFilesFromTable()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);

        assertUpdate("ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')");

        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                ".*File already exists.*");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table(schema_name=>'tpch', table_name=>'" + hiveTableName + "')",
                ".*File already exists.*");
        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesTargetTableNotFound()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertQueryFails(
                "ALTER TABLE table_not_found EXECUTE add_files_from_table('tpch', '" + hiveTableName + "')",
                ".* Table 'iceberg.tpch.table_not_found' does not exist");
        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
    }

    @Test
    void testAddFilesSourceTableNotFound()
    {
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int)");
        assertQueryFails(
                "ALTER TABLE " + icebergTableName + " EXECUTE add_files_from_table('tpch', 'table_not_found')",
                "Table 'tpch.table_not_found' not found");
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesLocationNotFound()
    {
        String tableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + tableName + "(x int)");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files('file:///location-not-found', 'ORC')",
                ".*Location not found.*");
        assertUpdate("DROP TABLE iceberg.tpch." + tableName);
    }

    @Test
    void testAddFilesInvalidArguments()
    {
        String tableName = "test_add_files_" + randomNameSuffix();
        assertUpdate("CREATE TABLE iceberg.tpch." + tableName + "(x int)");

        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files_from_table(schema_name=>'tpch')",
                "Required procedure argument 'table_name' is missing");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files_from_table(table_name=>'test')",
                "Required procedure argument 'schema_name' is missing");

        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files(location=>'file:///tmp')",
                "Required procedure argument 'format' is missing");
        assertQueryFails(
                "ALTER TABLE " + tableName + " EXECUTE add_files(format=>'ORC')",
                "Required procedure argument 'location' is missing");

        assertUpdate("DROP TABLE iceberg.tpch." + tableName);
    }
}
