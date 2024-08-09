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
                .addIcebergProperty("hive.metastore.catalog.dir", dataDirectory.toFile().toURI().toString())
                .addIcebergProperty("iceberg.add-files-procedure.enabled", "true")
                .build();

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .put("hive.metastore.catalog.dir", dataDirectory.toFile().toURI().toString())
                .buildOrThrow());

        return queryRunner;
    }

    @Test
    void testAddFiles()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')");

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

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')");
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
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + path + "', format=>'ORC')",
                ".*NULL value not allowed for NOT NULL column: x");
        assertQueryFails(
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')",
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

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')");

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

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', '" + hiveSchemaName + "', '" + hiveTableName + "')");

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
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')",
                "Expected target 'string' type, but got source 'int' type");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesDifferentDataColumnDefinitions()
    {
        for (String format : List.of("ORC", "PARQUET", "AVRO")) {
            String hiveTableName = "test_add_files_" + randomNameSuffix();
            String icebergTableName = "test_add_files_" + randomNameSuffix();

            assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " WITH (format = '" + format + "') AS SELECT 1 x", 1);
            assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (format = '" + format + "') AS SELECT 2 y", 1);

            assertQueryFails(
                    "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')",
                    "Column 'x' does not exist");

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
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')",
                "Column 'hive_part' does not exist");

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

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['part=test1'])");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1')");

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['part=test2'])");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2')");

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['part=test3', 'part=test4'])");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')");

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
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['part=test1'])",
                "Partition filter is not supported for non-partitioned tables");

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
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['invalid_part=test1'])",
                ".*Invalid partition: invalid_part=test1");
        // Invalid partition value
        assertQueryFails(
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['part=invalid_value'])",
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

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['parent=parent1/child=child1'])");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'parent1', 'child1')");

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['parent=parent1/child=child2'])");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'parent1', 'child1'), (2, 'parent1', 'child2')");

        // TODO: Add support for partial partition filters
        assertQueryFails(
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "', ARRAY['parent=parent2'])",
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
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + tableLocation + "', format=>'ORC')",
                ".*Recursive directory must not exist when recursive_directory argument is 'fail'.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + tableLocation + "', format=>'ORC', recursive_directory=>'fail')",
                ".*Recursive directory must not exist when recursive_directory argument is 'fail'.*");

        assertUpdate("CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + tableLocation + "', format=>'ORC', recursive_directory=>'false')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + icebergTableName);

        assertUpdate("CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + tableLocation + "', format=>'ORC', recursive_directory=>'true')");
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

        assertUpdate("CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + directory + "', format=>'ORC')");

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

        assertUpdate("CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + path + "', format=>'ORC')");

        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesToPartitionTableWithLocation()
    {
        String hiveTableName = "test_add_files_location_" + randomNameSuffix();
        String icebergTableName = "test_add_files_location_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " WITH (partitioning = ARRAY['part']) AS SELECT 1 x, 'test' part", 1);
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 2 x", 1);

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch." + hiveTableName);
        String directory = Location.of(path).parentDirectory().toString();

        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + directory + "', format=>'ORC')",
                ".*The procedure does not support partitioned tables with location argument");

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
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + location + "', format=>'TEXTFILE')",
                "The procedure does not support storage format: TEXTFILE");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + icebergTableName + "', location=>'" + location + "', format=>'PARQUET')",
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

        assertQueryFails(
                "CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')",
                ".*Unsupported storage format: TEXTFILE.*");

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
                "CALL iceberg.system.add_files('tpch', '" + targetHiveTableName + "', 'tpch', '" + sourceHiveTableName + "')",
                "The target table must be Iceberg table");

        assertQuery("SELECT * FROM hive.tpch." + sourceHiveTableName, "VALUES 1");
        assertQuery("SELECT * FROM hive.tpch." + targetHiveTableName, "VALUES 2");

        assertUpdate("DROP TABLE hive.tpch." + sourceHiveTableName);
        assertUpdate("DROP TABLE hive.tpch." + targetHiveTableName);
    }

    @Test
    void testAddDuplicatedFiles()
    {
        // TODO Consider adding 'check_duplicate_files' option like Spark
        String hiveTableName = "test_add_files_" + randomNameSuffix();
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + " AS SELECT 2 x", 1);

        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')");
        assertUpdate("CALL iceberg.system.add_files('tpch', '" + icebergTableName + "', 'tpch', '" + hiveTableName + "')");

        assertQuery("SELECT * FROM iceberg.tpch." + icebergTableName, "VALUES 1, 2, 1");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesTargetTableNotFound()
    {
        String hiveTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + " AS SELECT 1 x", 1);
        assertQueryFails("CALL system.add_files('tpch', 'table_not_found', 'tpch', '" + hiveTableName + "')", "Table 'tpch.table_not_found' not found");
        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
    }

    @Test
    void testAddFilesSourceTableNotFound()
    {
        String icebergTableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + icebergTableName + "(x int)");
        assertQueryFails("CALL system.add_files('tpch', '" + icebergTableName + "', 'tpch', 'table_not_found')", "Table 'tpch.table_not_found' not found");
        assertUpdate("DROP TABLE iceberg.tpch." + icebergTableName);
    }

    @Test
    void testAddFilesLocationNotFound()
    {
        String tableName = "test_add_files_" + randomNameSuffix();

        assertUpdate("CREATE TABLE iceberg.tpch." + tableName + "(x int)");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', location=>'file:///location-not-found', format=>'ORC')",
                ".*Location not found.*");
        assertUpdate("DROP TABLE iceberg.tpch." + tableName);
    }

    @Test
    void testAddFilesInvalidArguments()
    {
        String tableName = "test_add_files_" + randomNameSuffix();
        assertUpdate("CREATE TABLE iceberg.tpch." + tableName + "(x int)");

        assertQueryFails("CALL iceberg.system.add_files(target_schema_name=>'tpch')", ".*Required procedure argument 'TARGET_TABLE_NAME' is missing");
        assertQueryFails("CALL iceberg.system.add_files(target_table_name=>'tpch')", ".*Required procedure argument 'TARGET_SCHEMA_NAME' is missing");

        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', source_schema_name=>'tpch')",
                ".*Either source schema and table names, or location and format must be provided.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', source_table_name=>'test')",
                ".*Either source schema and table names, or location and format must be provided.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', location=>'file:///tmp')",
                ".*Either source schema and table names, or location and format must be provided.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', format=>'ORC')",
                ".*Either source schema and table names, or location and format must be provided.*");

        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', source_schema_name=>'tpch', source_table_name=>'test', location=>'file:///tmp')",
                ".*LOCATION argument must be null.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', source_schema_name=>'tpch', source_table_name=>'test', format=>'ORC')",
                ".*FORMAT argument must be null.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', source_schema_name=>'tpch', location=>'file:///tmp', format=>'ORC')",
                ".*SOURCE_SCHEMA_NAME argument must be null.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', source_table_name=>'tpch', location=>'file:///tmp', format=>'ORC')",
                ".*SOURCE_TABLE_NAME argument must be null.*");
        assertQueryFails(
                "CALL iceberg.system.add_files(target_schema_name=>'tpch', target_table_name=>'" + tableName + "', partition_filter=>ARRAY['a=1'], location=>'file:///tmp', format=>'ORC')",
                ".*PARTITION_FILTER argument must be null.*");

        assertUpdate("DROP TABLE iceberg.tpch." + tableName);
    }
}
