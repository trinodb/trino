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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().mkdirs();

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "iceberg.register-table-procedure.enabled", "true",
                "iceberg.add-files-procedure.enabled", "true",
                "iceberg.hive-catalog-name", "hive",
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true"));

        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of(
                "hive.security", "allow-all",
                "hive.metastore", "file",
                // Intentionally share the file metastore directory with Iceberg
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA tpch");

        return queryRunner;
    }

    @Test
    void testCreateV3TableAllowed()
    {
        String tableName = "test_create_v3_table_allowed_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id integer) WITH (format_version = 3)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUpgradeV2ToV3Allowed()
    {
        try (TestTable table = newTrinoTable("test_upgrade_v2_to_v3", "(id integer) WITH (format_version = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");

            // After upgrade, inserts should continue to work.
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1, 2");
        }
    }

    @Test
    void testUpgradeV1ToV3Allowed()
    {
        try (TestTable table = newTrinoTable("test_upgrade_v1_to_v3", "(id integer) WITH (format_version = 1)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");

            // After upgrade, inserts should continue to work.
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1, 2");
        }
    }

    @Test
    void testDowngradeV3ToV2Fails()
    {
        try (TestTable table = newTrinoTable("test_downgrade_v3_to_v2", "(id integer) WITH (format_version = 3)")) {
            assertThatThrownBy(() -> getQueryRunner().execute("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 2"))
                    .rootCause()
                    .hasMessageContaining("Cannot downgrade v3 table");
        }
    }

    @Test
    void testDowngradeV3ToV1Fails()
    {
        try (TestTable table = newTrinoTable("test_downgrade_v3_to_v1", "(id integer) WITH (format_version = 3)")) {
            assertThatThrownBy(() -> getQueryRunner().execute("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 1"))
                    .rootCause()
                    .hasMessageContaining("Cannot downgrade v3 table");
        }
    }

    @Test
    void testDeleteV3TableFails()
    {
        String tableName = "test_delete_v3_fails_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id integer) WITH (format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1, 2, 3", 3);

        assertThat(query("DELETE FROM " + tableName + " WHERE id = 2"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Iceberg table updates for format version 3 are not supported yet");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUpdateV3TableFails()
    {
        String tableName = "test_update_v3_fails_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id integer, v varchar) WITH (format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);

        assertThat(query("UPDATE " + tableName + " SET v = 'bb' WHERE id = 2"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Iceberg table updates for format version 3 are not supported yet");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testMergeV3TableFails()
    {
        String tableName = "test_merge_v3_fails_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id integer, v varchar) WITH (format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);

        assertThat(query(
                "MERGE INTO " + tableName + " t " +
                        "USING (VALUES (2, 'bb')) AS s(id, v) " +
                        "ON (t.id = s.id) " +
                        "WHEN MATCHED THEN UPDATE SET v = s.v"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Iceberg table updates for format version 3 are not supported yet");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testOptimizeV3TableFails()
    {
        String tableName = "test_optimize_v3_fails_" + randomNameSuffix();

        // Small table, created through Trino
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 3) AS " +
                "SELECT nationkey, name FROM tpch.tiny.nation", 25);

        // OPTIMIZE must fail for v3
        assertThat(query("ALTER TABLE " + tableName + " EXECUTE optimize"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("OPTIMIZE is not supported for Iceberg table format version > 2");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testTimestampNano()
            throws IOException
    {
        String tableName = "test_timestamp_nano_" + randomNameSuffix();
        Path tableLocation = dataDirectory.resolve(tableName);

        // Create table with timestamp_nano column using Iceberg API
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts_nano", Types.TimestampNanoType.withoutZone()));

        Table table = new HadoopTables(new Configuration(false)).create(
                schema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(FORMAT_VERSION, "3"),
                tableLocation.toString());

        // Write data with nanosecond precision
        String dataPath = tableLocation.resolve("data")
                .resolve("data-" + UUID.randomUUID() + ".parquet")
                .toString();
        try (DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            Record record = GenericRecord.create(schema);
            record.setField("id", 1);
            // 2024-01-15 12:30:45.123456789
            record.setField("ts_nano", LocalDateTime.of(2024, 1, 15, 12, 30, 45, 123456789));
            writer.write(record);
            writer.close();

            table.newFastAppend()
                    .appendFile(writer.toDataFile())
                    .commit();
        }

        // Register table in Trino and verify
        String registered = "registered_timestamp_nano_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(registered, tableLocation));

        assertThat(query("SELECT id, ts_nano FROM " + registered))
                .matches("VALUES (1, TIMESTAMP '2024-01-15 12:30:45.123456789')");

        assertUpdate("DROP TABLE " + registered);
    }

    @Test
    void testTrinoTimestampNano()
    {
        for (String format : List.of("PARQUET", "ORC", "AVRO")) {
            String tableName = "test_trino_timestamp_nano_" + randomNameSuffix();

            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, ts_nano TIMESTAMP(9)) WITH (format_version = 3, format = '" + format + "')");

            // Insert with full nanosecond precision
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2024-01-15 12:30:45.123456789')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, TIMESTAMP '2024-06-30 23:59:59.999999999')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, NULL)", 1);

            // Verify data is read back correctly with nanosecond precision preserved
            assertThat(query("SELECT id, ts_nano FROM " + tableName + " ORDER BY id"))
                    .matches("VALUES " +
                            "(INTEGER '1', TIMESTAMP '2024-01-15 12:30:45.123456789'), " +
                            "(INTEGER '2', TIMESTAMP '2024-06-30 23:59:59.999999999'), " +
                            "(INTEGER '3', NULL)");

            // Test that nanosecond precision differences are preserved
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, TIMESTAMP '2024-01-15 12:30:45.123456780')", 1);

            // Verify all rows including the one with different nanosecond precision
            assertThat(query("SELECT id, ts_nano FROM " + tableName + " ORDER BY id"))
                    .matches("VALUES " +
                            "(INTEGER '1', TIMESTAMP '2024-01-15 12:30:45.123456789'), " +
                            "(INTEGER '2', TIMESTAMP '2024-06-30 23:59:59.999999999'), " +
                            "(INTEGER '3', NULL), " +
                            "(INTEGER '4', TIMESTAMP '2024-01-15 12:30:45.123456780')");

            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testTimestampNanoWithTimeZone()
            throws IOException
    {
        String tableName = "test_timestamp_nano_tz_" + randomNameSuffix();
        Path tableLocation = dataDirectory.resolve(tableName);

        // Create table with timestamp_nano (with UTC adjustment) column using Iceberg API
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts_nano_tz", Types.TimestampNanoType.withZone()));

        Table table = new HadoopTables(new Configuration(false)).create(
                schema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(FORMAT_VERSION, "3"),
                tableLocation.toString());

        // Write data with nanosecond precision
        String dataPath = tableLocation.resolve("data")
                .resolve("data-" + UUID.randomUUID() + ".parquet")
                .toString();
        try (DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            Record record = GenericRecord.create(schema);
            record.setField("id", 1);
            // 2024-01-15 12:30:45.123456789 UTC
            record.setField("ts_nano_tz", OffsetDateTime.of(2024, 1, 15, 12, 30, 45, 123456789, ZoneOffset.UTC));
            writer.write(record);
            writer.close();

            table.newFastAppend()
                    .appendFile(writer.toDataFile())
                    .commit();
        }

        // Register table in Trino and verify
        String registered = "registered_timestamp_nano_tz_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(registered, tableLocation));

        assertThat(query("SELECT id, ts_nano_tz FROM " + registered))
                .matches("VALUES (1, TIMESTAMP '2024-01-15 12:30:45.123456789 UTC')");

        assertUpdate("DROP TABLE " + registered);
    }

    @Test
    void testTrinoTimestampNanoWithTimeZone()
    {
        for (String format : List.of("PARQUET", "ORC", "AVRO")) {
            String tableName = "test_trino_timestamp_nano_tz_" + randomNameSuffix();

            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, ts_nano_tz TIMESTAMP(9) WITH TIME ZONE) WITH (format_version = 3, format = '" + format + "')");

            // Insert with full nanosecond precision
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2024-01-15 12:30:45.123456789 UTC')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, TIMESTAMP '2024-06-30 23:59:59.999999999 UTC')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, NULL)", 1);
            // Insert with non-UTC timezone - should be normalized to UTC when read back
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, TIMESTAMP '2024-01-15 18:00:45.123456789 +05:30')", 1);

            // Verify data is read back correctly with nanosecond precision preserved
            // Note: row 4 was inserted as +05:30 but reads back as UTC
            assertThat(query("SELECT id, ts_nano_tz FROM " + tableName + " ORDER BY id"))
                    .matches("VALUES " +
                            "(INTEGER '1', TIMESTAMP '2024-01-15 12:30:45.123456789 UTC'), " +
                            "(INTEGER '2', TIMESTAMP '2024-06-30 23:59:59.999999999 UTC'), " +
                            "(INTEGER '3', NULL), " +
                            "(INTEGER '4', TIMESTAMP '2024-01-15 12:30:45.123456789 UTC')");

            // Test that nanosecond precision differences are preserved
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, TIMESTAMP '2024-01-15 12:30:45.123456780 UTC')", 1);

            // Verify all rows including the one with different nanosecond precision
            assertThat(query("SELECT id, ts_nano_tz FROM " + tableName + " ORDER BY id"))
                    .matches("VALUES " +
                            "(INTEGER '1', TIMESTAMP '2024-01-15 12:30:45.123456789 UTC'), " +
                            "(INTEGER '2', TIMESTAMP '2024-06-30 23:59:59.999999999 UTC'), " +
                            "(INTEGER '3', NULL), " +
                            "(INTEGER '4', TIMESTAMP '2024-01-15 12:30:45.123456789 UTC'), " +
                            "(INTEGER '5', TIMESTAMP '2024-01-15 12:30:45.123456780 UTC')");

            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testV3RejectsAddFilesProcedure()
    {
        String tableName = "add_files_target_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (x integer) WITH (format = 'ORC', format_version = 3)");

        assertThat(query("ALTER TABLE " + tableName + " EXECUTE add_files(location => 'file:///tmp', format => 'ORC')"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("ADD_FILES is not supported for Iceberg table format version > 2.");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testV3RejectsAddFilesFromTableProcedure()
    {
        String tableName = "add_files_from_table_target_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (x integer) WITH (format = 'ORC', format_version = 3)");

        assertThat(query("ALTER TABLE " + tableName + " EXECUTE add_files_from_table(schema_name => 'tpch', table_name => 'non_existent')"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("ADD_FILES_FROM_TABLE is not supported for Iceberg table format version > 2.");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testV3InitialDefault()
            throws IOException
    {
        // Create a data file with only 'id' column
        String temp = "tmp_v3_defaults_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + temp);
        long dataFileSize = getFileSize(dataFilePath);

        String hadoopTableName = "hadoop_v3_defaults_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        // Create a v3 table with two columns: id (exists in file) and value (missing from file, has initial-default)
        Schema schemaWithInitialDefault = new Schema(
                Types.NestedField.optional("id")
                        .withId(1)
                        .ofType(Types.IntegerType.get())
                        .build(),
                Types.NestedField.optional("value")
                        .withId(2)
                        .ofType(Types.IntegerType.get())
                        .withInitialDefault(Expressions.lit(42))
                        .build());

        Table icebergTable = new HadoopTables(new Configuration(false)).create(
                schemaWithInitialDefault,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "ORC"),
                hadoopTableLocation.toString());

        icebergTable.newFastAppend()
                .appendFile(orcDataFile(dataFilePath, dataFileSize))
                .commit();

        String registered = "registered_v3_defaults_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')"
                .formatted(registered, hadoopTableLocation));

        // The 'value' column is missing from the data file, so it should return the initial-default (42)
        assertQuery("SELECT id, value FROM " + registered, "VALUES (1, 42)");

        assertUpdate("DROP TABLE " + registered);
        assertUpdate("DROP TABLE " + temp);
    }

    @Test
    void testV3WriteDefault()
            throws IOException
    {
        // Create a data file with only 'id' column
        String temp = "tmp_v3_write_defaults_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + temp);
        long dataFileSize = getFileSize(dataFilePath);

        String hadoopTableName = "hadoop_v3_write_defaults_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        // Create a v3 table with a column that has write-default (but not initial-default)
        // Note: write-default is used for INSERT, initial-default is used for reading missing columns
        Schema schemaWithWriteDefault = new Schema(
                Types.NestedField.optional("id")
                        .withId(1)
                        .ofType(Types.IntegerType.get())
                        .build(),
                Types.NestedField.optional("value")
                        .withId(2)
                        .ofType(Types.IntegerType.get())
                        .withWriteDefault(Expressions.lit(99))
                        .build());

        Table icebergTable = new HadoopTables(new Configuration(false)).create(
                schemaWithWriteDefault,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "ORC"),
                hadoopTableLocation.toString());

        icebergTable.newFastAppend()
                .appendFile(orcDataFile(dataFilePath, dataFileSize))
                .commit();

        String registered = "registered_v3_write_defaults_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')"
                .formatted(registered, hadoopTableLocation));

        // The 'value' column is missing from the data file and has no initial-default, so it should return NULL
        // (write-default is only used for INSERT, not for reading missing columns)
        assertQuery("SELECT id, value FROM " + registered, "VALUES (1, NULL)");

        assertUpdate("DROP TABLE " + registered);
        assertUpdate("DROP TABLE " + temp);
    }

    @Test
    void testWriteDefaultOnInsert()
    {
        String hadoopTableName = "hadoop_v3_write_default_insert_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        // Create a v3 table with write-default on column 'b'
        Schema schemaWithWriteDefault = new Schema(
                Types.NestedField.optional("a")
                        .withId(1)
                        .ofType(Types.IntegerType.get())
                        .build(),
                Types.NestedField.optional("b")
                        .withId(2)
                        .ofType(Types.IntegerType.get())
                        .withWriteDefault(Expressions.lit(42))
                        .build());

        new HadoopTables(new Configuration(false)).create(
                schemaWithWriteDefault,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "ORC"),
                hadoopTableLocation.toString());

        String tableName = "test_write_default_insert_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')"
                .formatted(tableName, hadoopTableLocation));

        // Verify SHOW CREATE TABLE shows DEFAULT
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("b integer DEFAULT 42");

        // Insert without column b should use write-default
        assertUpdate("INSERT INTO " + tableName + " (a) VALUES (1)", 1);
        assertQuery("SELECT a, b FROM " + tableName, "VALUES (1, 42)");

        // Insert with explicit column b should use the provided value
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (2, 100)", 1);
        assertQuery("SELECT a, b FROM " + tableName + " WHERE a = 2", "VALUES (2, 100)");

        // Explicit NULL should write NULL, not default
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (3, NULL)", 1);
        assertQuery("SELECT a, b FROM " + tableName + " WHERE a = 3", "VALUES (3, NULL)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateTableWithDefault()
    {
        String tableName = "test_create_table_with_default_" + randomNameSuffix();

        // Create a v3 table with DEFAULT values using Trino DDL
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INTEGER, " +
                "int_col INTEGER DEFAULT 42, " +
                "varchar_col VARCHAR DEFAULT 'hello', " +
                "double_col DOUBLE DEFAULT 3.14" +
                ") WITH (format_version = 3)");

        // Verify SHOW CREATE TABLE shows the defaults
        String showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).contains("int_col integer DEFAULT 42");
        assertThat(showCreate).contains("varchar_col varchar DEFAULT 'hello'");
        assertThat(showCreate).contains("double_col double DEFAULT DOUBLE '3.14'");

        // Insert without default columns - should use defaults
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
        assertQuery("SELECT id, int_col, varchar_col, double_col FROM " + tableName,
                "VALUES (1, 42, 'hello', 3.14)");

        // Insert with explicit values
        assertUpdate("INSERT INTO " + tableName + " (id, int_col, varchar_col, double_col) VALUES (2, 100, 'world', 2.71)", 1);
        assertQuery("SELECT id, int_col, varchar_col, double_col FROM " + tableName + " WHERE id = 2",
                "VALUES (2, 100, 'world', 2.71)");

        // Insert with explicit NULL
        assertUpdate("INSERT INTO " + tableName + " (id, int_col) VALUES (3, NULL)", 1);
        assertQuery("SELECT id, int_col FROM " + tableName + " WHERE id = 3",
                "VALUES (3, NULL)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testAddColumnWithDefault()
    {
        String tableName = "test_add_column_with_default_" + randomNameSuffix();

        // Create a v3 table and insert initial data
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1), (2)", 2);

        // Add a column with default
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN new_col INTEGER DEFAULT 99");

        // Verify SHOW CREATE TABLE shows the default
        String showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).contains("new_col integer DEFAULT 99");

        // Existing rows should see the initial-default (99) for the new column
        assertQuery("SELECT id, new_col FROM " + tableName + " ORDER BY id",
                "VALUES (1, 99), (2, 99)");

        // Insert new row without specifying new_col - should use write-default
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (3)", 1);
        assertQuery("SELECT id, new_col FROM " + tableName + " WHERE id = 3",
                "VALUES (3, 99)");

        // Insert with explicit value
        assertUpdate("INSERT INTO " + tableName + " (id, new_col) VALUES (4, 200)", 1);
        assertQuery("SELECT id, new_col FROM " + tableName + " WHERE id = 4",
                "VALUES (4, 200)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateTableWithDefaultVariousTypes()
    {
        String tableName = "test_create_table_default_types_" + randomNameSuffix();

        // Create a v3 table with various types of default values
        // Covers: BOOLEAN, BIGINT, REAL, DECIMAL, DATE, TIME, TIMESTAMP, UUID, VARBINARY
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INTEGER, " +
                "bool_col BOOLEAN DEFAULT true, " +
                "bigint_col BIGINT DEFAULT 9223372036854775807, " +
                "real_col REAL DEFAULT 1.5, " +
                "decimal_col DECIMAL(10,2) DEFAULT 123.45, " +
                "date_col DATE DEFAULT DATE '2020-06-15', " +
                "time_col TIME(6) DEFAULT TIME '12:34:56.123456', " +
                "timestamp_col TIMESTAMP(6) DEFAULT TIMESTAMP '2020-06-15 12:34:56.123456', " +
                "uuid_col UUID DEFAULT UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', " +
                "binary_col VARBINARY DEFAULT X'48454C4C4F'" +
                ") WITH (format_version = 3)");

        // Verify SHOW CREATE TABLE displays the defaults correctly
        String showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).contains("bool_col boolean DEFAULT true");
        assertThat(showCreate).contains("bigint_col bigint DEFAULT 9223372036854775807");
        assertThat(showCreate).contains("real_col real DEFAULT REAL '1.5'");
        assertThat(showCreate).contains("decimal_col decimal(10, 2) DEFAULT DECIMAL '123.45'");
        assertThat(showCreate).contains("date_col date DEFAULT DATE '2020-06-15'");
        assertThat(showCreate).contains("time_col time(6) DEFAULT TIME '12:34:56.123456'");
        assertThat(showCreate).contains("timestamp_col timestamp(6) DEFAULT TIMESTAMP '2020-06-15 12:34:56.123456'");
        assertThat(showCreate).contains("uuid_col uuid DEFAULT UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
        assertThat(showCreate).contains("binary_col varbinary DEFAULT X'48454C4C4F'");

        // Insert without default columns
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);

        // Verify all defaults are applied
        assertQuery(
                "SELECT id, bool_col, bigint_col, real_col, decimal_col, date_col, time_col, timestamp_col, uuid_col, binary_col FROM " + tableName,
                "SELECT " +
                        "CAST(1 AS INTEGER), " +
                        "true, " +
                        "9223372036854775807, " +
                        "CAST(1.5 AS REAL), " +
                        "CAST(123.45 AS DECIMAL(10,2)), " +
                        "DATE '2020-06-15', " +
                        "TIME '12:34:56.123456', " +
                        "TIMESTAMP '2020-06-15 12:34:56.123456', " +
                        "UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', " +
                        "X'48454C4C4F'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateTableWithDefaultNull()
    {
        String tableName = "test_default_null_" + randomNameSuffix();

        // Create table with DEFAULT NULL - this should be allowed
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, value INTEGER DEFAULT NULL) WITH (format_version = 3)");

        // Insert without value column
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
        assertQuery("SELECT id, value FROM " + tableName, "VALUES (1, NULL)");

        // Insert with explicit value
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 42)", 1);
        assertQuery("SELECT id, value FROM " + tableName + " WHERE id = 2", "VALUES (2, 42)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testAlterColumnSetDefault()
    {
        String tableName = "test_alter_column_set_default_" + randomNameSuffix();

        // Create a v3 table without default values
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, value INTEGER) WITH (format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10)", 1);

        // Initially no default on 'value' column
        String showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).doesNotContain("DEFAULT");

        // Set a default value on 'value' column
        assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN value SET DEFAULT 42");

        // Verify SHOW CREATE TABLE shows the new default
        showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).contains("value integer DEFAULT 42");

        // Insert without 'value' - should now use the new default
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (2)", 1);
        assertQuery("SELECT id, value FROM " + tableName + " WHERE id = 2", "VALUES (2, 42)");

        // Change the default to a different value
        assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN value SET DEFAULT 100");

        showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).contains("value integer DEFAULT 100");

        // Insert should use the new default
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (3)", 1);
        assertQuery("SELECT id, value FROM " + tableName + " WHERE id = 3", "VALUES (3, 100)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testAlterColumnDropDefault()
    {
        String tableName = "test_alter_column_drop_default_" + randomNameSuffix();

        // Create a v3 table with a default value
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, value INTEGER DEFAULT 42) WITH (format_version = 3)");

        // Verify default is present
        String showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).contains("value integer DEFAULT 42");

        // Insert without 'value' - should use the default
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
        assertQuery("SELECT id, value FROM " + tableName + " WHERE id = 1", "VALUES (1, 42)");

        // Drop the default value
        assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN value DROP DEFAULT");

        // Verify default is no longer shown
        showCreate = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        assertThat(showCreate).doesNotContain("DEFAULT");

        // Insert without 'value' - should now use NULL (no default)
        assertUpdate("INSERT INTO " + tableName + " (id) VALUES (2)", 1);
        assertQuery("SELECT id, value FROM " + tableName + " WHERE id = 2", "VALUES (2, NULL)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testV3InsertProducesRowLineageMetadata()
    {
        String tableName = "test_v3_insert_lineage_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, v VARCHAR) WITH (format = 'PARQUET', format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

        BaseTable table = loadTable(tableName);
        table.refresh();

        Snapshot snapshot = table.currentSnapshot();
        assertThat(snapshot).isNotNull();

        TableMetadata metadata = table.operations().current();
        assertThat(metadata.formatVersion()).isEqualTo(3);
        assertThat(metadata.nextRowId()).isGreaterThan(0L);

        int fileCount = 0;
        long totalRecords = 0;

        for (DataFile file : snapshot.addedDataFiles(table.io())) {
            fileCount++;
            totalRecords += file.recordCount();

            // These are the lineage “inputs” Iceberg uses to materialize _row_id and last_updated_sequence_number
            assertThat(file.firstRowId()).as("data file firstRowId must be set in v3").isNotNull();
            assertThat(file.dataSequenceNumber()).as("data file dataSequenceNumber must be set").isNotNull();
        }

        assertThat(fileCount).isGreaterThan(0);
        assertThat(totalRecords).isEqualTo(3);

        assertUpdate("DROP TABLE " + tableName);
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }

    @Test
    void testV3RejectsEncryptionKeyProperty()
    {
        String tableName = "test_v3_encryption_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (format = 'ORC', format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        // Set encryption.key-id property via Iceberg API
        BaseTable icebergTable = loadTable(tableName);
        icebergTable.updateProperties()
                .set("encryption.key-id", "test_key")
                .commit();

        assertQueryFails(
                "SELECT * FROM " + tableName,
                ".*Iceberg table encryption is not supported.*");

        // Also verify INSERT fails with encryption key set
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES 2",
                ".*Iceberg table encryption is not supported.*");

        // Clean up by removing the property first
        icebergTable.updateProperties()
                .remove("encryption.key-id")
                .commit();
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testV3RejectsEncryptionKeysInMetadata()
            throws Exception
    {
        String temp = "tmp_v3_encryption_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + temp);
        long dataFileSize = getFileSize(dataFilePath);

        String hadoopTableName = "hadoop_v3_encryption_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));

        Table icebergTable = new HadoopTables(new Configuration(false)).create(
                schema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "ORC"),
                hadoopTableLocation.toString());

        icebergTable.newFastAppend()
                .appendFile(orcDataFile(dataFilePath, dataFileSize))
                .commit();

        // Inject encryption-keys + snapshot key-id into the current metadata.json.
        injectEncryptionKeysIntoMetadataJson(hadoopTableLocation, "k1");

        String registered = "registered_v3_encryption_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')"
                .formatted(registered, hadoopTableLocation));

        assertQueryFails(
                "SELECT * FROM " + registered,
                ".*Iceberg table encryption is not supported.*");

        // Use unregister_table instead of DROP TABLE because DROP TABLE triggers the same validation error
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '%s')".formatted(registered));
        assertUpdate("DROP TABLE " + temp);
        deleteRecursively(hadoopTableLocation, ALLOW_INSECURE);
    }

    @Test
    void testV3RejectsDeletionVectorsPuffinDeleteFile()
            throws IOException
    {
        String temp = "tmp_v3_dv_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + temp);
        long dataFileSize = getFileSize(dataFilePath);

        String hadoopTableName = "hadoop_v3_dv_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));

        Table icebergTable = new HadoopTables(new Configuration(false)).create(
                schema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "ORC"),
                hadoopTableLocation.toString());

        DataFile dataFile = orcDataFile(dataFilePath, dataFileSize);
        icebergTable.newFastAppend()
                .appendFile(dataFile)
                .commit();

        // Create a "fake" deletion-vector delete file entry:
        // - format = PUFFIN (this is what Trino rejects)
        // - referencedDataFile/contentOffset/contentSizeInBytes populated enough for Iceberg to accept it
        // We intentionally do not create an actual puffin file; Trino will fail before reading it.
        String puffinPath = hadoopTableLocation.resolve("data").resolve("dv-" + randomNameSuffix() + ".puffin").toString();
        DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(puffinPath)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(123)
                .withRecordCount(0)
                .withReferencedDataFile(dataFile.location())
                .withContentOffset(0)
                .withContentSizeInBytes(0)
                .build();

        icebergTable.newRowDelta()
                .addDeletes(puffinDeleteFile)
                .commit();

        String registered = "registered_v3_dv_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')"
                .formatted(registered, hadoopTableLocation));

        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM " + registered))
                .rootCause()
                .hasMessageContaining("Iceberg deletion vector is not supported yet");

        // Use unregister_table instead of DROP TABLE to avoid deleting the underlying files before deleteRecursively
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '%s')".formatted(registered));
        deleteRecursively(hadoopTableLocation, ALLOW_INSECURE);

        assertUpdate("DROP TABLE " + temp);
    }

    private static DataFile orcDataFile(String dataFilePath, long size)
    {
        return org.apache.iceberg.DataFiles.builder(PartitionSpec.unpartitioned())
                .withFormat(FileFormat.ORC)
                .withInputFile(localInput(new java.io.File(dataFilePath)))
                .withPath(dataFilePath)
                .withFileSizeInBytes(size)
                .withRecordCount(1)
                .build();
    }

    private static void injectEncryptionKeysIntoMetadataJson(Path tableLocation, String keyId)
            throws IOException
    {
        Path metadataFile = latestMetadataJson(tableLocation);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = (ObjectNode) mapper.readTree(metadataFile.toFile());

        // Add "encryption-keys" - any valid base64 is fine for this test; we only care that Iceberg parses it.
        ObjectNode key = mapper.createObjectNode();
        key.put("key-id", keyId);
        key.put("encrypted-key-metadata", "AA==");
        ArrayNode keys = mapper.createArrayNode();
        keys.add(key);
        root.set("encryption-keys", keys);

        // Set current snapshot's "key-id"
        JsonNode currentSnapshotIdNode = root.get("current-snapshot-id");
        if (currentSnapshotIdNode != null && currentSnapshotIdNode.isNumber()) {
            long currentSnapshotId = currentSnapshotIdNode.asLong();
            ArrayNode snapshots = (ArrayNode) root.get("snapshots");
            if (snapshots != null) {
                for (JsonNode snapshotNode : snapshots) {
                    JsonNode snapshotIdNode = snapshotNode.get("snapshot-id");
                    if (snapshotIdNode != null && snapshotIdNode.asLong() == currentSnapshotId) {
                        ((ObjectNode) snapshotNode).put("key-id", keyId);
                        break;
                    }
                }
            }
        }

        Files.writeString(metadataFile, mapper.writeValueAsString(root));
        // delete the crc file, since it is no longer valid
        Path crc = metadataFile.resolveSibling("." + metadataFile.getFileName() + ".crc");
        Files.deleteIfExists(crc);
    }

    private static Path latestMetadataJson(Path tableLocation)
            throws IOException
    {
        Path metadataDir = tableLocation.resolve("metadata");
        try (var stream = Files.list(metadataDir)) {
            return stream
                    .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
                    .max(Comparator.naturalOrder())
                    .orElseThrow(() -> new IllegalStateException("No metadata.json found in " + metadataDir));
        }
    }

    private long getFileSize(String dataFilePath)
            throws IOException
    {
        return getFileSystemFactory(getQueryRunner())
                .create(ConnectorIdentity.ofUser("test"))
                .newInputFile(Location.of(dataFilePath))
                .length();
    }

    @Test
    void testOrcTimestampNanoFiltering()
    {
        String tableName = "test_orc_timestamp_nano_filtering_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (d TIMESTAMP(9), b INTEGER) WITH (format_version = 3, format = 'ORC')");

        // Insert data with nanosecond precision
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(TIMESTAMP '2024-01-15 10:00:00.000000001', 1)," +
                "(TIMESTAMP '2024-01-15 10:59:59.999999999', 2)," +
                "(TIMESTAMP '2024-01-15 11:00:00.000000001', 3)," +
                "(TIMESTAMP '2024-01-15 11:30:45.123456789', 4)", 4);

        // Debug: Check what's actually in the table
        assertThat(query("SELECT d, b FROM " + tableName + " ORDER BY b"))
                .matches("VALUES " +
                        "(TIMESTAMP '2024-01-15 10:00:00.000000001', INTEGER '1'), " +
                        "(TIMESTAMP '2024-01-15 10:59:59.999999999', INTEGER '2'), " +
                        "(TIMESTAMP '2024-01-15 11:00:00.000000001', INTEGER '3'), " +
                        "(TIMESTAMP '2024-01-15 11:30:45.123456789', INTEGER '4')");

        // Test filter at hour boundary - this is the failing case
        assertThat(query("SELECT b FROM " + tableName + " WHERE d >= TIMESTAMP '2024-01-15 11:00:00.000000000' ORDER BY b"))
                .matches("VALUES INTEGER '3', INTEGER '4'");

        // Test filter with slightly later timestamp
        assertThat(query("SELECT b FROM " + tableName + " WHERE d >= TIMESTAMP '2024-01-15 11:00:00.000000001' ORDER BY b"))
                .matches("VALUES INTEGER '3', INTEGER '4'");

        // Test filter that should return all rows
        assertThat(query("SELECT b FROM " + tableName + " WHERE d >= TIMESTAMP '2024-01-15 10:00:00.000000000' ORDER BY b"))
                .matches("VALUES INTEGER '1', INTEGER '2', INTEGER '3', INTEGER '4'");

        // Test filter that should return first two rows
        assertThat(query("SELECT b FROM " + tableName + " WHERE d < TIMESTAMP '2024-01-15 11:00:00.000000000' ORDER BY b"))
                .matches("VALUES INTEGER '1', INTEGER '2'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testOrcTimestampNanoWithTimeZoneFiltering()
    {
        String tableName = "test_orc_timestamp_nano_tz_filtering_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (d TIMESTAMP(9) WITH TIME ZONE, b INTEGER) WITH (format_version = 3, format = 'ORC')");

        // Insert data with nanosecond precision
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(TIMESTAMP '2024-01-15 10:00:00.000000001 UTC', 1)," +
                "(TIMESTAMP '2024-01-15 10:59:59.999999999 UTC', 2)," +
                "(TIMESTAMP '2024-01-15 11:00:00.000000001 UTC', 3)," +
                "(TIMESTAMP '2024-01-15 11:30:45.123456789 UTC', 4)", 4);

        // Debug: Check what's actually in the table
        assertThat(query("SELECT d, b FROM " + tableName + " ORDER BY b"))
                .matches("VALUES " +
                        "(TIMESTAMP '2024-01-15 10:00:00.000000001 UTC', INTEGER '1'), " +
                        "(TIMESTAMP '2024-01-15 10:59:59.999999999 UTC', INTEGER '2'), " +
                        "(TIMESTAMP '2024-01-15 11:00:00.000000001 UTC', INTEGER '3'), " +
                        "(TIMESTAMP '2024-01-15 11:30:45.123456789 UTC', INTEGER '4')");

        // Test filter at hour boundary - this is the failing case
        assertThat(query("SELECT b FROM " + tableName + " WHERE d >= TIMESTAMP '2024-01-15 11:00:00.000000000 UTC' ORDER BY b"))
                .matches("VALUES INTEGER '3', INTEGER '4'");

        assertUpdate("DROP TABLE " + tableName);
    }
}
