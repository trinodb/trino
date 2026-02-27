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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;
    private TrinoCatalog catalog;

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

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
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
        catalog = getTrinoCatalog(metastore, fileSystemFactory, "iceberg");

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
    void testDeleteV3Table()
    {
        try (TestTable table = newTrinoTable("test_delete_v3", "(id integer) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3, 4, 5", 5);

            // First delete - creates initial DV
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (INTEGER '1'), (INTEGER '3'), (INTEGER '4'), (INTEGER '5')");

            // Second delete - merges with existing DV
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 4", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (INTEGER '1'), (INTEGER '3'), (INTEGER '5')");

            // Third delete - another merge
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (INTEGER '3'), (INTEGER '5')");
        }
    }

    @Test
    void testUpdateV3Table()
    {
        try (TestTable table = newTrinoTable("test_update_v3", "(id integer, v varchar) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'b')", 2);

            assertUpdate("UPDATE " + table.getName() + " SET v = 'bb' WHERE id = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (INTEGER '1', VARCHAR 'a'), (INTEGER '2', VARCHAR 'bb')");
        }
    }

    @Test
    void testMergeV3Table()
    {
        try (TestTable table = newTrinoTable("test_merge_v3", "(id integer, v varchar) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'b')", 2);

            assertUpdate("MERGE INTO " + table.getName() + " t USING (VALUES (2, 'bb')) AS s(id, v) ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET v = s.v", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (INTEGER '1', VARCHAR 'a'), (INTEGER '2', VARCHAR 'bb')");
        }
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
    {
        // Create a data file with only 'id' column
        String tableName = "v3_defaults_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (format_version = 3, format = 'ORC')");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        // Add a value column (missing from file, has initial-default)
        loadTable(tableName).updateSchema()
                .addColumn("value", Types.IntegerType.get(), Expressions.lit(42))
                .commit();

        // The 'value' column is missing from the data file, so it should return the initial-default (42)
        assertQuery("SELECT id, value FROM " + tableName, "VALUES (1, 42)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testV3WriteDefault()
    {
        // Create a data file with only 'id' column
        String temp = "tmp_v3_write_defaults_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        // Create a v3 table with a column that has write-default (but not initial-default)
        // Note: write-default is used for INSERT, initial-default is used for reading missing columns
        String tableName = "v3_write_defaults_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);
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

        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        schemaWithWriteDefault,
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of("format-version", "3"))
                .commitTransaction();

        BaseTable tempTable = loadTable(temp);
        loadTable(tableName).newFastAppend()
                .appendFile(getOnlyElement(tempTable.currentSnapshot().addedDataFiles(tempTable.io())))
                .commit();

        // The 'value' column is missing from the data file and has no initial-default, so it should return NULL
        // (write-default is only used for INSERT, not for reading missing columns)
        assertQuery("SELECT id, value FROM " + tableName, "VALUES (1, NULL)");

        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("DROP TABLE " + temp);
    }

    @Test
    void testWriteDefaultOnInsert()
    {
        String tableName = "test_write_default_insert_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

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

        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        schemaWithWriteDefault,
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of("format-version", "3"))
                .commitTransaction();

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
        Table tempTable = loadTable(temp);

        String hadoopTableName = "hadoop_v3_encryption_" + randomNameSuffix();
        Path hadoopTableLocation = Path.of(tempTable.location()).resolveSibling(hadoopTableName);

        // Use HadoopTables to prevent stale caches from direct metadata.json modification
        Table icebergTable = new HadoopTables(new Configuration(false)).create(
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "ORC"),
                hadoopTableLocation.toString());

        icebergTable.newFastAppend()
                .appendFile(getOnlyElement(tempTable.currentSnapshot().addedDataFiles(tempTable.io())))
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
    void testIcebergWritesAndTrinoReadsDeletionVector()
            throws Exception
    {
        String tableName = "deletion_vector" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (format_version = 3)");

        Table icebergTable = loadTable(tableName);

        // Write a data file with 5 rows: ids 0 to 4
        String dataPath = Path.of(icebergTable.location()).resolve("data")
                .resolve("data-" + UUID.randomUUID() + ".parquet")
                .toString();
        try (DataWriter<Record> writer = Parquet.writeData(icebergTable.io().newOutputFile(dataPath))
                .forTable(icebergTable)
                .withSpec(icebergTable.spec())
                .withPartition(null) // unpartitioned
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            Record record = GenericRecord.create(icebergTable.schema());
            for (int i = 0; i < 5; i++) {
                record.setField("id", i);
                writer.write(record);
            }
            writer.close();

            icebergTable.newFastAppend()
                    .appendFile(writer.toDataFile())
                    .commit();
        }

        // Write a deletion vector for rows 0 and 3
        try (DVFileWriter dvWriter = new BaseDVFileWriter(
                OutputFileFactory.builderFor(icebergTable, 1, 1).format(FileFormat.PUFFIN).build(),
                _ -> PositionDeleteIndex.empty())) {
            dvWriter.delete(dataPath, 0L, icebergTable.spec(), null);
            dvWriter.delete(dataPath, 3L, icebergTable.spec(), null);
            dvWriter.close();

            icebergTable.newRowDelta()
                    .addDeletes(getOnlyElement(dvWriter.result().deleteFiles()))
                    .commit();
        }

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1), (2), (4)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testTrinoWritesAndReadsDeletionVector()
    {
        try (TestTable table = newTrinoTable("trino_v3_dv_delete", "(id INTEGER) WITH (format = 'PARQUET', format_version = 3)")) {
            // 1000 rows: 1..1000
            for (int i = 0; i < 10; i++) {
                assertUpdate("INSERT INTO " + table.getName() + " SELECT x FROM UNNEST(sequence(%s, %s)) t(x)".formatted(i * 100 + 1, (i + 1) * 100), 100);
            }

            // verify insert
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1000', INTEGER '1', INTEGER '1000', BIGINT '500', BIGINT '200', BIGINT '10')");

            // delete nothing
            assertUpdate("DELETE FROM " + table.getName() + " WHERE random() > 1", 0);

            // verify nothing deleted and no deletion vectors created
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1000', INTEGER '1', INTEGER '1000', BIGINT '500', BIGINT '200', BIGINT '10')");
            assertThat(query("SELECT count(*) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '0')");

            // delete evens => 500 rows removed
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 2 = 0", 500);

            // verify delete
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '500', INTEGER '1', INTEGER '999', BIGINT '0', BIGINT '100', BIGINT '10')");

            // Check DV via $files: cardinality 500, 10 PUFFIN entries, 1 file
            assertThat(query("SELECT sum(record_count), count(*), count_if(file_format = 'PUFFIN'), count(distinct file_path) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '500', BIGINT '10', BIGINT '10', BIGINT '1')");

            // delete multiples of 5 => 100 rows removed
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 5 = 0", 100);

            // verify delete
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '400', INTEGER '1', INTEGER '999', BIGINT '0', BIGINT '0', BIGINT '10')");

            // Check DV via $files: cardinality 600, 10 PUFFIN entries, 1 file
            assertThat(query("SELECT sum(record_count), count(*), count_if(file_format = 'PUFFIN'), count(distinct file_path) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '600', BIGINT '10', BIGINT '10', BIGINT '1')");
        }
    }

    @Test
    void testTrinoWritesAndReadsDeletionVectorPartitioned()
    {
        try (TestTable table = newTrinoTable("trino_v3_dv_delete_part", "(id INTEGER) WITH (format = 'PARQUET', format_version = 3, partitioning = ARRAY['bucket(id, 7)'])")) {
            for (int i = 0; i < 10; i++) {
                assertUpdate("INSERT INTO " + table.getName() + " SELECT x FROM UNNEST(sequence(%s, %s)) t(x)".formatted(i * 100 + 1, (i + 1) * 100), 100);
            }

            // verify insert
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1000', INTEGER '1', INTEGER '1000', BIGINT '500', BIGINT '200', BIGINT '70')");

            // delete evens => 500 rows removed
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 2 = 0", 500);

            // verify delete
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '500', INTEGER '1', INTEGER '999', BIGINT '0', BIGINT '100', BIGINT '70')");

            // Check DV via $files: cardinality 500, PUFFIN delete entry for each data file
            assertThat(query("SELECT sum(record_count), count(*), count_if(file_format = 'PUFFIN'), count(distinct file_path) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '500', BIGINT '70', BIGINT '70', BIGINT '1')");

            // delete multiples of 5 => 100 rows removed
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 5 = 0", 100);

            // verify delete
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 2 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '400', INTEGER '1', INTEGER '999', BIGINT '0', BIGINT '0', BIGINT '70')");

            // Check DV via $files again: cardinality 600, PUFFIN delete entry for each data file, 2 total puffin files (not all partitions are modified)
            assertThat(query("SELECT sum(record_count), count(*), count_if(file_format = 'PUFFIN'), count(distinct file_path) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '600', BIGINT '70', BIGINT '70', BIGINT '2')");

            // delete odds => 400 rows removed
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 2 = 1", 400);

            // verify delete
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0')");

            // Check DV via $files again: cardinality 1000, PUFFIN delete entry for each data file, only 1 puffin file since all rows are now deleted
            assertThat(query("SELECT sum(record_count), count(*), count_if(file_format = 'PUFFIN'), count(distinct file_path) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '1000', BIGINT '70', BIGINT '70', BIGINT '1')");

            // re-insert 100 rows
            assertUpdate("INSERT INTO " + table.getName() + " SELECT x FROM UNNEST(sequence(1, 100)) t(x)", 100);
            assertThat(query("SELECT count(*), min(id), max(id) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '100', INTEGER '1', INTEGER '100')");
            assertThat(query("SELECT sum(record_count), count(*), count_if(file_format = 'PUFFIN'), count(distinct file_path) FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (BIGINT '1000', BIGINT '70', BIGINT '70', BIGINT '1')");
        }
    }

    @Test
    void testV2ToV3MigrationWithDeletes()
    {
        try (TestTable table = newTrinoTable("trino_v2_to_v3_dv_migration", "(id INTEGER, grp INTEGER) WITH (format = 'PARQUET', format_version = 2)")) {
            // 1000 rows: id=1..1000, grp cycles 0..6 (just to have a second column)
            for (int i = 0; i < 10; i++) {
                assertUpdate("INSERT INTO " + table.getName() + " SELECT x, x %% 7 FROM UNNEST(sequence(%s, %s)) t(x)".formatted(i * 100 + 1, (i + 1) * 100), 100);
            }

            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 3 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1000', INTEGER '1', INTEGER '1000', BIGINT '333', BIGINT '200', BIGINT '10')");

            // v2 delete files (legacy position deletes)
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 3 = 0", 333);

            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 3 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '667', INTEGER '1', INTEGER '1000', BIGINT '0', BIGINT '134', BIGINT '10')");

            // Ensure we produced legacy delete files and no Puffin yet
            assertThat(query("SELECT count_if(file_format <> 'PUFFIN') > 0, count_if(file_format = 'PUFFIN') > 0 FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (true, false)");

            // Upgrade table to v3
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");

            // Verify data still correct after upgrade
            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 3 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '667', INTEGER '1', INTEGER '1000', BIGINT '0', BIGINT '134', BIGINT '10')");

            // Additional deletes in v3 should be recorded as deletion vectors and still respect the v2 deletes
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 5 = 0", 134);

            assertThat(query("SELECT count(*), min(id), max(id), count_if(id % 3 = 0), count_if(id % 5 = 0), count(distinct \"$path\") FROM " + table.getName()))
                    .matches("VALUES (BIGINT '533', INTEGER '1', INTEGER '998', BIGINT '0', BIGINT '0', BIGINT '10')");

            // Ensure v3 deletion vectors (Puffin) are present after the upgrade
            assertThat(query("SELECT count_if(file_format <> 'PUFFIN') > 0, count_if(file_format = 'PUFFIN') > 0 FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (true, true)");

            // delete remaining rows
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 3 > 0", 533);
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0')");

            // We still have both legacy delete files because they are shared across multiple files (not single-file position deletes)
            assertThat(query("SELECT count_if(file_format <> 'PUFFIN') > 0, count_if(file_format = 'PUFFIN') > 0 FROM \"" + table.getName() + "$files\" WHERE content = 1"))
                    .matches("VALUES (true, true)");
        }
    }

    @Test
    void testV3DeletionVectorWithExistingEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_v3_dv_with_equality", "(id INTEGER, grp INTEGER) WITH (format = 'PARQUET', format_version = 2)")) {
            // Insert data: 100 rows with id=1..100, grp=id%5
            assertUpdate("INSERT INTO " + table.getName() + " SELECT x, x % 5 FROM UNNEST(sequence(1, 100)) t(x)", 100);

            assertThat(query("SELECT count(*), count_if(grp = 0), count_if(grp = 1) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '100', BIGINT '20', BIGINT '20')");

            // Add equality delete for grp = 1
            Table icebergTable = loadTable(table.getName());
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("grp", 1), Optional.empty());

            // Verify equality delete applied
            assertThat(query("SELECT count(*), count_if(grp = 0), count_if(grp = 1) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '80', BIGINT '20', BIGINT '0')");

            // Upgrade to v3
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES format_version = 3");

            // Verify data still correct after upgrade (equality deletes still work)
            assertThat(query("SELECT count(*), count_if(grp = 0), count_if(grp = 1) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '80', BIGINT '20', BIGINT '0')");

            // Delete some rows using deletion vectors (id % 10 = 0 -> 10 rows, none overlap with grp=1)
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id % 10 = 0", 10);

            // Verify both equality deletes and deletion vectors work together
            // 80 - 10 = 70 rows remain; grp=0 had 20 rows, 10 deleted (10,20,30,40,50,60,70,80,90,100 all have grp=0)
            assertThat(query("SELECT count(*), count_if(grp = 0), count_if(grp = 1), count_if(id % 10 = 0) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '70', BIGINT '10', BIGINT '0', BIGINT '0')");
        }
    }

    private void injectEncryptionKeysIntoMetadataJson(Path tableLocation, String keyId)
            throws IOException
    {
        Path metadataFile = Path.of(getLatestMetadataLocation(fileSystemFactory.create(SESSION), tableLocation.toString()));

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
}
