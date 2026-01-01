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
import java.util.Comparator;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void testDeleteV3Table()
    {
        try (TestTable table = newTrinoTable("test_delete_v3", "(id integer) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);

            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (INTEGER '1'), (INTEGER '3')");
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
    void testV3RejectsColumnDefaults()
            throws IOException
    {
        String temp = "tmp_v3_defaults_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + temp);
        long dataFileSize = getFileSize(dataFilePath);

        String hadoopTableName = "hadoop_v3_defaults_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        Schema schemaWithInitialDefault = new Schema(
                Types.NestedField.optional("id")
                        .withId(1)
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

        assertQueryFails(
                "SELECT * FROM " + registered,
                ".*Iceberg v3 column default values are not supported.*");

        // Also verify INSERT fails with column defaults
        assertQueryFails(
                "INSERT INTO " + registered + " VALUES 2",
                ".*Iceberg v3 column default values are not supported.*");

        // Use unregister_table instead of DROP TABLE because DROP TABLE triggers the same validation error
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '%s')".formatted(registered));
        assertUpdate("DROP TABLE " + temp);
        deleteRecursively(hadoopTableLocation, ALLOW_INSECURE);
    }

    @Test
    void testV3RejectsColumnWriteDefaults()
            throws IOException
    {
        String temp = "tmp_v3_write_defaults_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + temp + " (id INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + temp + " VALUES 1", 1);

        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + temp);
        long dataFileSize = getFileSize(dataFilePath);

        String hadoopTableName = "hadoop_v3_write_defaults_" + randomNameSuffix();
        Path hadoopTableLocation = dataDirectory.resolve(hadoopTableName);

        Schema schemaWithWriteDefault = new Schema(
                Types.NestedField.optional("id")
                        .withId(1)
                        .ofType(Types.IntegerType.get())
                        .withWriteDefault(Expressions.lit(42))
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

        assertQueryFails(
                "SELECT * FROM " + registered,
                ".*Iceberg v3 column default values are not supported.*");

        // Also verify INSERT fails with column defaults
        assertQueryFails(
                "INSERT INTO " + registered + " VALUES 2",
                ".*Iceberg v3 column default values are not supported.*");

        // Use unregister_table instead of DROP TABLE because DROP TABLE triggers the same validation error
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '%s')".formatted(registered));
        assertUpdate("DROP TABLE " + temp);
        deleteRecursively(hadoopTableLocation, ALLOW_INSECURE);
    }

    @Test
    void testV3InsertProducesRowLineageMetadata()
    {
        String tableName = "test_v3_insert_lineage_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, v VARCHAR) WITH (format = 'PARQUET', format_version = 3)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

        // Validate metadata-level invariants (existing coverage)
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

            assertThat(file.firstRowId()).as("data file firstRowId must be set in v3").isNotNull();
            assertThat(file.dataSequenceNumber()).as("data file dataSequenceNumber must be set").isNotNull();
        }

        assertThat(fileCount).isGreaterThan(0);
        assertThat(totalRecords).isEqualTo(3);

        assertThat(query("SELECT \"$row_id\" FROM " + tableName))
                .matches("VALUES (BIGINT '0'), (BIGINT '1'), (BIGINT '2')");

        assertQuery("SELECT count(DISTINCT \"$last_updated_sequence_number\") FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testRowLineageUpgradeBehavior()
    {
        String tableName = "test_v2_to_v3_row_lineage_" + randomNameSuffix();

        // Create a v2 table and write an initial snapshot. v2 snapshots do not have row IDs assigned.
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, v VARCHAR) WITH (format = 'PARQUET', format_version = 2)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

        BaseTable table = loadTable(tableName);
        table.refresh();
        long v2SnapshotId = table.currentSnapshot().snapshotId();

        // v2 snapshot:
        // - $row_id is NULL for all rows because row IDs are only assigned starting in v3 snapshots with first-row-id assignment.
        // - $last_updated_sequence_number is still non-null because Trino synthesizes it from the file data sequence number.
        // - distinct_lusn_values_in_snapshot is 1 because all rows come from the same commit.
        assertThat(query(
                "SELECT " +
                        "  count(*) AS row_count, " +
                        "  count_if(\"$row_id\" IS NULL) AS row_id_nulls, " +
                        "  count_if(\"$last_updated_sequence_number\" IS NULL) AS lusn_nulls, " +
                        "  count(DISTINCT \"$row_id\") AS distinct_row_id, " +
                        "  count(DISTINCT \"$last_updated_sequence_number\") AS distinct_lusn_values_in_snapshot " +
                        "FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .matches("VALUES (BIGINT '3', BIGINT '3', BIGINT '0', BIGINT '0', BIGINT '1')");

        // Upgrade the table metadata to v3. This does not rewrite old snapshots, so existing rows still have NULL $row_id.
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 3");

        table.refresh();
        long postUpgradeSnapshotId = table.currentSnapshot().snapshotId();

        // Immediately after upgrade (no new snapshot yet):
        // - still reading the pre-upgrade data files, so $row_id remains NULL for all rows
        // - Trino still synthesizes $last_updated_sequence_number, so it remains non-null
        assertThat(query(
                "SELECT " +
                        "  count(*) AS row_count, " +
                        "  count_if(\"$row_id\" IS NULL) AS row_id_nulls, " +
                        "  count_if(\"$last_updated_sequence_number\" IS NULL) AS lusn_nulls, " +
                        "  count(DISTINCT \"$row_id\") AS distinct_row_id, " +
                        "  count(DISTINCT \"$last_updated_sequence_number\") AS distinct_lusn_values_in_snapshot " +
                        "FROM " + tableName + " FOR VERSION AS OF " + postUpgradeSnapshotId))
                .matches("VALUES (BIGINT '3', BIGINT '3', BIGINT '0', BIGINT '0', BIGINT '1')");

        // Create the first post-upgrade snapshot. This is when Iceberg assigns first-row-id ranges to manifests/files,
        // and $row_id becomes materializable for all rows visible in the new snapshot.
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'd')", 1);

        table.refresh();
        long firstAssignedSnapshotId = table.currentSnapshot().snapshotId();

        // After the first v3 snapshot:
        // - all rows should have non-null $row_id and non-null $last_updated_sequence_number
        // - $row_id should be unique across the 4 rows
        // - distinct_lusn_values_in_snapshot is 2 because the snapshot contains rows written in two different commits
        //   (the original v2 insert commit and this post-upgrade insert commit)
        assertThat(query(
                "SELECT " +
                        "  count(*) AS row_count, " +
                        "  count_if(\"$row_id\" IS NULL) AS row_id_nulls, " +
                        "  count_if(\"$last_updated_sequence_number\" IS NULL) AS lusn_nulls, " +
                        "  count(DISTINCT \"$row_id\") AS distinct_row_id, " +
                        "  count(DISTINCT \"$last_updated_sequence_number\") AS distinct_lusn_values_in_snapshot " +
                        "FROM " + tableName + " FOR VERSION AS OF " + firstAssignedSnapshotId))
                .matches("VALUES (BIGINT '4', BIGINT '0', BIGINT '0', BIGINT '4', BIGINT '2')");

        // Baseline snapshot for "before update" comparisons.
        long beforeUpdateSnapshotId = firstAssignedSnapshotId;

        // Update one row, producing a new snapshot.
        assertUpdate("UPDATE " + tableName + " SET v = 'bb' WHERE id = 2", 1);

        // Value updated.
        assertThat(query("SELECT v FROM " + tableName + " WHERE id = 2"))
                .matches("VALUES (VARCHAR 'bb')");

        // Current expected behavior: UPDATE does not preserve $row_id (it behaves like delete+insert with a new row id).
        assertThat(query(
                "SELECT " +
                        "  (SELECT \"$row_id\" FROM " + tableName + " WHERE id = 2) = " +
                        "  (SELECT \"$row_id\" FROM " + tableName + " FOR VERSION AS OF " + beforeUpdateSnapshotId + " WHERE id = 2)"))
                .matches("VALUES (BOOLEAN 'false')");

        // The updated row should have a higher last-updated sequence number than it did in the baseline snapshot.
        assertThat(query(
                "SELECT " +
                        "  (SELECT \"$last_updated_sequence_number\" FROM " + tableName + " WHERE id = 2) > " +
                        "  (SELECT \"$last_updated_sequence_number\" FROM " + tableName + " FOR VERSION AS OF " + beforeUpdateSnapshotId + " WHERE id = 2)"))
                .matches("VALUES (BOOLEAN 'true')");

        // After the update:
        // - still no NULLs for $row_id or $last_updated_sequence_number
        // - $row_id remains unique
        // - distinct_lusn_values_in_snapshot is 3 because the table now contains rows written across three commits:
        //   initial v2 insert, first post-upgrade insert, and this update commit.
        assertThat(query(
                "SELECT " +
                        "  count(*) AS row_count, " +
                        "  count_if(\"$row_id\" IS NULL) AS row_id_nulls, " +
                        "  count_if(\"$last_updated_sequence_number\" IS NULL) AS lusn_nulls, " +
                        "  count(DISTINCT \"$row_id\") AS distinct_row_id, " +
                        "  count(DISTINCT \"$last_updated_sequence_number\") AS distinct_lusn_values_in_snapshot " +
                        "FROM " + tableName))
                .matches("VALUES (BIGINT '4', BIGINT '0', BIGINT '0', BIGINT '4', BIGINT '3')");

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
    void testIcebergWritesAndTrinoReadsDeletionVector()
            throws Exception
    {
        Path hadoopTableLocation = dataDirectory.resolve("deletion_vector" + randomNameSuffix());

        Table icebergTable = new HadoopTables(new Configuration(false)).create(
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.format.default", "PARQUET"),
                hadoopTableLocation.toString());

        // Write a data file with 5 rows: ids 0 to 4
        String dataPath = hadoopTableLocation.resolve("data")
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
        try (BaseDVFileWriter dvWriter = new BaseDVFileWriter(
                OutputFileFactory.builderFor(icebergTable, 1, 1).format(FileFormat.PUFFIN).build(),
                _ -> PositionDeleteIndex.empty())) {
            dvWriter.delete(dataPath, 0L, icebergTable.spec(), null);
            dvWriter.delete(dataPath, 3L, icebergTable.spec(), null);
            dvWriter.close();

            icebergTable.newRowDelta()
                    .addDeletes(getOnlyElement(dvWriter.result().deleteFiles()))
                    .commit();
        }

        String registered = "registered_v3_dv_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')"
                .formatted(registered, hadoopTableLocation));

        assertThat(query("SELECT * FROM " + registered))
                .matches("VALUES (1), (2), (4)");

        assertUpdate("DROP TABLE " + registered);
    }

    @Test
    void testTrinoWritesAndReadsDeletionVector()
    {
        try (TestTable table = newTrinoTable("trino_v3_dv_delete", "(id INTEGER) WITH (format = 'PARQUET', format_version = 3)")) {
            // 1000 rows: 1..1000
            for (int i = 0; i < 10; i++) {
                assertUpdate(
                        "INSERT INTO " + table.getName() + " SELECT x FROM UNNEST(sequence(%s, %s)) t(x)".formatted(i * 100 + 1, (i + 1) * 100),
                        100);
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
}
