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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

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
}
