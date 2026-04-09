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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.encryption.DefaultEncryptionManagerFactory;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.createEncryptionManager;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.createTestRecords;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.createTestRecordsWithName;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writeEncryptedAvroDataFile;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writeEncryptedDataFile;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writeEncryptedEqualityDeleteFile;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writeEncryptedPositionDeleteFile;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writePlaintextDataFile;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writePlaintextFileWithEncryptionKeyMetadata;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergParquetEncryption
        extends AbstractTestQueryFramework
{
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "age", Types.IntegerType.get()));

    private static final String TEST_KEY_ID = "test-key-id";
    private static final TestingFileMetastoreKeyManagementClient KMS_CLIENT = new TestingFileMetastoreKeyManagementClient();

    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setAdditionalOverrideModule(binder -> newOptionalBinder(binder, EncryptionManagerFactory.class)
                        .setBinding()
                        .toInstance(new DefaultEncryptionManagerFactory(Optional.of(KMS_CLIENT))))
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        catalog = getTrinoCatalog(getHiveMetastore(getQueryRunner()), getFileSystemFactory(getQueryRunner()), "iceberg");
        getQueryRunner().execute("CREATE SCHEMA IF NOT EXISTS tpch");
    }

    @Test
    public void testReadEncryptedParquetTable()
            throws Exception
    {
        String tableName = "enc_parquet_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            DataFile dataFile = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 100));
            table.newFastAppend().appendFile(dataFile).commit();
            invalidateTableCache(schemaTableName);

            assertThat(computeActual("SELECT bool_and(key_metadata IS NOT NULL) FROM \"" + tableName + "$files\"").getOnlyValue())
                    .isEqualTo(true);
            assertThat(computeActual("SELECT count(*), min(id), max(id), min(age), max(age) FROM " + tableName)
                    .getMaterializedRows().get(0).getFields())
                    .containsExactly(100L, 0, 99, 0, 99);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testReadEncryptedParquetTableWithPositionDeletes()
            throws Exception
    {
        String tableName = "enc_parquet_pos_delete_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            DataFile dataFile = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 100));
            table.newFastAppend().appendFile(dataFile).commit();

            // Delete rows at positions 0 and 1 (id=0, id=1)
            DeleteFile posDeleteFile = writeEncryptedPositionDeleteFile(
                    table, encryptionManager, dataFile.path().toString(), 0, 1);
            table.newRowDelta().addDeletes(posDeleteFile).commit();
            invalidateTableCache(schemaTableName);

            assertThat(computeActual("SELECT count(*) FROM " + tableName).getOnlyValue())
                    .isEqualTo(98L);
            assertThat(computeActual("SELECT min(id) FROM " + tableName).getOnlyValue())
                    .isEqualTo(2);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testReadEncryptedParquetTableWithEqualityDeletes()
            throws Exception
    {
        String tableName = "enc_parquet_eq_delete_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            DataFile dataFile = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 100));
            table.newFastAppend().appendFile(dataFile).commit();

            // Equality delete all rows where age=99 (that's id=0)
            Schema deleteRowSchema = TABLE_SCHEMA.select("age");
            List<Integer> equalityFieldIds = ImmutableList.of(TABLE_SCHEMA.findField("age").fieldId());
            Record deleteRecord = GenericRecord.create(deleteRowSchema);
            deleteRecord.setField("age", 99);

            DeleteFile eqDeleteFile = writeEncryptedEqualityDeleteFile(
                    table, encryptionManager, deleteRowSchema, equalityFieldIds, ImmutableList.of(deleteRecord));
            table.newRowDelta().addDeletes(eqDeleteFile).commit();
            invalidateTableCache(schemaTableName);

            assertThat(computeActual("SELECT count(*) FROM " + tableName).getOnlyValue())
                    .isEqualTo(99L);
            assertThat(computeActual("SELECT count(*) FROM " + tableName + " WHERE age = 99").getOnlyValue())
                    .isEqualTo(0L);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPlaintextFileWithKeyMetadataFails()
            throws Exception
    {
        Schema schemaWithString = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()));

        String tableName = "enc_plaintext_fail_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, schemaWithString);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            DataFile fakeEncryptedFile = writePlaintextFileWithEncryptionKeyMetadata(
                    table, encryptionManager, createTestRecordsWithName(schemaWithString, 100));
            table.newFastAppend().appendFile(fakeEncryptedFile).commit();
            invalidateTableCache(schemaTableName);

            assertThatThrownBy(() -> computeActual("SELECT count(*) FROM " + tableName + " WHERE name = 'name_1'"))
                    .hasMessageContaining("Applying decryptor on plaintext file");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPlaintextFileWithKeyMetadataAllowedBySessionProperty()
            throws Exception
    {
        Schema schemaWithString = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()));

        String tableName = "enc_plaintext_allowed_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, schemaWithString);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            DataFile fakeEncryptedFile = writePlaintextFileWithEncryptionKeyMetadata(
                    table, encryptionManager, createTestRecordsWithName(schemaWithString, 100));
            table.newFastAppend().appendFile(fakeEncryptedFile).commit();
            invalidateTableCache(schemaTableName);

            Session allowPlaintext = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "plaintext_files_allowed_for_encrypted_tables", "true")
                    .build();
            assertThat(computeActual(allowPlaintext, "SELECT count(*) FROM " + tableName + " WHERE name = 'name_1'").getOnlyValue())
                    .isEqualTo(1L);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testReadMixedEncryptedAndPlaintextFiles()
            throws Exception
    {
        String tableName = "enc_mixed_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);

            // Write a plaintext file before encryption is enabled
            DataFile plaintextFile = writePlaintextDataFile(table, createTestRecords(TABLE_SCHEMA, 50));
            table.newFastAppend().appendFile(plaintextFile).commit();

            // Enable encryption and write an encrypted file
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);
            DataFile encryptedFile = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 50));
            table.newFastAppend().appendFile(encryptedFile).commit();
            invalidateTableCache(schemaTableName);

            // Both files should be readable — plaintext file has no key_metadata so no decryption is attempted
            assertThat(computeActual("SELECT count(*) FROM " + tableName).getOnlyValue())
                    .isEqualTo(100L);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testReadEncryptedAvroTableFails()
            throws Exception
    {
        String tableName = "enc_avro_fail_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        TABLE_SCHEMA,
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.of(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of(DEFAULT_FILE_FORMAT, "AVRO"))
                .commitTransaction();
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            DataFile dataFile = writeEncryptedAvroDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 10));
            table.newFastAppend().appendFile(dataFile).commit();
            invalidateTableCache(schemaTableName);

            assertThatThrownBy(() -> computeActual("SELECT * FROM " + tableName))
                    .hasMessageContaining("Reading encrypted non-Parquet file is not supported");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testWriteToEncryptedTableFails()
            throws Exception
    {
        String tableName = "enc_write_fail_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            // Write data via Iceberg API so DELETE has rows to process
            DataFile dataFile = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 10));
            table.newFastAppend().appendFile(dataFile).commit();
            invalidateTableCache(schemaTableName);

            assertThatThrownBy(() -> getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Writing to encrypted Iceberg tables is not supported");
            assertThatThrownBy(() -> getQueryRunner().execute("DELETE FROM " + tableName + " WHERE id = 1"))
                    .hasMessageContaining("Writing to encrypted Iceberg tables is not supported");
            assertThatThrownBy(() -> getQueryRunner().execute("MERGE INTO " + tableName + " USING (VALUES 1) t(id) ON " + tableName + ".id = t.id WHEN MATCHED THEN DELETE"))
                    .hasMessageContaining("Writing to encrypted Iceberg tables is not supported");
            assertThatThrownBy(() -> getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE optimize"))
                    .hasMessageContaining("Writing to encrypted Iceberg tables is not supported");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testReadEncryptedTableChanges()
            throws Exception
    {
        String tableName = "enc_table_changes_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(TEST_KEY_ID, KMS_CLIENT);

            // Write first snapshot with encrypted data
            DataFile dataFile1 = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 50));
            table.newFastAppend().appendFile(dataFile1).commit();
            invalidateTableCache(schemaTableName);
            long startSnapshotId = table.currentSnapshot().snapshotId();

            // Write second snapshot with more encrypted data
            DataFile dataFile2 = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 30));
            table.newFastAppend().appendFile(dataFile2).commit();
            invalidateTableCache(schemaTableName);
            long endSnapshotId = table.currentSnapshot().snapshotId();

            // Read table changes between snapshots
            assertThat(computeActual(
                    "SELECT count(*) FROM TABLE(system.table_changes('tpch', '%s', %d, %d))".formatted(tableName, startSnapshotId, endSnapshotId))
                    .getOnlyValue())
                    .isEqualTo(30L);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void createIcebergTable(SchemaTableName schemaTableName, Schema schema)
    {
        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        schema,
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.of(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of(DEFAULT_FILE_FORMAT, "PARQUET"))
                .commitTransaction();
    }

    private void setTableEncryptionKey(Table table, SchemaTableName schemaTableName)
    {
        table.updateProperties()
                .set(ENCRYPTION_TABLE_KEY, TEST_KEY_ID)
                .commit();
        invalidateTableCache(schemaTableName);
    }

    private void invalidateTableCache(SchemaTableName schemaTableName)
    {
        ((TrinoHiveCatalog) catalog).invalidateTableCache(schemaTableName);
    }
}
