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

import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionManagerFactory;
import io.trino.plugin.iceberg.encryption.TestingKmsClient;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.StreamSupport;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTableEncryption
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.encryption.kms-impl", TestingKmsClient.class.getName())
                .build();
    }

    @Test
    public void testEncryptedTableOperations()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_encrypted_table_",
                "(id INT, data VARCHAR) WITH (" +
                        "format_version = 3, " +
                        "encryption_key_id = 'test-key', " +
                        "encryption_data_key_length = 16)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate("UPDATE " + tableName + " SET data = 'bb' WHERE id = 2", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertQuery("SELECT id, data FROM " + tableName + " ORDER BY id", "VALUES (2, 'bb'), (3, 'c')");

            assertEncryptedFilesHaveKeyMetadata(tableName);
        }
    }

    @Test
    public void testEncryptedOrcWriterValidation()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, "orc_writer_validate_percentage", "100")
                .build();

        try (TestTable table = new TestTable(
                sql -> getQueryRunner().execute(session, sql),
                "test_encrypted_table_orc_validate_",
                "(id INT, data VARCHAR) WITH (" +
                        "format = 'ORC', " +
                        "format_version = 3, " +
                        "encryption_key_id = 'test-key', " +
                        "encryption_data_key_length = 16)")) {
            String tableName = table.getName();
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            assertQuery(session, "SELECT id, data FROM " + tableName + " ORDER BY id", "VALUES (1, 'a'), (2, 'b')");
        }
    }

    @Test
    public void testEncryptedTableChangesFunction()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_encrypted_table_changes_",
                "(id INT, data VARCHAR) WITH (" +
                        "format_version = 3, " +
                        "encryption_key_id = 'test-key', " +
                        "encryption_data_key_length = 16)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            long snapshotAfterFirstInsert = ((Number) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1")).longValue();

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            long snapshotAfterSecondInsert = ((Number) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1")).longValue();

            assertQuery(
                    "SELECT id, data, _change_type, _change_version_id FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', " + snapshotAfterFirstInsert + ", " + snapshotAfterSecondInsert + "))",
                    "VALUES (2, 'b', 'insert', " + snapshotAfterSecondInsert + ")");
        }
    }

    private void assertEncryptedFilesHaveKeyMetadata(String tableName)
    {
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        IcebergConfig icebergConfig = new IcebergConfig().setEncryptionKmsImpl(TestingKmsClient.class.getName());
        IcebergEncryptionManagerFactory encryptionManagerFactory = new IcebergEncryptionManagerFactory(icebergConfig);
        BaseTable icebergTable = loadTable(
                tableName,
                metastore,
                fileSystemFactory,
                ICEBERG_CATALOG,
                getSession().getSchema().orElseThrow(),
                encryptionManagerFactory);

        List<FileScanTask> scanTasks = StreamSupport.stream(icebergTable.newScan().planFiles().spliterator(), false)
                .toList();
        assertThat(scanTasks).isNotEmpty();
        scanTasks.forEach(task -> assertThat(task.file().keyMetadata()).isNotNull());

        List<DeleteFile> deleteFiles = scanTasks.stream()
                .flatMap(task -> task.deletes().stream())
                .toList();
        deleteFiles.forEach(deleteFile -> assertThat(deleteFile.keyMetadata()).isNotNull());
    }
}
