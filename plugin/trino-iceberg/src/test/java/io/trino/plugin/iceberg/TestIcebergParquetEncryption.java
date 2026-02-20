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
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetEncryption
        extends AbstractTestQueryFramework
{
    private TrinoCatalog catalog;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-metastore.encryption.kms-impl", TestingFileMetastoreKeyManagementClient.class.getName())
                .build();
        metastore = getHiveMetastore(queryRunner);
        catalog = getTrinoCatalog(metastore, getFileSystemFactory(queryRunner), "iceberg");
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS tpch");
        return queryRunner;
    }

    @Test
    public void testReadIcebergLibraryEncryptedParquetTable()
            throws Exception
    {
        String tableName = "enc_parquet_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);
        Schema tableSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "age", Types.IntegerType.get()));

        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        tableSchema,
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "PARQUET"))
                .commitTransaction();

        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            table.newFastAppend()
                    .appendFile(writeEncryptedParquetDataFile(table))
                    .commit();
            setTableEncryptionKey(schemaTableName, "test-key-id");

            assertThat(computeActual("SELECT bool_and(key_metadata IS NOT NULL) FROM \"" + tableName + "$files\"").getOnlyValue())
                    .isEqualTo(true);
            MaterializedResult summary = computeActual("SELECT count(*), min(id), max(id), min(age), max(age) FROM " + tableName);
            assertThat(summary.getMaterializedRows()).hasSize(1);
            assertThat(summary.getMaterializedRows().get(0).getFields()).containsExactly(100L, 0, 99, 0, 99);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void setTableEncryptionKey(SchemaTableName schemaTableName, String keyId)
    {
        io.trino.metastore.Table metastoreTable = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName()).orElseThrow();
        String metadataLocation = metastoreTable.getParameters().get(METADATA_LOCATION_PROP);
        metastore.replaceTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                io.trino.metastore.Table.builder(metastoreTable)
                        .setParameter(ENCRYPTION_TABLE_KEY, keyId)
                        .setParameter(PREVIOUS_METADATA_LOCATION_PROP, metadataLocation)
                        .build(),
                PrincipalPrivileges.NO_PRIVILEGES,
                ImmutableMap.of(
                        "expected_parameter_key", METADATA_LOCATION_PROP,
                        "expected_parameter_value", metadataLocation));
        ((TrinoHiveCatalog) catalog).getMetastore().invalidateTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    private static DataFile writeEncryptedParquetDataFile(Table table)
            throws Exception
    {
        EncryptionManager encryptionManager = EncryptionTestHelpers.createEncryptionManager();
        String dataPath = Path.of(table.location())
                .resolve("data")
                .resolve("enc-" + randomNameSuffix() + ".parquet")
                .toString();

        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(table.io().newOutputFile(dataPath));
        try (DataWriter<Record> writer = Parquet.writeData(encryptedOutputFile)
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .withKeyMetadata(encryptedOutputFile.keyMetadata())
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            Record record = GenericRecord.create(table.schema());
            for (int i = 0; i < 100; i++) {
                record.setField("id", i);
                record.setField("age", 99 - i);
                writer.write(record);
            }
            writer.close();
            return writer.toDataFile();
        }
    }
}
