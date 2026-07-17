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
package io.trino.plugin.iceberg.encryption;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.createEncryptionManager;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.createTestRecords;
import static io.trino.plugin.iceberg.util.EncryptedFileTestUtils.writeEncryptedDataFile;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseIcebergEncryptionFlociTest
        extends AbstractTestQueryFramework
{
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "age", Types.IntegerType.get()));

    protected final String bucketName = "test-iceberg-encryption-floci-" + randomNameSuffix();

    protected Hive3FlociDataLake hiveFlociDataLake;
    private KeyManagementClient kmsClient;
    private String kmsKeyId;
    private DefaultEncryptionManagerFactory encryptionManagerFactory;
    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake(bucketName));
        hiveFlociDataLake.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", hiveFlociDataLake.getHiveMetastoreEndpoint().toString())
                                .put("hive.metastore.thrift.client.read-timeout", "1m")
                                .put("fs.s3.enabled", "true")
                                .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                                .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                                .put("s3.endpoint", hiveFlociDataLake.floci().endpoint().toString())
                                .put("s3.region", FLOCI_REGION)
                                .put("s3.path-style-access", "true")
                                .put("s3.streaming.part-size", "5MB")
                                .buildOrThrow())
                .setAdditionalOverrideModule(binder -> {
                    kmsKeyId = kmsKey();
                    kmsClient = kmsClient();
                    encryptionManagerFactory = new DefaultEncryptionManagerFactory(Optional.of(kmsClient));
                    newOptionalBinder(binder, EncryptionManagerFactory.class)
                            .setBinding()
                            .toInstance(encryptionManagerFactory);
                })
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName("tpch")
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/tpch'"))
                                .build())
                .build();
    }

    @BeforeAll
    void setUp()
    {
        // Use the same encryption manager factory (backed by the Floci KMS client) as the query engine.
        // Otherwise the catalog would wrap keys with a different KMS client than the one used to unwrap them during reads.
        catalog = getTrinoCatalog(getHiveMetastore(getQueryRunner()), getFileSystemFactory(getQueryRunner()), "iceberg", encryptionManagerFactory);
    }

    protected abstract String kmsKey();

    protected abstract KeyManagementClient kmsClient();

    @Test
    void testReadEncryptedParquetTable()
            throws Exception
    {
        String tableName = "enc_parquet_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);

        createIcebergTable(schemaTableName, TABLE_SCHEMA);
        try {
            Table table = catalog.loadTable(SESSION, schemaTableName);
            setTableEncryptionKey(table, schemaTableName);
            EncryptionManager encryptionManager = createEncryptionManager(kmsKeyId, kmsClient);

            DataFile dataFile = writeEncryptedDataFile(table, encryptionManager, createTestRecords(TABLE_SCHEMA, 100));
            table.newFastAppend().appendFile(dataFile).commit();
            invalidateTableCache(schemaTableName);

            assertThat(computeActual("SELECT bool_and(key_metadata IS NOT NULL) FROM \"" + tableName + "$files\"").getOnlyValue())
                    .isEqualTo(true);
            assertThat(computeActual("SELECT count(*), min(id), max(id), min(age), max(age) FROM " + tableName)
                    .getMaterializedRows().getFirst().getFields())
                    .containsExactly(100L, 0, 99, 0, 99);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS tpch." + tableName);
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
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of(DEFAULT_FILE_FORMAT, "PARQUET"))
                .commitTransaction();
    }

    private void setTableEncryptionKey(Table table, SchemaTableName schemaTableName)
    {
        table.updateProperties()
                .set(ENCRYPTION_TABLE_KEY, kmsKeyId)
                .commit();
        invalidateTableCache(schemaTableName);
    }

    private void invalidateTableCache(SchemaTableName schemaTableName)
    {
        ((TrinoHiveCatalog) catalog).invalidateTableCache(schemaTableName);
    }
}
