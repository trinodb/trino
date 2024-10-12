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
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergHiveMetastoreAutoCleanMetadataFile
        extends AbstractTestQueryFramework
{
    private static final String bucketName = "test-auto-clean-metadata" + randomNameSuffix();

    private TrinoCatalog trinoCatalog;
    private IcebergTableOperationsProvider tableOperationsProvider;
    private TrinoFileSystem fileSystem;

    // Use MinIO for storage, since HDFS is hard to get working in a unit test
    private HiveMinioDataLake dataLake;
    private TrinoFileSystemFactory fileSystemFactory;
    public static final int METADATA_PREVIOUS_VERSIONS_MAX = 5;

    @AfterAll
    public void tearDown()
            throws Exception
    {
        if (dataLake != null) {
            dataLake.stop();
            dataLake = null;
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.dataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        this.dataLake.start();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", FileFormat.PARQUET.name())
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put(
                                        "hive.metastore.uri",
                                        dataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                                .put(
                                        "hive.metastore.thrift.client.read-timeout",
                                        "1m") // read timed out sometimes happens with the default timeout
                                .put("fs.hadoop.enabled", "false")
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", dataLake.getMinio().getMinioAddress())
                                .put("s3.path-style-access", "true")
                                .put("s3.streaming.part-size", "5MB") // minimize memory usage
                                .put("s3.max-connections", "2") // verify no leaks
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName("tpch")
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/tpch'"))
                                .build())
                .build();
        HiveMetastore metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        this.fileSystemFactory = getFileSystemFactory(queryRunner);
        this.tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);

        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        this.trinoCatalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable());

        return queryRunner;
    }

    @Test
    public void testInsertWithAutoCleanMetadataFile()
    {
        assertUpdate("CREATE TABLE table_to_metadata_count (_bigint BIGINT, _varchar VARCHAR)");

        Table table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("tpch", "table_to_metadata_count"));
        table.updateProperties()
                .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
                .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(METADATA_PREVIOUS_VERSIONS_MAX))
                .commit();
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO table_to_metadata_count VALUES (1, 'a')", 1);
        }
        try {
            int count = 0;
            fileSystem = fileSystemFactory.create(SESSION);
            FileIterator fileIterator = fileSystem.listFiles(Location.of(table.location()));
            while (fileIterator.hasNext()) {
                FileEntry next = fileIterator.next();
                if (next.location().path().endsWith("metadata.json")) {
                    count++;
                }
            }
            assertThat(count).isEqualTo(1 + METADATA_PREVIOUS_VERSIONS_MAX);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
