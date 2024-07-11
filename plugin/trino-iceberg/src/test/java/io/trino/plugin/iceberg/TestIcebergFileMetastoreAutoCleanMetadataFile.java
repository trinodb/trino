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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
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
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergFileMetastoreAutoCleanMetadataFile
        extends AbstractTestQueryFramework
{
    private TrinoCatalog catalog;
    private IcebergTableOperationsProvider tableOperationsProvider;
    private TrinoFileSystemFactory fileSystemFactory;
    private File metastoreDir;

    public static final int METADATA_PREVIOUS_VERSIONS_MAX = 5;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDirectory("test_iceberg_clean_metadata").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setMetastoreDirectory(metastoreDir)
                .build();

        HiveMetastore metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        this.fileSystemFactory = getFileSystemFactory(queryRunner);
        this.tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);


        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        this.catalog = new TrinoHiveCatalog(
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

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testInsertWithAutoCleanMetadataFile()
    {
        assertUpdate("CREATE TABLE table_to_file_metadata_count (_bigint BIGINT, _varchar VARCHAR)");
        Table table = IcebergUtil.loadIcebergTable(catalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("tpch", "table_to_file_metadata_count"));
        table.updateProperties()
                .set("write.metadata.delete-after-commit.enabled", "true")
                .set("write.metadata.previous-versions-max", String.valueOf(METADATA_PREVIOUS_VERSIONS_MAX))
                .commit();

        int count = 0;
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO table_to_file_metadata_count VALUES (1, 'a')", 1);
        }
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
            FileIterator fileIterator = fileSystem.listFiles(Location.of(table.location()));
            while (fileIterator.hasNext()) {
                FileEntry next = fileIterator.next();
                if (next.location().path().endsWith("metadata.json")) {
                    count++;
                }
            }
            assertThat(count).isEqualTo(1 + METADATA_PREVIOUS_VERSIONS_MAX);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
