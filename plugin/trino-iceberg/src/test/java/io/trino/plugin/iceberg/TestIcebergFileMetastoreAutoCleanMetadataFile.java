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
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.Table;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestIcebergFileMetastoreAutoCleanMetadataFile
        extends AbstractTestQueryFramework
{
    private TrinoCatalog trinoCatalog;
    private IcebergTableOperationsProvider tableOperationsProvider;
    private TrinoFileSystem fileSystem;

    public static final int METADATA_PREVIOUS_VERSIONS_MAX = 5;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.createIcebergQueryRunner();
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(queryRunner);
        tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        CachingHiveMetastore cachingHiveMetastore = memoizeMetastore(metastore, 1000);
        trinoCatalog = new TrinoHiveCatalog(
                new CatalogName("catalog"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                false,
                false,
                false);

        return queryRunner;
    }

    @BeforeClass
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @Test
    public void testInsertWithAutoCleanMetadataFile()
    {
        assertUpdate("CREATE TABLE table_to_file_metadata_count (_bigint BIGINT, _varchar VARCHAR)");
        Table table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("tpch", "table_to_file_metadata_count"));
        table.updateProperties()
                .set("write.metadata.delete-after-commit.enabled", "true")
                .set("write.metadata.previous-versions-max", String.valueOf(METADATA_PREVIOUS_VERSIONS_MAX))
                .commit();
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO table_to_file_metadata_count VALUES (1, 'a')", 1);
        }
        try {
            int count = 0;
            FileIterator fileIterator = fileSystem.listFiles(Location.of(table.location()));
            while (fileIterator.hasNext()) {
                FileEntry next = fileIterator.next();
                if (next.location().path().endsWith("metadata.json")) {
                    count++;
                }
            }
            assertEquals(count, 1 + METADATA_PREVIOUS_VERSIONS_MAX);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
