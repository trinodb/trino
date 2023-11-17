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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import java.io.File;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMergeAppend
        extends AbstractTestQueryFramework
{
    private TrinoCatalog trinoCatalog;
    private IcebergTableOperationsProvider tableOperationsProvider;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.createIcebergQueryRunner();
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(createTestingFileHiveMetastore(baseDir), 1000);
        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(queryRunner);
        tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        trinoCatalog = new TrinoHiveCatalog(
                new CatalogName("catalog"),
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
    public void testInsertWithAppend()
    {
        assertUpdate("CREATE TABLE table_to_insert (_bigint BIGINT, _varchar VARCHAR)");
        Table table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("tpch", "table_to_insert"));
        table.updateProperties()
                .set("commit.manifest.min-count-to-merge", "2")
                .commit();
        assertUpdate("INSERT INTO table_to_insert VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        MaterializedResult result = computeActual("select * from \"table_to_insert$manifests\"");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertUpdate("INSERT INTO table_to_insert VALUES (4, 'd')", 1);
        result = computeActual("select * from \"table_to_insert$manifests\"");
        assertThat(result.getRowCount()).isEqualTo(1);
    }
}
