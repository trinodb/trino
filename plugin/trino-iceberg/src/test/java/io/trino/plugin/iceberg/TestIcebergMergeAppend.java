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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
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
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static org.testng.Assert.assertEquals;

public class TestIcebergMergeAppend
        extends AbstractTestQueryFramework
{
    private TrinoCatalog trinoCatalog;
    private IcebergTableOperationsProvider tableOperationsProvider;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.createIcebergQueryRunner();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                hdfsEnvironment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));
        tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        trinoCatalog = new TrinoHiveCatalog(
                new CatalogName("catalog"),
                memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                tableOperationsProvider,
                "trino-version",
                false,
                false,
                false);

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
        assertEquals(result.getRowCount(), 1);
        assertUpdate("INSERT INTO table_to_insert VALUES (4, 'd')", 1);
        result = computeActual("select * from \"table_to_insert$manifests\"");
        assertEquals(result.getRowCount(), 1);
    }
}
