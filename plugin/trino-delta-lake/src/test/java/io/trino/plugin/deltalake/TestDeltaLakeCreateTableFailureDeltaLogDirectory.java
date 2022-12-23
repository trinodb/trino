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
package io.trino.plugin.deltalake;

import io.trino.Session;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeCreateTableFailureDeltaLogDirectory
        extends AbstractTestQueryFramework
{
    private String dataDirectory;
    private HiveMetastore metastore;
    private final AtomicReference<Path> deltaLogDirectory = new AtomicReference<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString();
        this.metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_ENVIRONMENT,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(dataDirectory)
                        .setMetastoreUser("test"))
        {
            @Override
            public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
            {
                deltaLogDirectory.set(Path.of(table.getStorage().getLocation(), "_delta_log"));
                assertThat(deltaLogDirectory.get()).isDirectory().exists();
                System.out.println("TestDeltaLakeCreateTableFailureDeltaLogDirectory: " + deltaLogDirectory);
                throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Failed to access table location: " + table.getStorage().getLocation());
            }
        };

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), EMPTY_MODULE));
        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, Map.of());
        queryRunner.execute("CREATE SCHEMA tpch");
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(TPCH_SCHEMA, false);
            deleteRecursively(Path.of(dataDirectory), ALLOW_INSECURE);
        }
    }

    @Test
    public void testCreateTableFailureRemoveDeltaLogDirectory()
    {
        // Verify _delta_log directory is deleted if CREATE TABLE failed
        assertQueryFails("CREATE TABLE test_create_table_failure (col INT)", "Failed to access table location: .*");
        assertThat(deltaLogDirectory.get()).doesNotExist();
    }
}
