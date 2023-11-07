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
import io.trino.Session;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergCreateTableInternalRetry
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA_NAME = "iceberg_internal_retry_schema";
    private File metastoreDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        metastoreDir = queryRunner.getCoordinator().getBaseDataDir().resolve("test_iceberg_table_smoke_test").toFile();
        this.metastoreDir.deleteOnExit();
        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_FILE_SYSTEM_FACTORY,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(metastoreDir.toURI().toString())
                        .setMetastoreUser("test"))
        {
            @Override
            public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
            {
                // Simulate retry mechanism with timeout failure of ThriftHiveMetastore.
                // 1. createTable correctly create table but timeout is triggered
                // 2. Retry to createTable throws TableAlreadyExistsException
                super.createTable(table, principalPrivileges);
                throw new TableAlreadyExistsException(table.getSchemaTableName());
            }
        };

        queryRunner.installPlugin(new TestingIcebergPlugin(Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)), Optional.empty(), EMPTY_MODULE));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of("iceberg.register-table-procedure.enabled", "true"));
        queryRunner.execute("CREATE SCHEMA " + SCHEMA_NAME);
        return queryRunner;
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateTableInternalRetry()
    {
        assertQuerySucceeds("CREATE TABLE test_ct_internal_retry(a int)");
        assertQuery("SHOW TABLES LIKE 'test_ct_internal_retry'", "VALUES 'test_ct_internal_retry'");
    }

    @Test
    public void testCreateTableAsSelectInternalRetry()
    {
        assertQuerySucceeds("CREATE TABLE test_ctas_internal_retry AS SELECT 1 a");
        assertQuery("SHOW TABLES LIKE 'test_ctas_internal_retry'", "VALUES 'test_ctas_internal_retry'");
    }
}
