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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeltaLakeCreateSchemaInternalRetry
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "delta_lake";
    private static final String TEST_SCHEMA_TIMEOUT = "test_delta_lake_schema_" + randomNameSuffix();
    private static final String TEST_SCHEMA_DIFFERENT_SESSION = "test_delta_lake_schema_" + randomNameSuffix();

    private String dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
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
            public synchronized void createDatabase(Database database)
            {
                if (database.getDatabaseName().equals(TEST_SCHEMA_DIFFERENT_SESSION)) {
                    // By modifying query id test simulates that schema was created from different session.
                    database = Database.builder(database)
                            .setParameters(ImmutableMap.of(PRESTO_QUERY_ID_NAME, "new_query_id"))
                            .build();
                }
                // Simulate retry mechanism with timeout failure.
                // 1. createDatabase correctly create schema but timeout is triggered
                // 2. Retry to createDatabase throws SchemaAlreadyExistsException
                super.createDatabase(database);
                throw new SchemaAlreadyExistsException(database.getDatabaseName());
            }
        };

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), Optional.empty(), EMPTY_MODULE));
        queryRunner.createCatalog(CATALOG_NAME, CONNECTOR_NAME, Map.of());
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(TEST_SCHEMA_TIMEOUT, false);
            metastore.dropDatabase(TEST_SCHEMA_DIFFERENT_SESSION, false);
            deleteRecursively(Path.of(dataDirectory), ALLOW_INSECURE);
        }
    }

    @Test
    public void testSchemaCreationWithTimeout()
    {
        assertQuerySucceeds("CREATE SCHEMA " + TEST_SCHEMA_TIMEOUT);
        assertQuery("SHOW SCHEMAS LIKE '" + TEST_SCHEMA_TIMEOUT + "'", "VALUES ('" + TEST_SCHEMA_TIMEOUT + "')");
    }

    @Test
    public void testSchemaCreationFailsWhenCreatedWithDifferentSession()
    {
        assertQueryFails("CREATE SCHEMA " + TEST_SCHEMA_DIFFERENT_SESSION, "Schema already exists: '" + TEST_SCHEMA_DIFFERENT_SESSION + "'");
        assertQuery("SHOW SCHEMAS LIKE '" + TEST_SCHEMA_DIFFERENT_SESSION + "'", "VALUES ('" + TEST_SCHEMA_DIFFERENT_SESSION + "')");
    }
}
