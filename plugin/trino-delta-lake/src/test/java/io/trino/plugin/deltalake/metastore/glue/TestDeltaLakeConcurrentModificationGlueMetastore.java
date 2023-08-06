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
package io.trino.plugin.deltalake.metastore.glue;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.glue.DefaultGlueColumnStatisticsProviderFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.reflect.Reflection.newProxy;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestDeltaLakeConcurrentModificationGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "test_delta_lake_concurrent";
    private static final String SCHEMA = "test_delta_lake_glue_concurrent_" + randomNameSuffix();
    private Path dataDirectory;
    private GlueHiveMetastore metastore;
    private final AtomicBoolean failNextGlueDeleteTableCall = new AtomicBoolean(false);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("data_delta_concurrent");
        GlueMetastoreStats stats = new GlueMetastoreStats();
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(dataDirectory.toUri().toString());

        AWSGlueAsync glueClient = createAsyncGlueClient(glueConfig, DefaultAWSCredentialsProviderChain.getInstance(), ImmutableSet.of(), stats.newRequestMetricsCollector());
        AWSGlueAsync proxiedGlueClient = newProxy(AWSGlueAsync.class, (proxy, method, args) -> {
            Object result;
            try {
                if (method.getName().equals("deleteTable") && failNextGlueDeleteTableCall.get()) {
                    // Simulate concurrent modifications on the table that is about to be dropped
                    failNextGlueDeleteTableCall.set(false);
                    throw new TrinoException(HIVE_METASTORE_ERROR, new ConcurrentModificationException("Test-simulated metastore concurrent modification exception"));
                }
                result = method.invoke(glueClient, args);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
            return result;
        });

        metastore = new GlueHiveMetastore(
                HDFS_FILE_SYSTEM_FACTORY,
                glueConfig,
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()),
                proxiedGlueClient,
                stats,
                table -> true);

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), Optional.empty(), EMPTY_MODULE));
        queryRunner.createCatalog(CATALOG_NAME, "delta_lake");
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);
        return queryRunner;
    }

    @Test
    public void testDropTableWithConcurrentModifications()
    {
        String tableName = "test_glue_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS data", 1);

        failNextGlueDeleteTableCall.set(true);
        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }
}
