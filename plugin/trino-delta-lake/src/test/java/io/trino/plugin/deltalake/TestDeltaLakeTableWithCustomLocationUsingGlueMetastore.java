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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.glue.DefaultGlueColumnStatisticsProviderFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.io.File;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeltaLakeTableWithCustomLocationUsingGlueMetastore
        extends BaseDeltaLakeTableWithCustomLocation
{
    private static final Logger LOG = Logger.get(TestDeltaLakeSharedGlueMetastoreWithTableRedirections.class);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        this.metastoreDir = new File(queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString());
        this.metastoreDir.deleteOnExit();

        queryRunner.installPlugin(new DeltaLakePlugin());
        queryRunner.createCatalog(
                CATALOG_NAME,
                CONNECTOR_NAME,
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.region", "us-east-2")
                        .put("hive.metastore.glue.default-warehouse-dir", metastoreDir.getPath())
                        .buildOrThrow());

        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsEnvironment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of()),
                hdfsConfig,
                new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setGlueRegion("us-east-2");
        metastore = new GlueHiveMetastore(
                hdfsEnvironment,
                glueConfig,
                DefaultAWSCredentialsProviderChain.getInstance(),
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()),
                Optional.empty(),
                table -> true);
        hdfsContext = new HdfsEnvironment.HdfsContext(queryRunner.getDefaultSession().toConnectorSession());

        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = '" + metastoreDir.getPath() + "')");
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        try {
            if (metastore != null) {
                // Data is on the local disk and will be deleted by the deleteOnExit hook
                metastore.dropDatabase(SCHEMA, false);
                deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", SCHEMA);
        }
    }
}
