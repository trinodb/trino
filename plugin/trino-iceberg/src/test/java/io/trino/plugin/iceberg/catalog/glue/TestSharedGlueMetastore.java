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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.glue.DefaultGlueColumnStatisticsProviderFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.iceberg.BaseSharedMetastoreTest;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

/**
 * Tests metadata operations on a schema which has a mix of Hive and Iceberg tables.
 * <p>
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
public class TestSharedGlueMetastore
        extends BaseSharedMetastoreTest
{
    private static final Logger LOG = Logger.get(TestSharedGlueMetastore.class);
    private static final String HIVE_CATALOG = "hive";

    private Path dataDirectory;
    private HiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(schema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(schema)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        this.dataDirectory.toFile().deleteOnExit();

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "glue",
                        "hive.metastore.glue.default-warehouse-dir", dataDirectory.toString()));
        queryRunner.createCatalog(
                "iceberg_with_redirections",
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "glue",
                        "hive.metastore.glue.default-warehouse-dir", dataDirectory.toString(),
                        "iceberg.hive-catalog-name", "hive"));

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of()),
                hdfsConfig,
                new NoHdfsAuthentication());
        this.glueMetastore = new GlueHiveMetastore(
                hdfsEnvironment,
                new GlueHiveMetastoreConfig(),
                DefaultAWSCredentialsProviderChain.getInstance(),
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(new GlueHiveMetastoreConfig(), directExecutor(), directExecutor()),
                Optional.empty(),
                table -> true);
        queryRunner.installPlugin(new TestingHivePlugin(glueMetastore));
        queryRunner.createCatalog(HIVE_CATALOG, "hive");
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.of("hive.iceberg-catalog-name", "iceberg"));

        queryRunner.execute("CREATE SCHEMA " + schema + " WITH (location = '" + dataDirectory.toString() + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, icebergSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        try {
            if (glueMetastore != null) {
                // Data is on the local disk and will be deleted by the deleteOnExit hook
                glueMetastore.dropDatabase(schema, false);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", schema);
        }
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION ROLE public\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";

        return format(expectedHiveCreateSchema, catalogName, schema, dataDirectory);
    }

    @Override
    protected String getExpectedIcebergCreateSchema(String catalogName)
    {
        String expectedIcebergCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";
        return format(expectedIcebergCreateSchema, catalogName, schema, dataDirectory, schema);
    }
}
