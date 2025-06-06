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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.BaseSharedMetastoreTest;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;

import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Tests metadata operations on a schema which has a mix of Hive and Iceberg tables.
 * <p>
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSharedGlueMetastore
        extends BaseSharedMetastoreTest
{
    private static final Logger LOG = Logger.get(TestSharedGlueMetastore.class);
    private static final String HIVE_CATALOG = "hive";

    private Path dataDirectory;
    private GlueHiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(tpchSchema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(tpchSchema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

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
                        "hive.metastore.glue.default-warehouse-dir", dataDirectory.toString(),
                        "fs.hadoop.enabled", "true"));
        queryRunner.createCatalog(
                "iceberg_with_redirections",
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "glue",
                        "hive.metastore.glue.default-warehouse-dir", dataDirectory.toString(),
                        "iceberg.hive-catalog-name", "hive",
                        "fs.hadoop.enabled", "true"));

        this.glueMetastore = createTestingGlueHiveMetastore(dataDirectory, this::closeAfterClass);
        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data"), glueMetastore));
        queryRunner.createCatalog(HIVE_CATALOG, "hive", ImmutableMap.of("fs.hadoop.enabled", "true"));
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.of("hive.iceberg-catalog-name", "iceberg", "fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA " + tpchSchema + " WITH (location = '" + dataDirectory.toUri() + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, icebergSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));
        queryRunner.execute("CREATE SCHEMA " + testSchema + " WITH (location = '" + dataDirectory.toUri() + "')");

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueMetastore != null) {
                // Data is on the local disk and will be deleted by the deleteOnExit hook
                glueMetastore.dropDatabase(tpchSchema, false);
                glueMetastore.dropDatabase(testSchema, false);
                glueMetastore.shutdown();
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s or %s", tpchSchema, testSchema);
        }
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";

        return format(expectedHiveCreateSchema, catalogName, tpchSchema, dataDirectory.toUri());
    }

    @Override
    protected String getExpectedIcebergCreateSchema(String catalogName)
    {
        String expectedIcebergCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";
        return format(expectedIcebergCreateSchema, catalogName, tpchSchema, dataDirectory.toUri());
    }
}
