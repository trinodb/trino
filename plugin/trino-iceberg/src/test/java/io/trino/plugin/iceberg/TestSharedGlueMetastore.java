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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

/**
 * Tests metadata operations on a schema which has a mix of Hive and Iceberg tables.
 *
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
public class TestSharedGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestSharedGlueMetastore.class);
    private static final String HIVE_CATALOG = "hive";

    private final String schema = "test_shared_glue_schema_" + randomTableSuffix();
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

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of()),
                hdfsConfig,
                new NoHdfsAuthentication());
        this.glueMetastore = new GlueHiveMetastore(
                hdfsEnvironment,
                new GlueHiveMetastoreConfig(),
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

    @Test
    public void testReadInformationSchema()
    {
        assertThat(query("SELECT table_schema FROM hive.information_schema.tables WHERE table_name = 'region'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM iceberg.information_schema.tables WHERE table_name = 'nation'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'region'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'nation'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment')");
        assertQuery("SELECT table_name, column_name from iceberg.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from hive_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES FROM iceberg." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive_with_redirections." + schema, "VALUES 'region', 'nation'");

        assertThatThrownBy(() -> query("SHOW CREATE TABLE iceberg." + schema + ".region"))
                .hasMessageContaining("Not an Iceberg table");
        assertThatThrownBy(() -> query("SHOW CREATE TABLE hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Iceberg table");

        assertThatThrownBy(() -> query("DESCRIBE iceberg." + schema + ".region"))
                .hasMessageContaining("Not an Iceberg table");
        assertThatThrownBy(() -> query("DESCRIBE hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Iceberg table");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(query("SHOW SCHEMAS FROM hive"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM iceberg"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM hive_with_redirections"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION ROLE public\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";

        String showCreateHiveSchema = (String) computeActual("SHOW CREATE SCHEMA hive." + schema).getOnlyValue();
        assertEquals(
                showCreateHiveSchema,
                format(expectedHiveCreateSchema, "hive", schema, dataDirectory));
        String showCreateIcebergSchema = (String) computeActual("SHOW CREATE SCHEMA iceberg." + schema).getOnlyValue();
        assertEquals(
                showCreateIcebergSchema,
                format("CREATE SCHEMA iceberg.%s\n" +
                                "WITH (\n" +
                                "   location = '%s'\n" +
                                ")",
                        schema,
                        dataDirectory));
        String showCreateHiveWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA hive_with_redirections." + schema).getOnlyValue();
        assertEquals(
                showCreateHiveWithRedirectionsSchema,
                format(expectedHiveCreateSchema, "hive_with_redirections", schema, dataDirectory));
    }

    @Test
    public void testSelect()
    {
        assertQuery("SELECT * FROM iceberg." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive." + schema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM hive_with_redirections." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive_with_redirections." + schema + ".region", "SELECT * FROM region");

        assertThatThrownBy(() -> query("SELECT * FROM iceberg." + schema + ".region"))
                .hasMessageContaining("Not an Iceberg table");
        assertThatThrownBy(() -> query("SELECT * FROM hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Iceberg table");
    }
}
