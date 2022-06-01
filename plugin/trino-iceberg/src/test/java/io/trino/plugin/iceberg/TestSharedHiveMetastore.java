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
import io.trino.Session;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestSharedHiveMetastore
        extends BaseSharedMetastoreTest
{
    private static final String HIVE_CATALOG = "hive";

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
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", dataDirectory.toString()));
        queryRunner.createCatalog(
                "iceberg_with_redirections",
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", dataDirectory.toString(),
                        "iceberg.hive-catalog-name", "hive"));

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of()),
                hdfsConfig,
                new NoHdfsAuthentication());
        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                hdfsEnvironment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(dataDirectory.toFile().toURI().toString())
                        .setMetastoreUser("test"));
        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog(HIVE_CATALOG, "hive", ImmutableMap.of("hive.allow-drop-table", "true"));
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.of("hive.iceberg-catalog-name", "iceberg"));

        queryRunner.execute("CREATE SCHEMA " + schema);
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, icebergSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + schema + ".region");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg." + schema + ".nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + schema);
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION USER user\n" +
                "WITH (\n" +
                "   location = 'file:%s/%s'\n" +
                ")";

        return format(expectedHiveCreateSchema, catalogName, schema, dataDirectory, schema);
    }

    @Override
    protected String getExpectedIcebergCreateSchema(String catalogName)
    {
        String expectedIcebergCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION USER user\n" +
                "WITH (\n" +
                "   location = '%s/%s'\n" +
                ")";
        return format(expectedIcebergCreateSchema, catalogName, schema, dataDirectory, schema);
    }
}
