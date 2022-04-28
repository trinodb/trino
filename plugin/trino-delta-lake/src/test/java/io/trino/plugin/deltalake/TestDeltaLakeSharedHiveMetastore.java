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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.nio.file.Path;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDeltaLakeSharedHiveMetastore
        extends BaseDeltaLakeSharedMetastoreTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog("delta")
                .setSchema(schema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(schema)
                .build();

        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(deltaLakeSession);
        DistributedQueryRunner queryRunner = builder.build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta");

        queryRunner.installPlugin(new TestingDeltaLakePlugin());
        queryRunner.createCatalog(
                "delta",
                "delta-lake",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", dataDir.toString())
                        .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + schema);

        queryRunner.installPlugin(new TestingHivePlugin());
        queryRunner.createCatalog(
                "hive",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", dataDir.toString())
                        .put("hive.allow-drop-table", "true")
                        .buildOrThrow());

        queryRunner.createCatalog(
                "delta_with_redirections",
                "delta-lake",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", dataDir.toString())
                        .put("delta.hive-catalog-name", "hive")
                        .buildOrThrow());

        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", dataDir.toString())
                        .put("hive.delta-lake-catalog-name", "delta")
                        .buildOrThrow());

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, deltaLakeSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + schema + ".region");
        assertQuerySucceeds("DROP TABLE IF EXISTS delta." + schema + ".nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + schema);
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        Path dataDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("delta");
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION USER user\n" +
                "WITH (\n" +
                "   location = '%s/%s'\n" +
                ")";
        return format(expectedHiveCreateSchema, catalogName, schema, dataDir, schema);
    }

    @Override
    protected String getExpectedDeltaLakeCreateSchema(String catalogName)
    {
        Path dataDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("delta");
        String expectedDeltaLakeCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s/%s'\n" +
                ")";
        return format(expectedDeltaLakeCreateSchema, catalogName, schema, dataDir, schema);
    }
}
