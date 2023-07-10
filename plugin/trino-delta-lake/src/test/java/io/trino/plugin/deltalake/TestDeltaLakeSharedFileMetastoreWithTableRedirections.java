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
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDeltaLakeSharedFileMetastoreWithTableRedirections
        extends BaseDeltaLakeSharedMetastoreWithTableRedirectionsTest
{
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog("delta_with_redirections")
                .setSchema(schema)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();
        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("data");

        queryRunner.installPlugin(new TestingDeltaLakePlugin());
        Map<String, String> deltaLakeProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", dataDirectory.toString())
                .put("delta.enable-non-concurrent-writes", "true")
                .put("delta.hive-catalog-name", "hive_with_redirections")
                .buildOrThrow();

        queryRunner.createCatalog("delta_with_redirections", CONNECTOR_NAME, deltaLakeProperties);
        queryRunner.execute("CREATE SCHEMA " + schema);

        queryRunner.installPlugin(new TestingHivePlugin());

        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", dataDirectory.toString())
                        .put("hive.delta-lake-catalog-name", "delta_with_redirections")
                        .buildOrThrow());

        queryRunner.execute("CREATE TABLE hive_with_redirections." + schema + ".hive_table (a_integer) WITH (format='PARQUET') AS VALUES 1, 2, 3");
        queryRunner.execute("CREATE TABLE delta_with_redirections." + schema + ".delta_table (a_varchar) AS VALUES 'a', 'b', 'c'");

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        getQueryRunner().execute("DROP TABLE IF EXISTS hive_with_redirections." + schema + ".region");
        getQueryRunner().execute("DROP TABLE IF EXISTS delta_with_redirections." + schema + ".nation");
        getQueryRunner().execute("DROP SCHEMA IF EXISTS hive." + schema);
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s/%s'\n" +
                ")";
        return format(expectedHiveCreateSchema, catalogName, schema, dataDirectory, schema);
    }

    @Override
    protected String getExpectedDeltaLakeCreateSchema(String catalogName)
    {
        String expectedDeltaLakeCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s/%s'\n" +
                ")";
        return format(expectedDeltaLakeCreateSchema, catalogName, schema, dataDirectory, schema);
    }
}
