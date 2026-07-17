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
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeSharedHiveMetastoreWithViews
        extends AbstractTestQueryFramework
{
    private final String bucketName = "delta-lake-shared-hive-with-views-" + randomNameSuffix();
    private Hive3FlociDataLake hiveFlociDataLake;
    private String schema;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake(bucketName));
        this.hiveFlociDataLake.start();

        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addMetastoreProperties(hiveFlociDataLake.getHiveHadoop())
                .addS3Properties(hiveFlociDataLake.floci(), bucketName)
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .build();
        try {
            queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data")));
            queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                    .put("hive.metastore", "thrift")
                    .put("hive.metastore.uri", hiveFlociDataLake.getHiveMetastoreEndpoint().toString())
                    .put("fs.s3.enabled", "true")
                    .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                    .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                    .put("s3.endpoint", hiveFlociDataLake.floci().endpoint().toString())
                    .put("s3.region", FLOCI_REGION)
                    .put("s3.path-style-access", "true")
                    .put("s3.streaming.part-size", "5MB") // minimize memory usage
                    .buildOrThrow());

            schema = queryRunner.getDefaultSession().getSchema().orElseThrow();
            queryRunner.execute("CREATE TABLE hive." + schema + ".hive_table (a_integer integer)");
            hiveFlociDataLake.runOnHive("CREATE VIEW " + schema + ".hive_view AS SELECT *  FROM " + schema + ".hive_table");
            queryRunner.execute("CREATE TABLE delta." + schema + ".delta_table (a_varchar varchar)");

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @AfterAll
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + schema + ".hive_table");
        hiveFlociDataLake.runOnHive("DROP VIEW IF EXISTS " + schema + ".hive_view");
        assertQuerySucceeds("DROP TABLE IF EXISTS delta." + schema + ".delta_table");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + schema);
    }

    @Test
    public void testReadInformationSchema()
    {
        assertThat(query("SELECT table_schema FROM hive.information_schema.tables WHERE table_name = 'hive_table' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM delta.information_schema.tables WHERE table_name = 'delta_table' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('hive_table', 'a_integer')");
        assertQuery("SELECT table_name, column_name from delta.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('delta_table', 'a_varchar')");
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES FROM delta." + schema, "VALUES 'hive_table', 'hive_view', 'delta_table'");
        assertQuery("SHOW TABLES FROM hive." + schema, "VALUES 'hive_table', 'hive_view', 'delta_table'");

        assertThat(query("SHOW CREATE TABLE delta." + schema + ".hive_table"))
                .failure().hasMessageContaining("not a Delta Lake table");
        assertThat(query("SHOW CREATE TABLE delta." + schema + ".hive_view"))
                .failure().hasMessageContaining("not a Delta Lake table");
        assertThat(query("SHOW CREATE TABLE hive." + schema + ".delta_table"))
                .failure().hasMessageContaining("Cannot query Delta Lake table");

        assertThat(query("DESCRIBE delta." + schema + ".hive_table"))
                .failure().hasMessageContaining("not a Delta Lake table");
        assertThat(query("DESCRIBE hive." + schema + ".delta_table"))
                .failure().hasMessageContaining("Cannot query Delta Lake table");
    }
}
