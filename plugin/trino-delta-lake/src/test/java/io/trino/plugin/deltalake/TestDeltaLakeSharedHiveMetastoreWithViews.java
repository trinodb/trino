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
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeSharedHiveMetastoreWithViews
        extends AbstractTestQueryFramework
{
    protected final String schema = "test_shared_schema_with_hive_views_" + randomNameSuffix();
    private final String bucketName = "delta-lake-shared-hive-with-views-" + randomNameSuffix();
    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        this.hiveMinioDataLake.start();

        DistributedQueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                "delta",
                schema,
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
        queryRunner.execute("CREATE SCHEMA " + schema + " WITH (location = 's3://" + bucketName + "/" + schema + "')");

        queryRunner.installPlugin(new TestingHivePlugin());
        Map<String, String> s3Properties = ImmutableMap.<String, String>builder()
                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("hive.s3.path-style-access", "true")
                .buildOrThrow();
        queryRunner.createCatalog(
                "hive",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .put("hive.allow-drop-table", "true")
                        .putAll(s3Properties)
                        .buildOrThrow());

        queryRunner.execute("CREATE TABLE hive." + schema + ".hive_table (a_integer integer)");
        hiveMinioDataLake.getHiveHadoop().runOnHive("CREATE VIEW " + schema + ".hive_view AS SELECT *  FROM " + schema + ".hive_table");
        queryRunner.execute("CREATE TABLE delta." + schema + ".delta_table (a_varchar varchar)");

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + schema + ".hive_table");
        hiveMinioDataLake.getHiveHadoop().runOnHive("DROP VIEW IF EXISTS " + schema + ".hive_view");
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

        assertThatThrownBy(() -> query("SHOW CREATE TABLE delta." + schema + ".hive_table"))
                .hasMessageContaining("not a Delta Lake table");
        assertThatThrownBy(() -> query("SHOW CREATE TABLE delta." + schema + ".hive_view"))
                .hasMessageContaining("not a Delta Lake table");
        assertThatThrownBy(() -> query("SHOW CREATE TABLE hive." + schema + ".delta_table"))
                .hasMessageContaining("Cannot query Delta Lake table");

        assertThatThrownBy(() -> query("DESCRIBE delta." + schema + ".hive_table"))
                .hasMessageContaining("not a Delta Lake table");
        assertThatThrownBy(() -> query("DESCRIBE hive." + schema + ".delta_table"))
                .hasMessageContaining("Cannot query Delta Lake table");
    }
}
