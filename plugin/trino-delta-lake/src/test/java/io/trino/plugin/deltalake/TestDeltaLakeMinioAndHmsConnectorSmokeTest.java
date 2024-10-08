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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Delta Lake connector smoke test exercising Hive metastore and MinIO storage with exclusive create support enabled.
 */
public class TestDeltaLakeMinioAndHmsConnectorSmokeTest
        extends BaseDeltaLakeAwsConnectorSmokeTest
{
    @Override
    protected Map<String, String> hiveStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("s3.path-style-access", "true")
                .put("s3.max-connections", "2")
                .buildOrThrow();
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("s3.path-style-access", "true")
                .put("s3.streaming.part-size", "5MB") // minimize memory usage
                .put("s3.max-connections", "4") // verify no leaks
                .buildOrThrow();
    }

    @Test
    public void testDeltaColumnInvariant()
    {
        String tableName = "test_invariants_" + randomNameSuffix();
        hiveMinioDataLake.copyResources("deltalake/invariants", tableName);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, getLocationForTable(bucketName, tableName)));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("INSERT INTO " + tableName + " VALUES(2)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1), (2)");

        assertThat(query("INSERT INTO " + tableName + " VALUES(3)"))
                .failure().hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");
        assertThat(query("UPDATE " + tableName + " SET dummy = 3 WHERE dummy = 1"))
                .failure().hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");

        assertQuery("SELECT * FROM " + tableName, "VALUES (1), (2)");
    }

    /**
     * @see databricks122.invariants_writer_feature
     */
    @Test
    public void testDeltaColumnInvariantWriterFeature()
    {
        String tableName = "test_invariants_writer_feature_" + randomNameSuffix();
        hiveMinioDataLake.copyResources("databricks122/invariants_writer_feature", tableName);
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, getLocationForTable(bucketName, tableName)));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES 1, 2");

        assertThat(query("INSERT INTO " + tableName + " VALUES 3"))
                .failure().hasMessageContaining("Check constraint violation: (\"col_invariants\" < 3)");
        assertThat(query("UPDATE " + tableName + " SET col_invariants = 3 WHERE col_invariants = 1"))
                .failure().hasMessageContaining("Check constraint violation: (\"col_invariants\" < 3)");

        assertQuery("SELECT * FROM " + tableName, "VALUES 1, 2");
    }

    @Test
    public void testSchemaEvolutionOnTableWithColumnInvariant()
    {
        String tableName = "test_schema_evolution_on_table_with_column_invariant_" + randomNameSuffix();
        hiveMinioDataLake.copyResources("deltalake/invariants", tableName);
        getQueryRunner().execute(format(
                "CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')",
                tableName,
                getLocationForTable(bucketName, tableName)));

        assertThat(query("INSERT INTO " + tableName + " VALUES(3)"))
                .failure().hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c INT");
        assertUpdate("COMMENT ON COLUMN " + tableName + ".c IS 'example column comment'");
        assertUpdate("COMMENT ON TABLE " + tableName + " IS 'example table comment'");

        assertThat(query("INSERT INTO " + tableName + " VALUES(3, 30)"))
                .failure().hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");

        assertUpdate("INSERT INTO " + tableName + " VALUES(2, 20)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, NULL), (2, 20)");
    }
}
