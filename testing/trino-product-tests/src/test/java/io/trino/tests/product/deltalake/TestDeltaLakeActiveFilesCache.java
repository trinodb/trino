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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests Delta Lake active files cache behavior.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeActiveFilesCache
{
    @Test
    void testRefreshTheFilesCacheWhenTableIsRecreated(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_cached_table_files_refresh_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1");
        // Add the files of the table in the active files cache
        assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1));

        // Recreate the table outside Trino to avoid updating the Trino table active files cache
        env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        // Delete the contents of the table explicitly from storage (because it has been created as `EXTERNAL`)
        removeS3Directory(env, env.getBucketName(), tableDirectory);

        env.executeSparkUpdate(format("CREATE TABLE default.%s (col INTEGER) USING DELTA LOCATION 's3://%s/%s'",
                tableName,
                env.getBucketName(),
                tableDirectory));
        env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES 2");

        // TODO https://github.com/trinodb/trino/issues/13737 Fix failure when active files cache is stale
        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM delta.default." + tableName))
                .hasMessageContaining("Error opening Hive split");

        // Verify flushing cache resolve the query failure
        env.executeTrinoUpdate("CALL delta.system.flush_metadata_cache(schema_name => 'default', table_name => '" + tableName + "')");
        assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(2));

        env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
    }

    private void removeS3Directory(DeltaLakeMinioEnvironment env, String bucket, String directory)
    {
        try (MinioClient minioClient = env.createMinioClient()) {
            for (String objectKey : minioClient.listObjects(bucket, directory)) {
                minioClient.removeObject(bucket, objectKey);
            }
        }
    }
}
