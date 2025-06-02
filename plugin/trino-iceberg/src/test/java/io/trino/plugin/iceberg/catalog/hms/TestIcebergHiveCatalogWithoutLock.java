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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.Hive4MinioDataLake;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergHiveCatalogWithoutLock
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-bucket" + randomNameSuffix();
        Hive4MinioDataLake hiveMinioDataLake = closeAfterClass(new Hive4MinioDataLake(bucketName));
        hiveMinioDataLake.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", hiveMinioDataLake.getHiveMetastoreEndpoint().toString())
                                .put("iceberg.hive-catalog.locking-enabled", "false")
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                                .put("s3.path-style-access", "true")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName("tpch")
                                .withSchemaProperties(Map.of("location", "'s3://%s/tpch'".formatted(bucketName)))
                                .build())
                .build();
    }

    @Test
    void testCommitWithoutLock()
    {
        try (TestTable table = newTrinoTable("test_lock", "(x int)", List.of("1", "2", "3"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 2, 3");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 3");
        }
    }
}
