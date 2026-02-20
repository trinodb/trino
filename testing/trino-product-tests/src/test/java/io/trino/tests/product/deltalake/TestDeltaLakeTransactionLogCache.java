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
import io.trino.testing.containers.environment.Row;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * JUnit 5 port of Delta Lake transaction log cache tests from TestDeltaLakeTransactionLogCache.
 * <p>
 * This class ports only the tests marked with DELTA_LAKE_OSS group.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeTransactionLogCache
{
    @Test
    void testAllDataFilesAreLoadedWhenTransactionLogFileAfterTheCachedTableVersionIsMissing(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_cached_table_files_accuracy_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s', checkpoint_interval = 10)".formatted(
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1));

            // Perform multiple changes on the table outside of Trino to avoid updating the Trino table active files cache
            env.executeSparkUpdate("DELETE FROM default." + tableName);
            // Perform more than 10 to make sure there is a checkpoint being created
            IntStream.range(2, 13).forEach(v -> env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES " + v));

            List<Row> expectedRows = List.of(
                    row(2),
                    row(3),
                    row(4),
                    row(5),
                    row(6),
                    row(7),
                    row(8),
                    row(9),
                    row(10),
                    row(11),
                    row(12));

            // Delete the first few transaction log files because they can safely be discarded
            // once there is a checkpoint created.
            String[] transactionLogFilesToRemove = {
                    tableDirectory + "/_delta_log/00000000000000000000.json",
                    tableDirectory + "/_delta_log/00000000000000000001.json",
                    tableDirectory + "/_delta_log/00000000000000000002.json",
                    tableDirectory + "/_delta_log/00000000000000000003.json",
                    tableDirectory + "/_delta_log/00000000000000000004.json",
                    tableDirectory + "/_delta_log/00000000000000000005.json"
            };

            try (MinioClient minioClient = env.createMinioClient()) {
                for (String key : transactionLogFilesToRemove) {
                    minioClient.removeObject(env.getBucketName(), key);
                }
            }

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            // The internal data files table cached value for the Delta table should be
            // fully refreshed now.
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }
}
