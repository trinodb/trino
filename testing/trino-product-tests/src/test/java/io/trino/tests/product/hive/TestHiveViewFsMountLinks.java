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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.BUCKET_PREFIX;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.S3_MOUNT_PATH;

/**
 * Tests Hive tables whose {@code hdfs://} location is resolved by the Hadoop ViewFS overload
 * scheme ({@code ViewDistributedFileSystem}), either to the HDFS fallback or to an {@code s3a://}
 * mount link.
 * <p>
 * Without the legacy S3 file system, nothing provides a Hadoop {@code FileSystem} for {@code s3a},
 * so ViewFS falls back to {@code org.apache.hadoop.fs.s3a.S3AFileSystem}, which Trino does not ship,
 * and reading tables behind an {@code s3a://} mount link fails with {@code ClassNotFoundException}.
 */
@ProductTest
@RequiresEnvironment(HiveViewFsEnvironment.class)
@TestGroup.HiveViewFs
@TestGroup.ProfileSpecificTests
class TestHiveViewFsMountLinks
{
    @Test
    void testReadTableOnViewFsHdfsFallback(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_hdfs_fallback";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)");
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testReadTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // hdfs:// alias that ViewFS resolves to s3a://viewfs-bucket/s3_data/<table>
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n3,c\n");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadPartitionedTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_partitioned";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    PARTITIONED BY (event_date STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "event_date=2026-11-01/data.csv", "1,a\n2,b\n");
            env.putS3Object(objectPrefix + "event_date=2026-11-02/data.csv", "3,c\n");

            env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");

            assertThat(env.executeTrino("SELECT id, name, event_date FROM hive.default." + tableName))
                    .containsOnly(
                            row(1, "a", "2026-11-01"),
                            row(2, "b", "2026-11-01"),
                            row(3, "c", "2026-11-02"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }
}
