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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.Hive4MinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static org.junit.jupiter.api.Assumptions.abort;

@Execution(ExecutionMode.SAME_THREAD) // TODO Make custom hive4 image to support running queries concurrently
class TestHive4OnDataLake
        extends BaseTestHiveOnDataLake
{
    private Hive4MinioDataLake hiveMinioDataLake;
    private HiveMetastore metastoreClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake = closeAfterClass(new Hive4MinioDataLake(bucketName()));

        this.hiveMinioDataLake.start();
        this.metastoreClient = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(this.hiveMinioDataLake.getHiveMetastore().getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                .addExtraProperty("sql.path", "hive.functions")
                .addExtraProperty("sql.default-function-catalog", "hive")
                .addExtraProperty("sql.default-function-schema", "functions")
                .setHiveProperties(
                        ImmutableMap.<String, String>builder()
                                .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                                .put("hive.non-managed-table-writes-enabled", "true")
                                // Below are required to enable caching on metastore
                                .put("hive.metastore-cache-ttl", "1d")
                                .put("hive.metastore-refresh-interval", "1d")
                                // This is required to reduce memory pressure to test writing large files
                                .put("s3.streaming.part-size", HIVE_S3_STREAMING_PART_SIZE.toString())
                                // This is required to enable AWS Athena partition projection
                                .put("hive.partition-projection-enabled", "true")
                                .buildOrThrow())
                .build();
    }

    @Override
    Minio minio()
    {
        return hiveMinioDataLake.getMinio();
    }

    @Override
    HiveMetastore metastoreClient()
    {
        return metastoreClient;
    }

    @Override
    MinioClient minioClient()
    {
        return hiveMinioDataLake.getMinioClient();
    }

    @Override
    String runOnHive(String query)
    {
        return hiveMinioDataLake.getHiveServer().runOnHive(query);
    }

    @Override
    @Test
    public void testSyncPartitionOnBucketRoot()
    {
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testUnpartitionedTableExternalLocationOnTopOfTheBucket()
    {
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testPartitionedTableExternalLocationOnTopOfTheBucket()
    {
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testInsertOverwritePartitionedAndBucketedAcidTable()
    {
        abort("Fails with `Processor has no capabilities, cannot create an ACID table`");
    }
}
