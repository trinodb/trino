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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

public class TestDeltaLakePreferredPartitioning
        extends AbstractTestQueryFramework
{
    private static final int WRITE_PARTITIONING_TEST_PARTITIONS_COUNT = 101;

    private final String bucketName = "mock-delta-lake-bucket-" + randomNameSuffix();
    protected Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);

        String schema = "default";
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(schema)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new DeltaLakePlugin());
            queryRunner.createCatalog(DELTA_CATALOG, DeltaLakeConnectorFactory.CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("file-metastore").toString())
                    .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                    .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                    .put("hive.s3.endpoint", minio.getMinioAddress())
                    .put("hive.s3.path-style-access", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .put("delta.max-partitions-per-writer", String.valueOf(WRITE_PARTITIONING_TEST_PARTITIONS_COUNT - 1))
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA " + schema + " WITH (location = 's3://" + bucketName + "/" + schema + "')");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @Test
    public void testPreferredWritePartitioningCreateTable()
    {
        String partitionedTableName1 = generateRandomTableName();
        String partitionedTableName2 = generateRandomTableName();

        assertUpdate(
                withForcedPreferredPartitioning(),
                format(
                        "CREATE TABLE IF NOT EXISTS %s " +
                                "WITH (location = '%s', partitioned_by = ARRAY['partkey']) AS " +
                                "SELECT orderkey, partkey %% %d AS partkey FROM tpch.tiny.lineitem",
                        partitionedTableName1,
                        getLocationForTable(partitionedTableName1),
                        WRITE_PARTITIONING_TEST_PARTITIONS_COUNT),
                60175);

        // cannot create > 100 partitions when preferred write partitioning is disabled
        assertQueryFails(
                withoutPreferredPartitioning(),
                format(
                        "CREATE TABLE IF NOT EXISTS %s " +
                                "WITH (location = '%s', partitioned_by = ARRAY['partkey']) AS " +
                                "SELECT orderkey, partkey %% %d AS partkey FROM tpch.tiny.lineitem",
                        partitionedTableName2,
                        getLocationForTable(partitionedTableName2),
                        WRITE_PARTITIONING_TEST_PARTITIONS_COUNT),
                "Exceeded limit of 100 open writers for partitions");
    }

    @Test
    public void testPreferredWritePartitioningInsertIntoTable()
    {
        String partitionedTableName = generateRandomTableName();
        createEmptyPartitionedTable(partitionedTableName);

        assertUpdate(
                withForcedPreferredPartitioning(),
                format(
                        "INSERT INTO %s SELECT orderkey, partkey %% %d AS partkey FROM tpch.tiny.lineitem",
                        partitionedTableName,
                        WRITE_PARTITIONING_TEST_PARTITIONS_COUNT),
                60175);

        // cannot insert > 100 records to N partitions when preferred write partitioning is disabled
        assertQueryFails(
                withoutPreferredPartitioning(),
                format(
                        "INSERT INTO %s SELECT orderkey, partkey %% %d AS partkey FROM tpch.tiny.lineitem",
                        partitionedTableName,
                        WRITE_PARTITIONING_TEST_PARTITIONS_COUNT),
                "Exceeded limit of 100 open writers for partitions");
    }

    private void createEmptyPartitionedTable(String tableName)
    {
        getQueryRunner().execute(
                withForcedPreferredPartitioning(),
                format(
                        "CREATE TABLE IF NOT EXISTS %s (orderkey bigint, partkey bigint) " +
                                "WITH (location = '%s', partitioned_by = ARRAY['partkey'])",
                        tableName,
                        getLocationForTable(tableName)));
    }

    private static String generateRandomTableName()
    {
        return "table_" + randomUUID().toString().replaceAll("-", "");
    }

    private String getLocationForTable(String tableName)
    {
        return format("s3://%s/%s", bucketName, tableName);
    }

    private Session withForcedPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                // It is important to explicitly set partitioned writer count to 1 since in above tests we are testing
                // the open writers limit for partitions. So, with default value of 32 writer count, we will never
                // hit that limit thus, tests will fail.
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "1")
                .build();
    }

    private Session withoutPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                // It is important to explicitly set partitioned writer count to 1 since in above tests we are testing
                // the open writers limit for partitions. So, with default value of 32 writer count, we will never
                // hit that limit thus, tests will fail.
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, "1")
                .build();
    }
}
