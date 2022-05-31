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
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

public class TestDeltaLakePreferredPartitioning
        extends AbstractTestQueryFramework
{
    private static final String TEST_BUCKET_NAME = "mock-delta-lake-bucket";
    private static final int WRITE_PARTITIONING_TEST_PARTITIONS_COUNT = 101;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(
                !new ParquetWriterConfig().isParquetOptimizedWriterEnabled(),
                "This test assumes the optimized Parquet writer is disabled by default");

        DockerizedMinioDataLake dockerizedMinioDataLake = closeAfterClass(createDockerizedMinioDataLakeForDeltaLake(TEST_BUCKET_NAME));

        return createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                "default",
                ImmutableMap.of(
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.max-partitions-per-writer", String.valueOf(WRITE_PARTITIONING_TEST_PARTITIONS_COUNT - 1)),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());
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

    private static String getLocationForTable(String tableName)
    {
        return format("s3://%s/%s", TEST_BUCKET_NAME, tableName);
    }

    private Session withForcedPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
                .setSystemProperty(PREFERRED_WRITE_PARTITIONING_MIN_NUMBER_OF_PARTITIONS, "1")
                .build();
    }

    private Session withoutPreferredPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                .build();
    }
}
