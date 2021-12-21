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

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingMetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseTestHiveOnDataLake
        extends AbstractTestQueryFramework
{
    private static final String HIVE_TEST_SCHEMA = "hive_insert_overwrite";

    private String bucketName;
    private HiveMinioDataLake dockerizedS3DataLake;
    private HiveMetastore metastoreClient;

    private final String hiveHadoopImage;

    public BaseTestHiveOnDataLake(String hiveHadoopImage)
    {
        this.hiveHadoopImage = requireNonNull(hiveHadoopImage, "hiveHadoopImage is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-hive-insert-overwrite-" + randomTableSuffix();
        this.dockerizedS3DataLake = closeAfterClass(
                new HiveMinioDataLake(bucketName, ImmutableMap.of(), hiveHadoopImage));
        this.dockerizedS3DataLake.start();
        this.metastoreClient = new BridgingHiveMetastore(new ThriftHiveMetastore(
                new TestingMetastoreLocator(
                        Optional.empty(),
                        this.dockerizedS3DataLake.getHiveHadoop().getHiveMetastoreEndpoint()),
                new HiveConfig(),
                new MetastoreConfig(),
                new ThriftMetastoreConfig(),
                new HdfsEnvironment(new HiveHdfsConfiguration(
                        new HdfsConfigurationInitializer(
                                new HdfsConfig(),
                                ImmutableSet.of()),
                        ImmutableSet.of()),
                        new HdfsConfig(),
                        new NoHdfsAuthentication()),
                false));
        return S3HiveQueryRunner.create(
                dockerizedS3DataLake,
                ImmutableMap.<String, String>builder()
                        // This is required when using MinIO which requires path style access
                        .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        // Below are required to enable caching on metastore
                        .put("hive.metastore-cache-ttl", "1d")
                        .put("hive.metastore-refresh-interval", "1d")
                        .build());
    }

    @BeforeClass
    public void setUp()
    {
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA,
                bucketName));
    }

    @Test
    public void testInsertOverwriteInTransaction()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(testTable, "partitioned_by=ARRAY['regionkey']"));
        assertThatThrownBy(
                () -> newTransaction()
                        .execute(getSession(), session -> {
                            getQueryRunner().execute(session, createInsertStatement(testTable));
                        }))
                .hasMessage("Overwriting existing partition in non auto commit context doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwriteNonPartitionedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(testTable));
        assertInsertFailure(
                testTable,
                "Overwriting unpartitioned table not supported when writing directly to target directory");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwriteNonPartitionedBucketedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        assertInsertFailure(
                testTable,
                "Overwriting unpartitioned table not supported when writing directly to target directory");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwritePartitionedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedExternalTable()
    {
        String testTable = getTestTableName();
        // Store table data in data lake bucket
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        copyTpchNationToTable(testTable);

        // Map this table as external table
        String externalTableName = testTable + "_ext";
        computeActual(getCreateTableStatement(
                externalTableName,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3",
                format("external_location = 's3a://%s/%s/%s/'", this.bucketName, HIVE_TEST_SCHEMA, testTable)));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(externalTableName);
    }

    @Test
    public void testFlushPartitionCache()
    {
        String tableName = "nation_" + randomTableSuffix();
        String fullyQualifiedTestTableName = getTestTableName(tableName);
        String partitionColumn = "regionkey";

        // Create table with partition on regionkey
        computeActual(getCreateTableStatement(
                fullyQualifiedTestTableName,
                format("partitioned_by=ARRAY['%s']", partitionColumn)));
        copyTpchNationToTable(fullyQualifiedTestTableName);

        String queryUsingPartitionCacheTemplate = "SELECT name FROM %s WHERE %s=%s";
        String partitionValue1 = "0";
        String queryUsingPartitionCacheForValue1 = format(queryUsingPartitionCacheTemplate, fullyQualifiedTestTableName, partitionColumn, partitionValue1);
        String expectedQueryResultForValue1 = "VALUES 'ALGERIA', 'MOROCCO', 'MOZAMBIQUE', 'ETHIOPIA', 'KENYA'";
        String partitionValue2 = "1";
        String queryUsingPartitionCacheForValue2 = format(queryUsingPartitionCacheTemplate, fullyQualifiedTestTableName, partitionColumn, partitionValue2);
        String expectedQueryResultForValue2 = "VALUES 'ARGENTINA', 'BRAZIL', 'CANADA', 'PERU', 'UNITED STATES'";

        // Fill partition cache and check we got expected results
        assertQuery(queryUsingPartitionCacheForValue1, expectedQueryResultForValue1);
        assertQuery(queryUsingPartitionCacheForValue2, expectedQueryResultForValue2);

        // Copy partition to new location and update metadata outside Trino
        renamePartitionResourcesOutsideTrino(tableName, partitionColumn, partitionValue1);
        renamePartitionResourcesOutsideTrino(tableName, partitionColumn, partitionValue2);

        // Should return 0 rows as we moved partition and cache is outdated. We use nonexistent partition
        assertQueryReturnsEmptyResult(queryUsingPartitionCacheForValue1);
        assertQueryReturnsEmptyResult(queryUsingPartitionCacheForValue2);

        // Refresh cache for schema_name => 'dummy_schema', table_name => 'dummy_table', partition_column =>
        getQueryRunner().execute(format(
                "CALL system.flush_metadata_cache(schema_name => '%s', table_name => '%s', partition_column => ARRAY['%s'], partition_value => ARRAY['%s'])",
                HIVE_TEST_SCHEMA,
                tableName,
                partitionColumn,
                partitionValue1));

        // Should return expected rows as we refresh cache
        assertQuery(queryUsingPartitionCacheForValue1, expectedQueryResultForValue1);
        // Should return 0 rows as we left cache untouched
        assertQueryReturnsEmptyResult(queryUsingPartitionCacheForValue2);

        computeActual(format("DROP TABLE %s", fullyQualifiedTestTableName));
    }

    private void renamePartitionResourcesOutsideTrino(String tableName, String partitionColumn, String regionKey)
    {
        String partitionName = format("%s=%s", partitionColumn, regionKey);
        String partitionS3KeyPrefix = format("%s/%s/%s", HIVE_TEST_SCHEMA, tableName, partitionName);
        String renamedPartitionSuffix = "CP";

        // Copy whole partition to new location
        AmazonS3 amazonS3 = dockerizedS3DataLake.getS3Client();
        amazonS3.listObjects(bucketName)
                .getObjectSummaries()
                .forEach(object -> {
                    String objectKey = object.getKey();
                    if (objectKey.startsWith(partitionS3KeyPrefix)) {
                        String fileName = objectKey.substring(objectKey.lastIndexOf('/'));
                        String destinationKey = partitionS3KeyPrefix + renamedPartitionSuffix + fileName;
                        amazonS3.copyObject(bucketName, objectKey, bucketName, destinationKey);
                    }
                });

        // Delete old partition and update metadata to point to location of new copy
        HiveIdentity hiveIdentity = HiveIdentity.none();
        Table hiveTable = metastoreClient.getTable(hiveIdentity, HIVE_TEST_SCHEMA, tableName).get();
        Partition hivePartition = metastoreClient.getPartition(hiveIdentity, hiveTable, List.of(regionKey)).get();
        Map<String, PartitionStatistics> partitionStatistics =
                metastoreClient.getPartitionStatistics(hiveIdentity, hiveTable, List.of(hivePartition));

        metastoreClient.dropPartition(hiveIdentity, HIVE_TEST_SCHEMA, tableName, List.of(regionKey), true);
        metastoreClient.addPartitions(hiveIdentity, HIVE_TEST_SCHEMA, tableName, List.of(
                new PartitionWithStatistics(
                        Partition.builder(hivePartition)
                                .withStorage(builder -> builder.setLocation(
                                        hivePartition.getStorage().getLocation() + renamedPartitionSuffix))
                                .build(),
                        partitionName,
                        partitionStatistics.get(partitionName))));
    }

    protected void assertInsertFailure(String testTable, String expectedMessageRegExp)
    {
        assertInsertFailure(getSession(), testTable, expectedMessageRegExp);
    }

    protected void assertInsertFailure(Session session, String testTable, String expectedMessageRegExp)
    {
        assertQueryFails(
                session,
                createInsertStatement(testTable),
                expectedMessageRegExp);
    }

    private String createInsertStatement(String testTable)
    {
        return format("INSERT INTO %s " +
                        "SELECT name, comment, nationkey, regionkey " +
                        "FROM tpch.tiny.nation",
                testTable);
    }

    protected void assertOverwritePartition(String testTable)
    {
        computeActual(format(
                "INSERT INTO %s VALUES " +
                        "('POLAND', 'Test Data', 25, 5), " +
                        "('CZECH', 'Test Data', 26, 5)",
                testTable));
        query(format("SELECT name, comment, nationkey, regionkey FROM %s WHERE regionkey = 5", testTable))
                .assertThat()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row("POLAND", "Test Data", 25L, 5L)
                        .row("CZECH", "Test Data", 26L, 5L)
                        .build());

        computeActual(format("INSERT INTO %s values('POLAND', 'Overwrite', 25, 5)", testTable));
        query(format("SELECT name, comment, nationkey, regionkey FROM %s WHERE regionkey = 5", testTable))
                .assertThat()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row("POLAND", "Overwrite", 25L, 5L)
                        .build());
        computeActual(format("DROP TABLE %s", testTable));
    }

    protected String getTestTableName()
    {
        return getTestTableName("nation_" + randomTableSuffix());
    }

    protected String getTestTableName(String tableName)
    {
        return format("hive.%s.%s", HIVE_TEST_SCHEMA, tableName);
    }

    protected String getCreateTableStatement(String tableName, String... propertiesEntries)
    {
        return getCreateTableStatement(tableName, Arrays.asList(propertiesEntries));
    }

    protected String getCreateTableStatement(String tableName, List<String> propertiesEntries)
    {
        return format(
                "CREATE TABLE %s (" +
                        "    name varchar(25), " +
                        "    comment varchar(152),  " +
                        "    nationkey bigint, " +
                        "    regionkey bigint) " +
                        (propertiesEntries.size() < 1 ? "" : propertiesEntries
                                .stream()
                                .collect(joining(",", "WITH (", ")"))),
                tableName);
    }

    protected void copyTpchNationToTable(String testTable)
    {
        computeActual(format("INSERT INTO " + testTable + " SELECT name, comment, nationkey, regionkey FROM tpch.tiny.nation"));
    }
}
