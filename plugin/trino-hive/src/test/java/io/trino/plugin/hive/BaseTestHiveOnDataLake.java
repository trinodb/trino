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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.metastore.Column;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.minio.MinioClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
abstract class BaseTestHiveOnDataLake
        extends AbstractTestQueryFramework
{
    private static final String HIVE_TEST_SCHEMA = "hive_datalake";
    private static final DataSize HIVE_S3_STREAMING_PART_SIZE = DataSize.of(5, MEGABYTE);

    private final HiveMinioDataLake hiveMinioDataLake;
    private final String bucketName;

    private HiveMetastore metastoreClient;

    public BaseTestHiveOnDataLake(String bucketName, HiveMinioDataLake hiveMinioDataLake)
    {
        this.bucketName = bucketName;
        this.hiveMinioDataLake = hiveMinioDataLake;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake.start();
        this.metastoreClient = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveMetastoreEndpoint())
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

    @BeforeAll
    public void setUp()
    {
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA,
                bucketName));
        computeActual("CREATE SCHEMA hive.functions");
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        hiveMinioDataLake.close();
    }

    @Test
    public void testInsertOverwriteInTransaction()
    {
        String testTable = getFullyQualifiedTestTableName();
        computeActual(getCreateTableStatement(testTable, "partitioned_by=ARRAY['regionkey']"));
        assertThatThrownBy(
                () -> newTransaction()
                        .execute(getSession(), session -> {
                            getQueryRunner().execute(session, createInsertAsSelectFromTpchStatement(testTable));
                        }))
                .hasMessage("Overwriting existing partition in non auto commit context doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwriteNonPartitionedTable()
    {
        String testTable = getFullyQualifiedTestTableName();
        computeActual(getCreateTableStatement(testTable));
        assertInsertFailure(
                testTable,
                "Overwriting unpartitioned table not supported when writing directly to target directory");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwriteNonPartitionedBucketedTable()
    {
        String testTable = getFullyQualifiedTestTableName();
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
        String testTable = getFullyQualifiedTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedTable()
    {
        String testTable = getFullyQualifiedTestTableName();
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
        String testTable = getFullyQualifiedTestTableName();
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
    public void testSyncPartitionOnBucketRoot()
    {
        String tableName = "test_sync_partition_on_bucket_root_" + randomNameSuffix();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "hello\u0001world\nbye\u0001world".getBytes(UTF_8),
                "part_key=part_val/data.txt");

        assertUpdate("CREATE TABLE " + fullyQualifiedTestTableName + "(" +
                " a varchar," +
                " b varchar," +
                " part_key varchar)" +
                "WITH (" +
                " external_location='s3://" + bucketName + "/'," +
                " partitioned_by=ARRAY['part_key']," +
                " format='TEXTFILE'" +
                ")");

        getQueryRunner().execute("CALL system.sync_partition_metadata(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + tableName + "', mode => 'ADD')");

        assertQuery("SELECT * FROM " + fullyQualifiedTestTableName, "VALUES ('hello', 'world', 'part_val'), ('bye', 'world', 'part_val')");

        assertUpdate("DROP TABLE " + fullyQualifiedTestTableName);
    }

    @Test
    public void testSyncPartitionCaseSensitivePathVariation()
    {
        String tableName = "test_sync_partition_case_variation_" + randomNameSuffix();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);
        String tableLocation = format("s3://%s/%s/%s/", bucketName, HIVE_TEST_SCHEMA, tableName);

        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=part_val/data.txt");

        assertUpdate("CREATE TABLE " + fullyQualifiedTestTableName + "(" +
                " a varchar," +
                " b varchar," +
                " part_key varchar)" +
                "WITH (" +
                " external_location='" + tableLocation + "'," +
                " partitioned_by=ARRAY['part_key']," +
                " format='TEXTFILE'" +
                ")");

        getQueryRunner().execute("CALL system.sync_partition_metadata(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + tableName + "', mode => 'ADD')");
        assertQuery("SELECT * FROM " + fullyQualifiedTestTableName, "VALUES ('Trino', 'rocks', 'part_val')");

        // Move the data to a location where the partition path differs only in case
        hiveMinioDataLake.getMinioClient().removeObject(bucketName, HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=part_val/data.txt");
        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/PART_KEY=part_val/data.txt");

        getQueryRunner().execute("CALL system.sync_partition_metadata(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + tableName + "', mode => 'FULL', case_sensitive => false)");
        assertQuery("SELECT * FROM " + fullyQualifiedTestTableName, "VALUES ('Trino', 'rocks', 'part_val')");

        // Verify that syncing again the partition metadata has no negative effect (e.g. drop the partition)
        getQueryRunner().execute("CALL system.sync_partition_metadata(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + tableName + "', mode => 'FULL', case_sensitive => false)");
        assertQuery("SELECT * FROM " + fullyQualifiedTestTableName, "VALUES ('Trino', 'rocks', 'part_val')");

        assertUpdate("DROP TABLE " + fullyQualifiedTestTableName);
    }

    @Test
    public void testSyncPartitionSpecialCharacters()
    {
        String tableName = "test_sync_partition_special_characters_" + randomNameSuffix();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);
        String tableLocation = format("s3://%s/%s/%s/", bucketName, HIVE_TEST_SCHEMA, tableName);

        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks\u0001hyphens".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=with-hyphen/data.txt");
        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks\u0001dots".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=with.dot/data.txt");
        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks\u0001colons".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=with%3Acolon/data.txt");
        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks\u0001slashes".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=with%2Fslash/data.txt");
        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks\u0001backslashes".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=with%5Cbackslash/data.txt");
        hiveMinioDataLake.getMinioClient().putObject(
                bucketName,
                "Trino\u0001rocks\u0001percents".getBytes(UTF_8),
                HIVE_TEST_SCHEMA + "/" + tableName + "/part_key=with%25percent/data.txt");

        assertUpdate("CREATE TABLE " + fullyQualifiedTestTableName + "(" +
                " a varchar," +
                " b varchar," +
                " c varchar," +
                " part_key varchar)" +
                "WITH (" +
                " external_location='" + tableLocation + "'," +
                " partitioned_by=ARRAY['part_key']," +
                " format='TEXTFILE'" +
                ")");

        getQueryRunner().execute("CALL system.sync_partition_metadata(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + tableName + "', mode => 'ADD')");
        assertQuery(
                "SELECT * FROM " + fullyQualifiedTestTableName,
                """
                    VALUES
                            ('Trino', 'rocks', 'hyphens', 'with-hyphen'),
                            ('Trino', 'rocks', 'dots', 'with.dot'),
                            ('Trino', 'rocks', 'colons', 'with:colon'),
                            ('Trino', 'rocks', 'slashes', 'with/slash'),
                            ('Trino', 'rocks', 'backslashes', 'with\\backslash'),
                            ('Trino', 'rocks', 'percents', 'with%percent')
                    """);

        assertUpdate("DROP TABLE " + fullyQualifiedTestTableName);
    }

    @Test
    public void testFlushPartitionCache()
    {
        String tableName = "nation_" + randomNameSuffix();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);
        String partitionColumn = "regionkey";

        testFlushPartitionCache(
                tableName,
                fullyQualifiedTestTableName,
                partitionColumn,
                format(
                        "CALL system.flush_metadata_cache(schema_name => '%s', table_name => '%s', partition_columns => ARRAY['%s'], partition_values => ARRAY['0'])",
                        HIVE_TEST_SCHEMA,
                        tableName,
                        partitionColumn));
    }

    private void testFlushPartitionCache(String tableName, String fullyQualifiedTestTableName, String partitionColumn, String flushCacheProcedureSql)
    {
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

        // Refresh cache
        getQueryRunner().execute(flushCacheProcedureSql);

        // Should return expected rows as we refresh cache
        assertQuery(queryUsingPartitionCacheForValue1, expectedQueryResultForValue1);
        // Should return 0 rows as we left cache untouched
        assertQueryReturnsEmptyResult(queryUsingPartitionCacheForValue2);

        // Refresh cache for schema_name => 'dummy_schema', table_name => 'dummy_table'
        getQueryRunner().execute(format(
                "CALL system.flush_metadata_cache(schema_name => '%s', table_name => '%s')",
                HIVE_TEST_SCHEMA,
                tableName));

        // Should return expected rows for all partitions
        assertQuery(queryUsingPartitionCacheForValue1, expectedQueryResultForValue1);
        assertQuery(queryUsingPartitionCacheForValue2, expectedQueryResultForValue2);

        computeActual(format("DROP TABLE %s", fullyQualifiedTestTableName));
    }

    @Test
    public void testWriteDifferentSizes()
    {
        String testTable = getFullyQualifiedTestTableName();
        computeActual(format(
                "CREATE TABLE %s (" +
                        "    col1 varchar, " +
                        "    col2 varchar, " +
                        "    regionkey bigint) " +
                        "    WITH (partitioned_by=ARRAY['regionkey'])",
                testTable));

        long partSizeInBytes = HIVE_S3_STREAMING_PART_SIZE.toBytes();

        // Exercise different code paths of Hive S3 streaming upload, with upload part size 5MB:
        // 1. fileSize <= 5MB (direct upload)
        testWriteWithFileSize(testTable, 50, 0, partSizeInBytes);

        // 2. 5MB < fileSize <= 10MB (upload in two parts)
        testWriteWithFileSize(testTable, 100, partSizeInBytes + 1, partSizeInBytes * 2);

        // 3. fileSize > 10MB (upload in three or more parts)
        testWriteWithFileSize(testTable, 150, partSizeInBytes * 2 + 1, partSizeInBytes * 3);

        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testEnumPartitionProjectionOnVarcharColumnWithWhitespace()
    {
        String tableName = "nation_" + randomNameSuffix();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " (" +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  \"short name\" varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short name'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short name\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short name\\.values[ |]+PL1,CZ1[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL2'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ2'"))));

        assertQuery(
                format("SELECT * FROM %s", getFullyQualifiedTestTableName("\"" + tableName + "$partitions\"")),
                "VALUES 'PL1', 'CZ1'");

        assertQuery(
                format("SELECT name FROM %s WHERE \"short name\"='PL1'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_1'");

        // No results should be returned as Partition Projection will not project partitions for this value
        assertQueryReturnsEmptyResult(
                format("SELECT name FROM %s WHERE \"short name\"='PL2'", fullyQualifiedTestTableName));

        assertQuery(
                format("SELECT name FROM %s WHERE \"short name\"='PL1' OR \"short name\"='CZ1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('CZECH_1')");

        // Only POLAND_1 row will be returned as other value is outside of projection
        assertQuery(
                format("SELECT name FROM %s WHERE \"short name\"='PL1' OR \"short name\"='CZ2'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1')");

        // All values within projection range will be returned
        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('CZECH_1')");
    }

    @Test
    public void testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplateCreatedOnTrino()
    {
        // It's important to mix case here to detect if we properly handle rewriting
        // properties between Trino and Hive (e.g for Partition Projection)
        String schemaName = "Hive_Datalake_MixedCase";
        String tableName = getRandomTestTableName();

        // We create new schema to include mixed case location path and create such keys in Object Store
        computeActual("CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')".formatted(schemaName, bucketName));

        String storageFormat = format(
                "s3a://%s/%s/%s/short_name1=${short_name1}/short_name2=${short_name2}/",
                this.bucketName,
                schemaName,
                tableName);
        computeActual(
                "CREATE TABLE " + getFullyQualifiedTestTableName(schemaName, tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL2', 'CZ2'] " +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true, " +
                        "  partition_projection_location_template='" + storageFormat + "' " +
                        ")");
        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(schemaName, tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+storage\\.location\\.template[ |]+" + quote(storageFormat) + "[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.values[ |]+PL2,CZ2[ |]+");
        testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplate(schemaName, tableName);
    }

    @Test
    public void testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplateCreatedOnHive()
    {
        String tableName = getRandomTestTableName();
        String storageFormat = format(
                "'s3a://%s/%s/%s/short_name1=${short_name1}/short_name2=${short_name2}/'",
                this.bucketName,
                HIVE_TEST_SCHEMA,
                tableName);
        hiveMinioDataLake.runOnHive(
                "CREATE TABLE " + getHiveTestTableName(tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint " +
                        ") PARTITIONED BY (" +
                        "  short_name1 varchar(152), " +
                        "  short_name2 varchar(152)" +
                        ") " +
                        "TBLPROPERTIES ( " +
                        "  'projection.enabled'='true', " +
                        "  'storage.location.template'=" + storageFormat + ", " +
                        "  'projection.short_name1.type'='enum', " +
                        "  'projection.short_name1.values'='PL1,CZ1', " +
                        "  'projection.short_name2.type'='enum', " +
                        "  'projection.short_name2.values'='PL2,CZ2' " +
                        ")");
        testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplate(HIVE_TEST_SCHEMA, tableName);
    }

    private void testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplate(String schemaName, String tableName)
    {
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(schemaName, tableName);
        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'PL2'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'CZ2'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'PL2'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'CZ2'"))));

        assertQuery(
                format("SELECT * FROM %s", getFullyQualifiedTestTableName(schemaName, "\"" + tableName + "$partitions\"")),
                "VALUES ('PL1','PL2'), ('PL1','CZ2'), ('CZ1','PL2'), ('CZ1','CZ2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='CZ2'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testEnumPartitionProjectionOnVarcharColumn()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL2', 'CZ2']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.values[ |]+PL2,CZ2[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'PL2'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'CZ2'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'PL2'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'CZ2'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='CZ2'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2='CZ2' OR short_name2='PL2' )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlignCreatedOnTrino()
    {
        String tableName = getRandomTestTableName();
        computeActual(
                "CREATE TABLE " + getFullyQualifiedTestTableName(tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar(152) WITH (" +
                        "    partition_projection_type='integer', " +
                        "    partition_projection_range=ARRAY['1', '4'], " +
                        "    partition_projection_digits=3" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");
        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+integer[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+1,4[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.digits[ |]+3[ |]+");
        testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlign(tableName);
    }

    @Test
    public void testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlignCreatedOnHive()
    {
        String tableName = "nation_" + randomNameSuffix();
        hiveMinioDataLake.runOnHive(
                "CREATE TABLE " + getHiveTestTableName(tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint " +
                        ") " +
                        "PARTITIONED BY ( " +
                        "  short_name1 varchar(152), " +
                        "  short_name2 varchar(152)" +
                        ") " +
                        "TBLPROPERTIES " +
                        "( " +
                        "  'projection.enabled'='true', " +
                        "  'projection.short_name1.type'='enum', " +
                        "  'projection.short_name1.values'='PL1,CZ1', " +
                        "  'projection.short_name2.type'='integer', " +
                        "  'projection.short_name2.range'='1,4', " +
                        "  'projection.short_name2.digits'='3'" +
                        ")");
        testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlign(tableName);
    }

    private void testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlign(String tableName)
    {
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);
        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'001'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'002'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'003'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'004'"))));

        assertQuery(
                format("SELECT * FROM %s", getFullyQualifiedTestTableName("\"" + tableName + "$partitions\"")),
                "VALUES ('PL1','001'), ('PL1','002'), ('PL1','003'), ('PL1','004')," +
                        "('CZ1','001'), ('CZ1','002'), ('CZ1','003'), ('CZ1','004')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='002'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2='002' OR short_name2='001' )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testIntegerPartitionProjectionOnIntegerColumnWithInterval()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 integer WITH (" +
                        "    partition_projection_type='integer', " +
                        "    partition_projection_range=ARRAY['0', '10'], " +
                        "    partition_projection_interval=3" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+integer[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+0,10[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval[ |]+3[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "0"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "3"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "6"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "9"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2=3", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2=3 OR short_name2=0 )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testIntegerPartitionProjectionOnIntegerColumnWithDefaults()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 integer WITH (" +
                        "    partition_projection_type='integer', " +
                        "    partition_projection_range=ARRAY['1', '4']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+integer[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+1,4[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "1"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "2"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "3"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "4"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2=2", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2=2 OR short_name2=1 )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testDatePartitionProjectionOnDateColumnWithDefaults()
    {
        String tableName = "nation_" + randomNameSuffix();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 date WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd', " +
                        "    partition_projection_range=ARRAY['2001-1-22', '2001-1-25']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+date[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.format[ |]+yyyy-MM-dd[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+2001-1-22,2001-1-25[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "DATE '2001-1-22'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "DATE '2001-1-23'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "DATE '2001-1-24'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "DATE '2001-1-25'"),
                        ImmutableList.of("'CZECH_3'", "'Comment'", "4", "5", "'CZ1'", "DATE '2001-1-26'"))));

        assertQuery(
                format("SELECT * FROM %s", getFullyQualifiedTestTableName("\"" + tableName + "$partitions\"")),
                "VALUES ('PL1','2001-1-22'), ('PL1','2001-1-23'), ('PL1','2001-1-24'), ('PL1','2001-1-25')," +
                        "('CZ1','2001-1-22'), ('CZ1','2001-1-23'), ('CZ1','2001-1-24'), ('CZ1','2001-1-25')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2=(DATE '2001-1-23')", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2=(DATE '2001-1-23') OR short_name2=(DATE '2001-1-22') )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 > DATE '2001-1-23'", fullyQualifiedTestTableName),
                "VALUES ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 >= DATE '2001-1-23' AND short_name2 <= DATE '2001-1-25'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testDatePartitionProjectionOnTimestampColumnWithInterval()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 timestamp WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd HH:mm:ss', " +
                        "    partition_projection_range=ARRAY['2001-1-22 00:00:00', '2001-1-22 00:00:06'], " +
                        "    partition_projection_interval=2, " +
                        "    partition_projection_interval_unit='SECONDS'" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+date[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.format[ |]+yyyy-MM-dd HH:mm:ss[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+2001-1-22 00:00:00,2001-1-22 00:00:06[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval[ |]+2[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval\\.unit[ |]+seconds[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "TIMESTAMP '2001-1-22 00:00:00'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "TIMESTAMP '2001-1-22 00:00:02'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "TIMESTAMP '2001-1-22 00:00:04'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "TIMESTAMP '2001-1-22 00:00:06'"),
                        ImmutableList.of("'CZECH_3'", "'Comment'", "4", "5", "'CZ1'", "TIMESTAMP '2001-1-22 00:00:08'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2=(TIMESTAMP '2001-1-22 00:00:02')", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2=(TIMESTAMP '2001-1-22 00:00:00') OR short_name2=(TIMESTAMP '2001-1-22 00:00:02') )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 > TIMESTAMP '2001-1-22 00:00:02'", fullyQualifiedTestTableName),
                "VALUES ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 >= TIMESTAMP '2001-1-22 00:00:02' AND short_name2 <= TIMESTAMP '2001-1-22 00:00:06'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testDatePartitionProjectionOnTimestampColumnWithIntervalExpressionCreatedOnTrino()
    {
        String tableName = getRandomTestTableName();
        String dateProjectionFormat = "yyyy-MM-dd HH:mm:ss";
        computeActual(
                "CREATE TABLE " + getFullyQualifiedTestTableName(tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 timestamp WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='" + dateProjectionFormat + "', " +
                        // We set range to -5 minutes to NOW in order to be sure it will grab all test dates
                        // which range is -4 minutes till now. Also, we have to consider max no. of partitions 1k
                        "    partition_projection_range=ARRAY['NOW-5MINUTES', 'NOW'], " +
                        "    partition_projection_interval=1, " +
                        "    partition_projection_interval_unit='SECONDS'" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");
        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.format[ |]+" + quote(dateProjectionFormat) + "[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+NOW-5MINUTES,NOW[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval[ |]+1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval\\.unit[ |]+seconds[ |]+");
        testDatePartitionProjectionOnTimestampColumnWithIntervalExpression(tableName, dateProjectionFormat);
    }

    @Test
    public void testDatePartitionProjectionOnTimestampColumnWithIntervalExpressionCreatedOnHive()
    {
        String tableName = getRandomTestTableName();
        String dateProjectionFormat = "yyyy-MM-dd HH:mm:ss";
        hiveMinioDataLake.runOnHive(
                "CREATE TABLE " + getHiveTestTableName(tableName) + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint " +
                        ") " +
                        "PARTITIONED BY (" +
                        "  short_name1 varchar(152), " +
                        "  short_name2 timestamp " +
                        ") " +
                        "TBLPROPERTIES ( " +
                        "  'projection.enabled'='true', " +
                        "  'projection.short_name1.type'='enum', " +
                        "  'projection.short_name1.values'='PL1,CZ1', " +
                        "  'projection.short_name2.type'='date', " +
                        "  'projection.short_name2.format'='" + dateProjectionFormat + "', " +
                        // We set range to -5 minutes to NOW in order to be sure it will grab all test dates
                        // which range is -4 minutes till now. Also, we have to consider max no. of partitions 1k
                        "  'projection.short_name2.range'='NOW-5MINUTES,NOW', " +
                        "  'projection.short_name2.interval'='1', " +
                        "  'projection.short_name2.interval.unit'='SECONDS'" +
                        ")");
        testDatePartitionProjectionOnTimestampColumnWithIntervalExpression(tableName, dateProjectionFormat);
    }

    private void testDatePartitionProjectionOnTimestampColumnWithIntervalExpression(String tableName, String dateProjectionFormat)
    {
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        Instant dayToday = Instant.now();
        DateFormat dateFormat = new SimpleDateFormat(dateProjectionFormat);
        dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        String minutesNowFormatted = moveDate(dateFormat, dayToday, MINUTES, 0);
        String minutes1AgoFormatter = moveDate(dateFormat, dayToday, MINUTES, -1);
        String minutes2AgoFormatted = moveDate(dateFormat, dayToday, MINUTES, -2);
        String minutes3AgoFormatted = moveDate(dateFormat, dayToday, MINUTES, -3);
        String minutes4AgoFormatted = moveDate(dateFormat, dayToday, MINUTES, -4);

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "TIMESTAMP '" + minutesNowFormatted + "'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "TIMESTAMP '" + minutes1AgoFormatter + "'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "TIMESTAMP '" + minutes2AgoFormatted + "'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "TIMESTAMP '" + minutes3AgoFormatted + "'"),
                        ImmutableList.of("'CZECH_3'", "'Comment'", "4", "5", "'CZ1'", "TIMESTAMP '" + minutes4AgoFormatted + "'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 > ( TIMESTAMP '%s' ) AND short_name2 <= ( TIMESTAMP '%s' )", fullyQualifiedTestTableName, minutes4AgoFormatted, minutes1AgoFormatter),
                "VALUES ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testDatePartitionProjectionOnVarcharColumnWithHoursInterval()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd HH', " +
                        "    partition_projection_range=ARRAY['2001-01-22 00', '2001-01-22 06'], " +
                        "    partition_projection_interval=2, " +
                        "    partition_projection_interval_unit='HOURS'" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+date[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.format[ |]+yyyy-MM-dd HH[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+2001-01-22 00,2001-01-22 06[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval[ |]+2[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval\\.unit[ |]+hours[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'2001-01-22 00'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'2001-01-22 02'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'2001-01-22 04'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'2001-01-22 06'"),
                        ImmutableList.of("'CZECH_3'", "'Comment'", "4", "5", "'CZ1'", "'2001-01-22 08'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='2001-01-22 02'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2='2001-01-22 00' OR short_name2='2001-01-22 02' )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 > '2001-01-22 02'", fullyQualifiedTestTableName),
                "VALUES ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 >= '2001-01-22 02' AND short_name2 <= '2001-01-22 06'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testDatePartitionProjectionOnVarcharColumnWithDaysInterval()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd', " +
                        "    partition_projection_range=ARRAY['2001-01-01', '2001-01-07'], " +
                        "    partition_projection_interval=2, " +
                        "    partition_projection_interval_unit='DAYS'" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+date[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.format[ |]+yyyy-MM-dd[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+2001-01-01,2001-01-07[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval[ |]+2[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.interval\\.unit[ |]+days[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'2001-01-01'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'2001-01-03'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'2001-01-05'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'2001-01-07'"),
                        ImmutableList.of("'CZECH_3'", "'Comment'", "4", "5", "'CZ1'", "'2001-01-09'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='2001-01-03'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2='2001-01-01' OR short_name2='2001-01-03' )", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 > '2001-01-03'", fullyQualifiedTestTableName),
                "VALUES ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 >= '2001-01-03' AND short_name2 <= '2001-01-07'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    @Test
    public void testDatePartitionProjectionOnVarcharColumnWithIntervalExpression()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);
        String dateProjectionFormat = "yyyy-MM-dd";

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='" + dateProjectionFormat + "', " +
                        "    partition_projection_range=ARRAY['NOW-3DAYS', 'NOW']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+date[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.format[ |]+" + quote(dateProjectionFormat) + "[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.range[ |]+NOW-3DAYS,NOW[ |]+");

        Instant dayToday = Instant.now();
        DateFormat dateFormat = new SimpleDateFormat(dateProjectionFormat);
        dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        String dayTodayFormatted = moveDate(dateFormat, dayToday, DAYS, 0);
        String day1AgoFormatter = moveDate(dateFormat, dayToday, DAYS, -1);
        String day2AgoFormatted = moveDate(dateFormat, dayToday, DAYS, -2);
        String day3AgoFormatted = moveDate(dateFormat, dayToday, DAYS, -3);
        String day4AgoFormatted = moveDate(dateFormat, dayToday, DAYS, -4);

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'" + dayTodayFormatted + "'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'" + day1AgoFormatter + "'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'" + day2AgoFormatted + "'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'" + day3AgoFormatted + "'"),
                        ImmutableList.of("'CZECH_3'", "'Comment'", "4", "5", "'CZ1'", "'" + day4AgoFormatted + "'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='%s'", fullyQualifiedTestTableName, day1AgoFormatter),
                "VALUES 'POLAND_2'");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2='%s' OR short_name2='%s' )", fullyQualifiedTestTableName, dayTodayFormatted, day1AgoFormatter),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 > '%s'", fullyQualifiedTestTableName, day2AgoFormatted),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name2 >= '%s' AND short_name2 <= '%s'", fullyQualifiedTestTableName, day4AgoFormatted, day1AgoFormatter),
                "VALUES ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2')");

        assertQuery(
                format("SELECT name FROM %s", fullyQualifiedTestTableName),
                "VALUES ('POLAND_1'), ('POLAND_2'), ('CZECH_1'), ('CZECH_2')");
    }

    private String moveDate(DateFormat format, Instant today, TemporalUnit unit, int move)
    {
        return format.format(new Date(today.plus(move, unit).toEpochMilli()));
    }

    @Test
    public void testDatePartitionProjectionFormatTextWillNotCauseIntervalRequirement()
    {
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName();

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='''start''yyyy-MM-dd''end''''s''', " +
                        "    partition_projection_range=ARRAY['start2001-01-01end''s', 'start2001-01-07end''s'] " +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")");
    }

    @Test
    public void testInjectedPartitionProjectionOnVarcharColumn()
    {
        String tableName = getRandomTestTableName();
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        computeActual(
                "CREATE TABLE " + fullyQualifiedTestTableName + " ( " +
                        "  name varchar(25), " +
                        "  comment varchar(152), " +
                        "  nationkey bigint, " +
                        "  regionkey bigint, " +
                        "  short_name1 varchar(152) WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "   ), " +
                        "  short_name2 varchar(152) WITH (" +
                        "    partition_projection_type='injected'" +
                        "   ) " +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")");

        assertThat(
                hiveMinioDataLake
                        .runOnHive("SHOW TBLPROPERTIES " + getHiveTestTableName(tableName)))
                .containsPattern("[ |]+projection\\.enabled[ |]+true[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.type[ |]+enum[ |]+")
                .containsPattern("[ |]+projection\\.short_name1\\.values[ |]+PL1,CZ1[ |]+")
                .containsPattern("[ |]+projection\\.short_name2\\.type[ |]+injected[ |]+");

        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'Comment'", "0", "5", "'PL1'", "'001'"),
                        ImmutableList.of("'POLAND_2'", "'Comment'", "1", "5", "'PL1'", "'002'"),
                        ImmutableList.of("'CZECH_1'", "'Comment'", "2", "5", "'CZ1'", "'003'"),
                        ImmutableList.of("'CZECH_2'", "'Comment'", "3", "5", "'CZ1'", "'004'"))));

        assertQuery(
                format("SELECT name FROM %s WHERE short_name1='PL1' AND short_name2='002'", fullyQualifiedTestTableName),
                "VALUES 'POLAND_2'");

        assertThatThrownBy(
                () -> getQueryRunner().execute(
                        format("SELECT name FROM %s WHERE short_name1='PL1' AND ( short_name2='002' OR short_name2='001' )", fullyQualifiedTestTableName)))
                .hasMessage("Column projection for column 'short_name2' failed. Injected projection requires single predicate for it's column in where clause. Currently provided can't be converted to single partition.");

        assertThatThrownBy(
                () -> getQueryRunner().execute(
                        format("SELECT name FROM %s", fullyQualifiedTestTableName)))
                .hasMessage("Column projection for column 'short_name2' failed. Injected projection requires single predicate for it's column in where clause");

        assertThatThrownBy(
                () -> getQueryRunner().execute(
                        format("SELECT name FROM %s WHERE short_name1='PL1'", fullyQualifiedTestTableName)))
                .hasMessage("Column projection for column 'short_name2' failed. Injected projection requires single predicate for it's column in where clause");
    }

    @Test
    public void testPartitionProjectionInvalidTableProperties()
    {
        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar " +
                        ") WITH ( " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Partition projection cannot be enabled on a table that is not partitioned");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar WITH ( " +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1']" +
                        "  ), " +
                        "  short_name1 varchar " +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Partition projection cannot be defined for non-partition column: 'name'");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH ( " +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1']" +
                        "  ), " +
                        "  short_name2 varchar " +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name2' failed. Projection type property missing");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  ), " +
                        "  short_name2 varchar WITH (" +
                        "    partition_projection_type='injected' " +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true, " +
                        "  partition_projection_location_template='s3a://dummy/short_name1=${short_name1}/'" +
                        ")"))
                .hasMessage("Partition projection location template: s3a://dummy/short_name1=${short_name1}/ " +
                        "is missing partition column: 'short_name2' placeholder");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='integer', " +
                        "    partition_projection_range=ARRAY['1', '2', '3']" +
                        "   ), " +
                        "  short_name2 varchar WITH (" +
                        "    partition_projection_type='enum', " +
                        "    partition_projection_values=ARRAY['PL1', 'CZ1'] " +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1', 'short_name2'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_range' needs to be list of 2 integers");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_values=ARRAY['2001-01-01', '2001-01-02']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Missing required property: 'partition_projection_format'");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd HH', " +
                        "    partition_projection_range=ARRAY['2001-01-01', '2001-01-02']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_range' needs to be a list of 2 valid dates formatted as 'yyyy-MM-dd HH' " +
                        "or '^\\s*NOW\\s*(([+-])\\s*([0-9]+)\\s*(DAY|HOUR|MINUTE|SECOND)S?\\s*)?$' that are sequential: Unparseable date: \"2001-01-01\"");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd', " +
                        "    partition_projection_range=ARRAY['NOW*3DAYS', '2001-01-02']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_range' needs to be a list of 2 valid dates formatted as 'yyyy-MM-dd' " +
                        "or '^\\s*NOW\\s*(([+-])\\s*([0-9]+)\\s*(DAY|HOUR|MINUTE|SECOND)S?\\s*)?$' that are sequential: Unparseable date: \"NOW*3DAYS\"");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd', " +
                        "    partition_projection_range=ARRAY['2001-01-02', '2001-01-01']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_range' needs to be a list of 2 valid dates formatted as 'yyyy-MM-dd' " +
                        "or '^\\s*NOW\\s*(([+-])\\s*([0-9]+)\\s*(DAY|HOUR|MINUTE|SECOND)S?\\s*)?$' that are sequential");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd', " +
                        "    partition_projection_range=ARRAY['2001-01-01', '2001-01-02'], " +
                        "    partition_projection_interval_unit='Decades'" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_interval_unit' value 'Decades' is invalid. " +
                        "Available options: [Days, Hours, Minutes, Seconds]");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd HH', " +
                        "    partition_projection_range=ARRAY['2001-01-01 10', '2001-01-02 10']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true " +
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_interval_unit' " +
                        "needs to be set when provided 'partition_projection_format' is less that single-day precision. " +
                        "Interval defaults to 1 day or 1 month, respectively. Otherwise, interval is required");

        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd', " +
                        "    partition_projection_range=ARRAY['2001-01-01 10', '2001-01-02 10']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'] " +
                        ")"))
                .hasMessage("Columns partition projection properties cannot be set when 'partition_projection_enabled' is not set");

        // Verify that ignored flag is only interpreted for pre-existing tables where configuration is loaded from metastore.
        // It should not allow creating corrupted config via Trino. It's a kill switch to run away when we have compatibility issues.
        assertThatThrownBy(() -> getQueryRunner().execute(
                "CREATE TABLE " + getFullyQualifiedTestTableName("nation_" + randomNameSuffix()) + " ( " +
                        "  name varchar, " +
                        "  short_name1 varchar WITH (" +
                        "    partition_projection_type='date', " +
                        "    partition_projection_format='yyyy-MM-dd HH', " +
                        "    partition_projection_range=ARRAY['2001-01-01 10', '2001-01-02 10']" +
                        "  )" +
                        ") WITH ( " +
                        "  partitioned_by=ARRAY['short_name1'], " +
                        "  partition_projection_enabled=true, " +
                        "  partition_projection_ignore=true " + // <-- Even if this is set we disallow creating corrupted configuration via Trino
                        ")"))
                .hasMessage("Column projection for column 'short_name1' failed. Property: 'partition_projection_interval_unit' " +
                        "needs to be set when provided 'partition_projection_format' is less that single-day precision. " +
                        "Interval defaults to 1 day or 1 month, respectively. Otherwise, interval is required");
    }

    @Test
    public void testPartitionProjectionIgnore()
    {
        String tableName = "nation_" + randomNameSuffix();
        String hiveTestTableName = getHiveTestTableName(tableName);
        String fullyQualifiedTestTableName = getFullyQualifiedTestTableName(tableName);

        // Create corrupted configuration
        hiveMinioDataLake.runOnHive(
                "CREATE TABLE " + hiveTestTableName + " ( " +
                        "  name varchar(25) " +
                        ") PARTITIONED BY (" +
                        "  date_time varchar(152) " +
                        ") " +
                        "TBLPROPERTIES ( " +
                        "  'projection.enabled'='true', " +
                        "  'projection.date_time.type'='date', " +
                        "  'projection.date_time.format'='yyyy-MM-dd HH', " +
                        "  'projection.date_time.range'='2001-01-01,2001-01-02' " +
                        ")");

        // Expect invalid Partition Projection properties to fail
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM " + fullyQualifiedTestTableName))
                .hasMessage("Column projection for column 'date_time' failed. Property: 'partition_projection_range' needs to be a list of 2 valid dates formatted as 'yyyy-MM-dd HH' " +
                        "or '^\\s*NOW\\s*(([+-])\\s*([0-9]+)\\s*(DAY|HOUR|MINUTE|SECOND)S?\\s*)?$' that are sequential: Unparseable date: \"2001-01-01\"");

        // Append kill switch table property to ignore Partition Projection properties
        hiveMinioDataLake.runOnHive(
                "ALTER TABLE " + hiveTestTableName + " SET TBLPROPERTIES ( 'trino.partition_projection.ignore'='TRUE' )");
        // Flush cache to get new definition
        computeActual("CALL system.flush_metadata_cache(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + tableName + "')");

        // Verify query execution works
        computeActual(createInsertStatement(
                fullyQualifiedTestTableName,
                ImmutableList.of(
                        ImmutableList.of("'POLAND_1'", "'2022-02-01 12'"),
                        ImmutableList.of("'POLAND_2'", "'2022-02-01 12'"),
                        ImmutableList.of("'CZECH_1'", "'2022-02-01 13'"),
                        ImmutableList.of("'CZECH_2'", "'2022-02-01 13'"))));

        assertQuery("SELECT * FROM " + fullyQualifiedTestTableName,
                "VALUES ('POLAND_1', '2022-02-01 12'), " +
                        "('POLAND_2', '2022-02-01 12'), " +
                        "('CZECH_1', '2022-02-01 13'), " +
                        "('CZECH_2', '2022-02-01 13')");
        assertQuery("SELECT * FROM " + fullyQualifiedTestTableName + " WHERE date_time = '2022-02-01 12'",
                "VALUES ('POLAND_1', '2022-02-01 12'), ('POLAND_2', '2022-02-01 12')");
    }

    @Test
    public void testAnalyzePartitionedTableWithCanonicalization()
    {
        String tableName = "test_analyze_table_canonicalization_" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s (a_varchar varchar, month varchar) WITH (partitioned_by = ARRAY['month'])".formatted(getFullyQualifiedTestTableName(tableName)));

        assertUpdate("INSERT INTO " + getFullyQualifiedTestTableName(tableName) + " VALUES ('A', '01'), ('B', '01'), ('C', '02'), ('D', '03')", 4);

        String tableLocation = (String) computeActual("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM " + getFullyQualifiedTestTableName(tableName)).getOnlyValue();

        String externalTableName = "external_" + tableName;
        List<String> partitionColumnNames = List.of("month");
        assertUpdate(
                """
                CREATE TABLE %s(
                  a_varchar varchar,
                  month integer)
                WITH (
                   partitioned_by = ARRAY['month'],
                   external_location='%s')
                """.formatted(getFullyQualifiedTestTableName(externalTableName), tableLocation));

        addPartitions(tableName, externalTableName, partitionColumnNames, TupleDomain.all());
        assertQuery("SELECT * FROM " + HIVE_TEST_SCHEMA + ".\"" + externalTableName + "$partitions\"", "VALUES 1, 2, 3");
        assertUpdate("ANALYZE " + getFullyQualifiedTestTableName(externalTableName), 4);
        assertQuery("SHOW STATS FOR " + getFullyQualifiedTestTableName(externalTableName),
                """
                        VALUES
                            ('a_varchar', 4.0, 2.0, 0.0, null, null, null),
                            ('month', null, 3.0, 0.0, null, 1, 3),
                            (null, null, null, null, 4.0, null, null)
                        """);

        assertUpdate("INSERT INTO " + getFullyQualifiedTestTableName(tableName) + " VALUES ('E', '04')", 1);
        addPartitions(
                tableName,
                externalTableName,
                partitionColumnNames,
                TupleDomain.fromFixedValues(Map.of("month", new NullableValue(VARCHAR, utf8Slice("04")))));
        assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + HIVE_TEST_SCHEMA + "', table_name => '" + externalTableName + "')");
        assertQuery("SELECT * FROM " + HIVE_TEST_SCHEMA + ".\"" + externalTableName + "$partitions\"", "VALUES 1, 2, 3, 4");
        assertUpdate("ANALYZE " + getFullyQualifiedTestTableName(externalTableName) + " WITH (partitions = ARRAY[ARRAY['04']])", 1);
        assertQuery("SHOW STATS FOR " + getFullyQualifiedTestTableName(externalTableName),
                """
                        VALUES
                            ('a_varchar', 5.0, 2.0, 0.0, null, null, null),
                            ('month', null, 4.0, 0.0, null, 1, 4),
                            (null, null, null, null, 5.0, null, null)
                        """);
        // TODO (https://github.com/trinodb/trino/issues/15998) fix selective ANALYZE for table with non-canonical partition values
        assertQueryFails("ANALYZE " + getFullyQualifiedTestTableName(externalTableName) + " WITH (partitions = ARRAY[ARRAY['4']])", ".*Partition.*not found.*");

        assertUpdate("DROP TABLE " + getFullyQualifiedTestTableName(externalTableName));
        assertUpdate("DROP TABLE " + getFullyQualifiedTestTableName(tableName));
    }

    @Test
    public void testExternalLocationWithTrailingSpace()
    {
        String tableName = "test_external_location_with_trailing_space_" + randomNameSuffix();
        String tableLocationDirWithTrailingSpace = tableName + " ";
        String tableLocation = format("s3a://%s/%s/%s", bucketName, HIVE_TEST_SCHEMA, tableLocationDirWithTrailingSpace);

        byte[] contents = "hello\u0001world\nbye\u0001world".getBytes(UTF_8);
        String targetPath = format("%s/%s/test.txt", HIVE_TEST_SCHEMA, tableLocationDirWithTrailingSpace);
        hiveMinioDataLake.getMinioClient().putObject(bucketName, contents, targetPath);

        assertUpdate(format(
                "CREATE TABLE %s (" +
                        "  a varchar, " +
                        "  b varchar) " +
                        "WITH (format='TEXTFILE', external_location='%s')",
                tableName,
                tableLocation));

        assertQuery("SELECT a, b FROM " + tableName, "VALUES ('hello', 'world'), ('bye', 'world')");

        String actualTableLocation = getTableLocation(tableName);
        assertThat(actualTableLocation).isEqualTo(tableLocation);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateSchemaInvalidName()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA \".\""))
                .hasMessage("Invalid object name: '.'");

        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA \"..\""))
                .hasMessage("Invalid object name: '..'");

        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA \"foo/bar\""))
                .hasMessage("Invalid object name: 'foo/bar'");
    }

    @Test
    public void testCreateTableInvalidName()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + HIVE_TEST_SCHEMA + ".\".\" (col integer)"))
                .hasMessageContaining("Invalid table name");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + HIVE_TEST_SCHEMA + ".\"..\" (col integer)"))
                .hasMessageContaining("Invalid table name");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + HIVE_TEST_SCHEMA + ".\"...\" (col integer)"))
                .hasMessage("Invalid table name");

        for (String tableName : Arrays.asList("foo/bar", "foo/./bar", "foo/../bar")) {
            assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + HIVE_TEST_SCHEMA + ".\"" + tableName + "\" (col integer)"))
                    .hasMessage(format("Invalid object name: '%s'", tableName));
            assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + HIVE_TEST_SCHEMA + ".\"" + tableName + "\" (col) AS VALUES 1"))
                    .hasMessage(format("Invalid object name: '%s'", tableName));
        }
    }

    @Test
    public void testRenameSchemaToInvalidObjectName()
    {
        String schemaName = "test_rename_schema_invalid_name_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA %1$s WITH (location='s3a://%2$s/%1$s')".formatted(schemaName, bucketName));

        for (String invalidSchemaName : Arrays.asList(".", "..", "foo/bar")) {
            assertThatThrownBy(() -> assertUpdate("ALTER SCHEMA hive." + schemaName + " RENAME TO  \"" + invalidSchemaName + "\""))
                    .hasMessage(format("Invalid object name: '%s'", invalidSchemaName));
        }

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testRenameTableToInvalidObjectName()
    {
        String tableName = "test_rename_table_invalid_name_" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s (a_varchar varchar)".formatted(getFullyQualifiedTestTableName(tableName)));

        for (String invalidTableName : Arrays.asList(".", "..", "foo/bar")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + getFullyQualifiedTestTableName(tableName) + " RENAME TO  \"" + invalidTableName + "\""))
                    .hasMessage(format("Invalid object name: '%s'", invalidTableName));
        }

        for (String invalidSchemaName : Arrays.asList(".", "..", "foo/bar")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + getFullyQualifiedTestTableName(tableName) + " RENAME TO  \"" + invalidSchemaName + "\".validTableName"))
                    .hasMessage(format("Invalid object name: '%s'", invalidSchemaName));
        }

        assertUpdate("DROP TABLE " + getFullyQualifiedTestTableName(tableName));
    }

    @Test
    public void testUnpartitionedTableExternalLocationWithTrainingSlash()
    {
        String tableName = "test_external_location_trailing_slash_" + randomNameSuffix();
        String tableLocationWithTrailingSlash = format("s3://%s/%s/%s/", bucketName, HIVE_TEST_SCHEMA, tableName);
        byte[] contents = "Trino\nSQL\non\neverything".getBytes(UTF_8);
        String dataFilePath = format("%s/%s/data.txt", HIVE_TEST_SCHEMA, tableName);
        hiveMinioDataLake.getMinioClient().putObject(bucketName, contents, dataFilePath);

        assertUpdate(format(
                "CREATE TABLE %s (" +
                        "  a_varchar varchar) " +
                        "WITH (" +
                        "   external_location='%s'," +
                        "   format='TEXTFILE')",
                tableName,
                tableLocationWithTrailingSlash));
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'SQL', 'on', 'everything'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnpartitionedTableExternalLocationOnTopOfTheBucket()
    {
        String topBucketName = "test-hive-unpartitioned-top-of-the-bucket-" + randomNameSuffix();
        hiveMinioDataLake.getMinio().createBucket(topBucketName);
        String tableName = "test_external_location_top_of_the_bucket_" + randomNameSuffix();

        byte[] contents = "Trino\nSQL\non\neverything".getBytes(UTF_8);
        hiveMinioDataLake.getMinioClient().putObject(topBucketName, contents, "data.txt");

        assertUpdate(format(
                "CREATE TABLE %s (" +
                        "  a_varchar varchar) " +
                        "WITH (" +
                        "   external_location='%s'," +
                        "   format='TEXTFILE')",
                tableName,
                format("s3://%s/", topBucketName)));
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'SQL', 'on', 'everything'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionedTableExternalLocationOnTopOfTheBucket()
    {
        String topBucketName = "test-hive-partitioned-top-of-the-bucket-" + randomNameSuffix();
        hiveMinioDataLake.getMinio().createBucket(topBucketName);
        String tableName = "test_external_location_top_of_the_bucket_" + randomNameSuffix();

        assertUpdate(format(
                "CREATE TABLE %s (" +
                        "  a_varchar varchar, " +
                        "  pkey integer) " +
                        "WITH (" +
                        "   external_location='%s'," +
                        "   partitioned_by=ARRAY['pkey'])",
                tableName,
                format("s3://%s/", topBucketName)));
        assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 1) , ('b', 1), ('c', 2), ('d', 2)", 4);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('a', 1), ('b',1), ('c', 2), ('d', 2)");
        assertUpdate("DELETE FROM " + tableName + " where pkey = 2");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('a', 1), ('b',1)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsPartitionedTable()
    {
        String tableName = "test_hive_drop_stats_partitioned_table_" + randomNameSuffix();
        assertUpdate(("CREATE TABLE %s (" +
                "  data integer," +
                "  p_varchar varchar," +
                "  p_integer integer" +
                ") " +
                "WITH (" +
                "  partitioned_by=ARRAY['p_varchar', 'p_integer']" +
                ")").formatted(getFullyQualifiedTestTableName(tableName)));

        // Drop stats for partition which does not exist
        assertThat(query(format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['partnotfound', '999']])", HIVE_TEST_SCHEMA, tableName)))
                .failure().hasMessage("No partition found for name: p_varchar=partnotfound/p_integer=999");

        assertUpdate("INSERT INTO " + getFullyQualifiedTestTableName(tableName) + " VALUES (1, 'part1', 10) , (2, 'part2', 10), (12, 'part2', 20)", 3);

        // Run analyze on the entire table
        assertUpdate("ANALYZE " + getFullyQualifiedTestTableName(tableName), 3);

        assertQuery("SHOW STATS FOR " + getFullyQualifiedTestTableName(tableName),
                """
                        VALUES
                            ('data', null, 1.0, 0.0, null, 1, 12),
                            ('p_varchar', 15.0, 2.0, 0.0, null, null, null),
                            ('p_integer', null, 2.0, 0.0, null, 10, 20),
                            (null, null, null, null, 3.0, null, null)
                        """);

        assertUpdate(format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['part1', '10']])", HIVE_TEST_SCHEMA, tableName));

        assertQuery("SHOW STATS FOR " + getFullyQualifiedTestTableName(tableName),
                """
                        VALUES
                            ('data', null, 1.0, 0.0, null, 2, 12),
                            ('p_varchar', 15.0, 2.0, 0.0, null, null, null),
                            ('p_integer', null, 2.0, 0.0, null, 10, 20),
                            (null, null, null, null, 3.0, null, null)
                        """);

        assertUpdate("DELETE FROM " + getFullyQualifiedTestTableName(tableName) + " WHERE p_varchar ='part1' and p_integer = 10");

        // Drop stats for partition which does not exist
        assertThat(query(format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['part1', '10']])", HIVE_TEST_SCHEMA, tableName)))
                .failure().hasMessage("No partition found for name: p_varchar=part1/p_integer=10");

        assertQuery("SHOW STATS FOR " + getFullyQualifiedTestTableName(tableName),
                """
                        VALUES
                            ('data', null, 1.0, 0.0, null, 2, 12),
                            ('p_varchar', 10.0, 1.0, 0.0, null, null, null),
                            ('p_integer', null, 2.0, 0.0, null, 10, 20),
                            (null, null, null, null, 2.0, null, null)
                        """);
        assertUpdate("DROP TABLE " + getFullyQualifiedTestTableName(tableName));
    }

    @Test
    public void testUnsupportedDropSchemaCascadeWithNonHiveTable()
    {
        String schemaName = "test_unsupported_drop_schema_cascade_" + randomNameSuffix();
        String icebergTableName = "test_dummy_iceberg_table" + randomNameSuffix();

        hiveMinioDataLake.runOnHive("CREATE DATABASE %2$s LOCATION 's3a://%1$s/%2$s'".formatted(bucketName, schemaName));
        try {
            hiveMinioDataLake.runOnHive("CREATE TABLE " + schemaName + "." + icebergTableName + " TBLPROPERTIES ('table_type'='iceberg') AS SELECT 1 a");

            assertQueryFails("DROP SCHEMA " + schemaName + " CASCADE", "\\QCannot query Iceberg table '%s.%s'".formatted(schemaName, icebergTableName));

            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
            assertThat(computeActual("SHOW TABLES FROM " + schemaName).getOnlyColumnAsSet()).contains(icebergTableName);
            assertThat(hiveMinioDataLake.getMinioClient().listObjects(bucketName, schemaName).stream()).isNotEmpty();
        }
        finally {
            hiveMinioDataLake.runOnHive("DROP DATABASE IF EXISTS " + schemaName + " CASCADE");
        }
    }

    @Test
    public void testUnsupportedCommentOnHiveView()
    {
        String viewName = HIVE_TEST_SCHEMA + ".test_unsupported_comment_on_hive_view_" + randomNameSuffix();

        hiveMinioDataLake.runOnHive("CREATE VIEW " + viewName + " AS SELECT 1 x");
        try {
            assertQueryFails("COMMENT ON COLUMN " + viewName + ".x IS NULL", "Hive views are not supported.*");
        }
        finally {
            hiveMinioDataLake.runOnHive("DROP VIEW " + viewName);
        }
    }

    @Test
    public void testCreateFunction()
    {
        String name = "test_" + randomNameSuffix();
        String name2 = "test_" + randomNameSuffix();

        assertUpdate("CREATE FUNCTION " + name + "(x integer) RETURNS bigint RETURN x * 10");
        assertQuery("SELECT " + name + "(99)", "SELECT 990");

        assertUpdate("CREATE OR REPLACE FUNCTION " + name + "(x integer) RETURNS bigint COMMENT 't42' RETURN x * 42");
        assertQuery("SELECT " + name + "(99)", "SELECT 4158");

        assertQueryFails("SELECT " + name + "(2.9)", ".*Unexpected parameters.*");

        assertUpdate("CREATE FUNCTION " + name + "(x double) RETURNS double COMMENT 't88' RETURN x * 8.8");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .build());

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");

        assertQueryFails("CREATE FUNCTION " + name + "(x int) RETURNS bigint RETURN x", "line 1:1: Function already exists");

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");

        assertUpdate("CREATE OR REPLACE FUNCTION " + name + "(x bigint) RETURNS bigint RETURN x * 23");
        assertUpdate("CREATE FUNCTION " + name2 + "(s varchar) RETURNS varchar RETURN 'Hello ' || s");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "bigint", "bigint", "scalar", true, "")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .row(name2, "varchar", "varchar", "scalar", true, "")
                        .build());

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(cast(99 as bigint))", "SELECT 2277");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");
        assertQuery("SELECT " + name2 + "('world')", "SELECT 'Hello world'");

        assertQueryFails("DROP FUNCTION " + name + "(varchar)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION " + name + "(z bigint)");
        assertUpdate("DROP FUNCTION " + name + "(double)");
        assertUpdate("DROP FUNCTION " + name + "(int)");
        assertQueryFails("DROP FUNCTION " + name + "(bigint)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION IF EXISTS " + name + "(bigint)");
        assertUpdate("DROP FUNCTION " + name2 + "(varchar)");
        assertQueryFails("DROP FUNCTION " + name2 + "(varchar)", "line 1:1: Function not found");
    }

    private void renamePartitionResourcesOutsideTrino(String tableName, String partitionColumn, String regionKey)
    {
        String partitionName = format("%s=%s", partitionColumn, regionKey);
        String partitionS3KeyPrefix = format("%s/%s/%s", HIVE_TEST_SCHEMA, tableName, partitionName);
        String renamedPartitionSuffix = "CP";

        // Copy whole partition to new location
        MinioClient minioClient = hiveMinioDataLake.getMinioClient();
        minioClient.listObjects(bucketName, "")
                .forEach(objectKey -> {
                    if (objectKey.startsWith(partitionS3KeyPrefix)) {
                        String fileName = objectKey.substring(objectKey.lastIndexOf('/'));
                        String destinationKey = partitionS3KeyPrefix + renamedPartitionSuffix + fileName;
                        minioClient.copyObject(bucketName, objectKey, bucketName, destinationKey);
                    }
                });

        // Delete old partition and update metadata to point to location of new copy
        Table hiveTable = metastoreClient.getTable(HIVE_TEST_SCHEMA, tableName).orElseThrow();
        Partition partition = metastoreClient.getPartition(hiveTable, List.of(regionKey)).orElseThrow();
        Map<String, Map<String, HiveColumnStatistics>> partitionStatistics = metastoreClient.getPartitionColumnStatistics(
                HIVE_TEST_SCHEMA,
                tableName,
                ImmutableSet.of(partitionName),
                partition.getColumns().stream().map(Column::getName).collect(toSet()));

        metastoreClient.dropPartition(HIVE_TEST_SCHEMA, tableName, List.of(regionKey), true);
        metastoreClient.addPartitions(HIVE_TEST_SCHEMA, tableName, List.of(
                new PartitionWithStatistics(
                        Partition.builder(partition)
                                .withStorage(builder -> builder.setLocation(
                                        partition.getStorage().getLocation() + renamedPartitionSuffix))
                                .build(),
                        partitionName,
                        new PartitionStatistics(getHiveBasicStatistics(partition.getParameters()), partitionStatistics.get(partitionName)))));
    }

    protected void assertInsertFailure(String testTable, String expectedMessageRegExp)
    {
        assertInsertFailure(getSession(), testTable, expectedMessageRegExp);
    }

    protected void assertInsertFailure(Session session, String testTable, String expectedMessageRegExp)
    {
        assertQueryFails(
                session,
                createInsertAsSelectFromTpchStatement(testTable),
                expectedMessageRegExp);
    }

    private String createInsertAsSelectFromTpchStatement(String testTable)
    {
        return format("INSERT INTO %s " +
                        "SELECT name, comment, nationkey, regionkey " +
                        "FROM tpch.tiny.nation",
                testTable);
    }

    protected String createInsertStatement(String testTable, List<List<String>> data)
    {
        String values = data.stream()
                .map(row -> String.join(", ", row))
                .collect(Collectors.joining("), ("));
        return format("INSERT INTO %s VALUES (%s)", testTable, values);
    }

    protected void assertOverwritePartition(String testTable)
    {
        computeActual(createInsertStatement(
                testTable,
                ImmutableList.of(
                        ImmutableList.of("'POLAND'", "'Test Data'", "25", "5"),
                        ImmutableList.of("'CZECH'", "'Test Data'", "26", "5"))));
        query(format("SELECT name, comment, nationkey, regionkey FROM %s WHERE regionkey = 5", testTable))
                .assertThat()
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row("POLAND", "Test Data", 25L, 5L)
                        .row("CZECH", "Test Data", 26L, 5L)
                        .build());

        computeActual(createInsertStatement(
                testTable,
                ImmutableList.of(
                        ImmutableList.of("'POLAND'", "'Overwrite'", "25", "5"))));
        query(format("SELECT name, comment, nationkey, regionkey FROM %s WHERE regionkey = 5", testTable))
                .assertThat()
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row("POLAND", "Overwrite", 25L, 5L)
                        .build());
        computeActual(format("DROP TABLE %s", testTable));
    }

    protected String getRandomTestTableName()
    {
        return "nation_" + randomNameSuffix();
    }

    protected String getFullyQualifiedTestTableName()
    {
        return getFullyQualifiedTestTableName(getRandomTestTableName());
    }

    protected String getFullyQualifiedTestTableName(String tableName)
    {
        return getFullyQualifiedTestTableName(HIVE_TEST_SCHEMA, tableName);
    }

    protected String getFullyQualifiedTestTableName(String schemaName, String tableName)
    {
        return "hive.%s.%s".formatted(schemaName, tableName);
    }

    protected String getHiveTestTableName(String tableName)
    {
        return getHiveTestTableName(HIVE_TEST_SCHEMA, tableName);
    }

    protected String getHiveTestTableName(String schemaName, String tableName)
    {
        return "%s.%s".formatted(schemaName, tableName);
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
                        (propertiesEntries.isEmpty() ? "" : propertiesEntries
                                .stream()
                                .collect(joining(",", "WITH (", ")"))),
                tableName);
    }

    protected void copyTpchNationToTable(String testTable)
    {
        computeActual(format("INSERT INTO " + testTable + " SELECT name, comment, nationkey, regionkey FROM tpch.tiny.nation"));
    }

    private void testWriteWithFileSize(String testTable, int scaleFactorInThousands, long fileSizeRangeStart, long fileSizeRangeEnd)
    {
        String scaledColumnExpression = format("array_join(transform(sequence(1, %d), x-> array_join(repeat(comment, 1000), '')), '')", scaleFactorInThousands);
        computeActual(format("INSERT INTO " + testTable + " SELECT %s, %s, regionkey FROM tpch.tiny.nation WHERE nationkey = 9", scaledColumnExpression, scaledColumnExpression));
        query(format("SELECT length(col1) FROM %s", testTable))
                .assertThat()
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(114L * scaleFactorInThousands * 1000)
                        .build());
        query(format("SELECT \"$file_size\" BETWEEN %d AND %d FROM %s", fileSizeRangeStart, fileSizeRangeEnd, testTable))
                .assertThat()
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(true)
                        .build());
    }

    private void addPartitions(
            String sourceTableName,
            String destinationExternalTableName,
            List<String> columnNames,
            TupleDomain<String> partitionsKeyFilter)
    {
        Optional<List<String>> partitionNames = metastoreClient.getPartitionNamesByFilter(HIVE_TEST_SCHEMA, sourceTableName, columnNames, partitionsKeyFilter);
        if (partitionNames.isEmpty()) {
            // nothing to add
            return;
        }
        Table table = metastoreClient.getTable(HIVE_TEST_SCHEMA, sourceTableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(HIVE_TEST_SCHEMA, sourceTableName)));
        Map<String, Optional<Partition>> partitionsByNames = metastoreClient.getPartitionsByNames(table, partitionNames.get());

        metastoreClient.addPartitions(
                HIVE_TEST_SCHEMA,
                destinationExternalTableName,
                partitionsByNames.entrySet().stream()
                        .map(e -> new PartitionWithStatistics(
                                e.getValue()
                                        .map(p -> Partition.builder(p).setTableName(destinationExternalTableName).build())
                                        .orElseThrow(),
                                e.getKey(),
                                PartitionStatistics.empty()))
                        .collect(toImmutableList()));
    }

    private String getTableLocation(String tableName)
    {
        return (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedAcidTable()
    {
        String testTable = getFullyQualifiedTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3",
                "format = 'ORC'",
                "transactional = true"));
        assertInsertFailure(
                testTable,
                "Overwriting existing partition in transactional tables doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
    }
}
