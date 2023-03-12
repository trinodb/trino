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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertLastEntryIsCheckpointed;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertTransactionLogVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestDeltaLakeDatabricksCreateTableAsSelectCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private static final Logger log = Logger.get(TestDeltaLakeDatabricksCreateTableAsSelectCompatibility.class);

    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeTestWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPrestoTypesWithDatabricks()
    {
        String tableName = "test_dl_ctas_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, boolean, tinyint, smallint, integer, bigint, real, double, short_decimal, long_decimal, varchar_25, varchar, date) " +
                    "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, BOOLEAN 'false', TINYINT '-128', SMALLINT '-32768', INTEGER '-2147483648', BIGINT '-9223372036854775808', REAL '-0x1.fffffeP+127f', DOUBLE '-0x1.fffffffffffffP+1023', CAST('-123456789.123456789' AS DECIMAL(18,9)), CAST('-12345678901234567890.123456789012345678' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '1901-01-01')" +
                    ", (2, BOOLEAN 'true', TINYINT '127', SMALLINT '32767', INTEGER '2147483647', BIGINT '9223372036854775807', REAL '0x1.fffffeP+127f', DOUBLE '0x1.fffffffffffffP+1023', CAST('123456789.123456789' AS DECIMAL(18,9)), CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)), CAST('Canada' AS VARCHAR(25)), CAST('Germany' AS VARCHAR), DATE '9999-12-31')" +
                    ", (3, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0', DOUBLE '0', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('' AS VARCHAR(25)), CAST('' AS VARCHAR), DATE '2020-08-22')" +
                    ", (4, BOOLEAN 'true', TINYINT '1', SMALLINT '1', INTEGER '1', BIGINT '1', REAL '1', DOUBLE '1', CAST('1' AS DECIMAL(18,9)), CAST('1' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')" +
                    ", (5, BOOLEAN 'true', TINYINT '37', SMALLINT '12242', INTEGER '2524', BIGINT '132', REAL '3.141592653', DOUBLE '2.718281828459045', CAST('3.141592653' AS DECIMAL(18,9)), CAST('2.718281828459045' AS DECIMAL(38,18)), CAST('\u653b\u6bbb\u6a5f\u52d5\u968a' AS VARCHAR(25)), CAST('\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!' AS VARCHAR), DATE '2020-08-22')" +
                    ", (6, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0x0.000002P-126f', DOUBLE '0x0.0000000000001P-1022', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')" +
                    ", (7, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0x1.0p-126f', DOUBLE '0x1.0p-1022', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')"))
                    .containsOnly(row(7));

            QueryResult databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s", tableName));
            QueryResult prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            assertThat(databricksResult).containsOnly(prestoResult.rows().stream()
                    .map(QueryAssert.Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPrestoTimestampsWithDatabricks()
    {
        String tableName = "test_dl_ctas_timestamps_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, timestamp_in_utc, timestamp_in_new_york, timestamp_in_warsaw) " +
                    "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, TIMESTAMP '1901-01-01 00:00:00 UTC', TIMESTAMP '1902-01-01 00:00:00 America/New_York', TIMESTAMP '1902-01-01 00:00:00 Europe/Warsaw')" +
                    ", (2, TIMESTAMP '9999-12-31 23:59:59.999 UTC', TIMESTAMP '9998-12-31 23:59:59.999 America/New_York', TIMESTAMP '9998-12-31 23:59:59.999 Europe/Warsaw')" +
                    ", (3, TIMESTAMP '2020-06-10 15:55:23 UTC', TIMESTAMP '2020-06-10 15:55:23.123 America/New_York', TIMESTAMP '2020-06-10 15:55:23.123 Europe/Warsaw')" +
                    ", (4, TIMESTAMP '2001-08-22 03:04:05.321 UTC', TIMESTAMP '2001-08-22 03:04:05.321 America/New_York', TIMESTAMP '2001-08-22 03:04:05.321 Europe/Warsaw')"))
                    .containsOnly(row(4));

            QueryResult databricksResult = onDelta().executeQuery("SELECT id, date_format(timestamp_in_utc, \"yyyy-MM-dd HH:mm:ss.SSS\"), date_format(timestamp_in_new_york, \"yyyy-MM-dd HH:mm:ss.SSS\"), date_format(timestamp_in_warsaw, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName);
            QueryResult prestoResult = onTrino().executeQuery("SELECT id, format('%1$tF %1$tT.%1$tL', timestamp_in_utc), format('%1$tF %1$tT.%1$tL', timestamp_in_new_york), format('%1$tF %1$tT.%1$tL', timestamp_in_warsaw) FROM delta.default.\"" + tableName + "\"");
            assertThat(databricksResult).containsOnly(prestoResult.rows().stream()
                    .map(QueryAssert.Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPrestoCacheInvalidatedOnCreateTable()
            throws URISyntaxException, IOException
    {
        String tableName = "test_dl_ctas_caching_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, boolean, tinyint) " +
                    "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, BOOLEAN 'false', TINYINT '-128')" +
                    ", (2, BOOLEAN 'true', TINYINT '127')" +
                    ", (3, BOOLEAN 'false', TINYINT '0')" +
                    ", (4, BOOLEAN 'false', TINYINT '1')" +
                    ", (5, BOOLEAN 'true', TINYINT '37')"))
                    .containsOnly(row(5));

            QueryResult databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s", tableName));
            QueryResult prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            assertThat(databricksResult).containsOnly(prestoResult.rows().stream()
                    .map(QueryAssert.Row::new)
                    .collect(toImmutableList()));

            dropDeltaTableWithRetry("default." + tableName);
            removeS3Directory(bucketName, "databricks-compatibility-test-" + tableName);

            assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, boolean, tinyint) " +
                    "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, BOOLEAN 'true', TINYINT '1')" +
                    ", (2, BOOLEAN 'true', TINYINT '1')" +
                    ", (3, BOOLEAN 'false', TINYINT '2')" +
                    ", (4, BOOLEAN 'true', TINYINT '3')" +
                    ", (5, BOOLEAN 'true', TINYINT '5')" +
                    ", (6, BOOLEAN 'false', TINYINT '8')" +
                    ", (7, BOOLEAN 'true', TINYINT '13')"))
                    .containsOnly(row(7));

            databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s", tableName));
            prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            assertThat(databricksResult).containsOnly(prestoResult.rows().stream()
                    .map(QueryAssert.Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateFromTrinoWithDefaultPartitionValues()
    {
        String tableName = "test_create_partitioned_table_default_as_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery(
                    "CREATE TABLE delta.default." + tableName + "(number_partition, string_partition, a_value) " +
                            "WITH (" +
                            "   location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', " +
                            "   partitioned_by = ARRAY['number_partition', 'string_partition']) " +
                            "AS VALUES (NULL, 'partition_a', 'jarmuz'), (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')"))
                    .containsOnly(row(3));

            List<QueryAssert.Row> expectedRows = ImmutableList.of(
                    row(null, "partition_a", "jarmuz"),
                    row(1, null, "brukselka"),
                    row(null, null, "kalafior"));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReplaceTableWithSchemaChange()
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            ImmutableList.Builder<QueryAssert.Row> expected = ImmutableList.builder();
            // Write to the table until a checkpoint file is written
            for (int i = 0; i < 12; i++) {
                onDelta().executeQuery("INSERT INTO " + tableName + " VALUES \"1960-01-01 01:02:03\", \"1961-01-01 01:02:03\", \"1962-01-01 01:02:03\"");
                expected.add(row("1960-01-01T01:02:03.000Z"), row("1961-01-01T01:02:03.000Z"), row("1962-01-01T01:02:03.000Z"));
            }

            assertTransactionLogVersion(s3, bucketName, tableName, 12);
            onDelta().executeQuery("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT CAST(ts AS TIMESTAMP) FROM " + tableName);
            assertThat(onTrino().executeQuery("SELECT to_iso8601(ts) FROM delta.default." + tableName)).containsOnly(expected.build());
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    // Databricks 11.3 doesn't create a checkpoint file at 'CREATE OR REPLACE TABLE' statement
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReplaceTableWithSchemaChangeOnCheckpoint()
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            ImmutableList.Builder<QueryAssert.Row> expected = ImmutableList.builder();
            // Write to the table until a checkpoint file is written
            for (int i = 0; i < 9; i++) {
                onDelta().executeQuery("INSERT INTO " + tableName + " VALUES \"1960-01-01 01:02:03\", \"1961-01-01 01:02:03\", \"1962-01-01 01:02:03\"");
                expected.add(row("1960-01-01T01:02:03.000Z"), row("1961-01-01T01:02:03.000Z"), row("1962-01-01T01:02:03.000Z"));
            }
            onDelta().executeQuery("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT CAST(ts AS TIMESTAMP) FROM " + tableName);
            assertLastEntryIsCheckpointed(s3, bucketName, tableName);

            onDelta().executeQuery("INSERT INTO " + tableName + " VALUES \"1960-01-01 01:02:03\", \"1961-01-01 01:02:03\", \"1962-01-01 01:02:03\"");
            expected.add(row("1960-01-01T01:02:03.000Z"), row("1961-01-01T01:02:03.000Z"), row("1962-01-01T01:02:03.000Z"));
            assertTransactionLogVersion(s3, bucketName, tableName, 11);

            assertThat(onTrino().executeQuery("SELECT to_iso8601(ts) FROM delta.default." + tableName)).containsOnly(expected.build());
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    private void removeS3Directory(String bucketName, String directoryPrefix)
    {
        ObjectListing listing = s3.listObjects(bucketName, directoryPrefix);
        do {
            List<String> objectKeys = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toUnmodifiableList());
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(objectKeys.toArray(new String[0]));
            log.info("Deleting keys: %s", objectKeys);
            s3.deleteObjects(deleteObjectsRequest);
            listing = s3.listNextBatchOfObjects(listing);
        }
        while (listing.isTruncated());
    }
}
