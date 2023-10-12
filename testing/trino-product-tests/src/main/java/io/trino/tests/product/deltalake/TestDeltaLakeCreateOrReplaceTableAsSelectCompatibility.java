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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertLastEntryIsCheckpointed;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertTransactionLogVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeMethodWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateOrReplaceTableOnDeltaWithSchemaChange()
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            List<Row> expectedRows = performInsert(onDelta(), tableName, 12);

            assertTransactionLogVersion(s3, bucketName, tableName, 12);
            onDelta().executeQuery("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT CAST(ts AS TIMESTAMP) FROM " + tableName);
            assertThat(onTrino().executeQuery("SELECT to_iso8601(ts) FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateOrReplaceTableOnTrinoWithSchemaChange()
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            List<Row> expectedRows = performInsert(onDelta(), tableName, 12);

            assertTransactionLogVersion(s3, bucketName, tableName, 12);
            onTrino().executeQuery("CREATE OR REPLACE TABLE delta.default." + tableName + " AS SELECT CAST(ts AS TIMESTAMP(6)) as ts FROM " + tableName);
            assertThat(onDelta().executeQuery("SELECT date_format(ts, \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\") FROM default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateOrReplaceTableWithSchemaChangeOnCheckpoint()
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            ImmutableList.Builder<Row> expected = ImmutableList.builder();
            expected.addAll(performInsert(onDelta(), tableName, 9));

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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateOrReplaceTableOnAppendOnlyTableFails()
    {
        String tableName = "test_replace_on_append_only_table_fails_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.appendOnly' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 12)");
            assertQueryFailure(() -> onDelta().executeQuery("" +
                    "CREATE OR REPLACE TABLE default." + tableName +
                    "         (a INT,c INT) " +
                    "         USING delta " +
                    "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                    "         TBLPROPERTIES ('delta.appendOnly' = true)"))
                    .hasMessageContaining("This table is configured to only allow appends");

            // Try setting `delta.appendOnly` to false
            assertQueryFailure(() -> onDelta().executeQuery("" +
                    "CREATE OR REPLACE TABLE default." + tableName +
                    "         (a INT,b INT) " +
                    "         USING delta " +
                    "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                    "         TBLPROPERTIES ('delta.appendOnly' = false)"))
                    .hasMessageContaining("This table is configured to only allow appends");

            assertQueryFailure(() -> onTrino().executeQuery("" +
                    "CREATE OR REPLACE TABLE delta.default." + tableName + " (a INT,c INT)" +
                    " WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "')"))
                    .hasMessageContaining("Cannot replace a table when 'delta.appendOnly' is set to true");
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateOrReplaceTableAsSelectOnAppendOnlyTableFails()
    {
        String tableName = "test_replace_on_append_only_table_fails_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.appendOnly' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 12)");
            assertThatThrownBy(() -> onDelta().executeQuery("" +
                    "CREATE OR REPLACE TABLE default." + tableName +
                    "         USING delta " +
                    "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                    "         TBLPROPERTIES ('delta.appendOnly' = true)" +
                    "         AS SELECT 1 as e"))
                    .hasMessageContaining("This table is configured to only allow appends");

            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE OR REPLACE TABLE delta.default." + tableName +
                    " WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS SELECT 1 as e"))
                    .hasMessageContaining("Cannot replace a table when 'delta.appendOnly' is set to true");
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    private static List<Row> performInsert(QueryExecutor queryExecutor, String tableName, int numberOfRows)
    {
        ImmutableList.Builder<Row> expectedRowBuilder = ImmutableList.builder();
        // Write to the table until a checkpoint file is written
        for (int i = 0; i < numberOfRows; i++) {
            queryExecutor.executeQuery("INSERT INTO " + tableName + " VALUES \"1960-01-01 01:02:03\", \"1961-01-01 01:02:03\", \"1962-01-01 01:02:03\"");
            expectedRowBuilder.add(row("1960-01-01T01:02:03.000Z"), row("1961-01-01T01:02:03.000Z"), row("1962-01-01T01:02:03.000Z"));
        }
        return expectedRowBuilder.build();
    }
}
