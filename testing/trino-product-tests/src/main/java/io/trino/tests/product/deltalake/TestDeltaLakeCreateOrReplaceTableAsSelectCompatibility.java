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
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertLastEntryIsCheckpointed;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertTransactionLogVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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
}
