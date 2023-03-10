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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.removeS3Directory;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestDeltaLakeActiveFilesCache
        extends BaseTestDeltaLakeS3Storage
{
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

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testRefreshTheFilesCacheWhenTableIsRecreated()
    {
        String tableName = "test_dl_cached_table_files_refresh_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 1");
        // Add the files of the table in the active files cache
        assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(row(1));

        // Recreate the table outside Trino to avoid updating the Trino table active files cache
        onDelta().executeQuery("DROP TABLE default." + tableName);
        // Delete the contents of the table explicitly from storage (because it has been created as `EXTERNAL`)
        removeS3Directory(s3, bucketName, tableDirectory);

        onDelta().executeQuery(format("CREATE TABLE default.%s (col INTEGER) USING DELTA LOCATION 's3://%s/%s'",
                tableName,
                bucketName,
                tableDirectory));
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES 2");

        // TODO https://github.com/trinodb/trino/issues/13737 Fix failure when active files cache is stale
        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                .hasMessageContaining("Error opening Hive split");

        // Verify flushing cache resolve the query failure
        onTrino().executeQuery("CALL delta.system.flush_metadata_cache(schema_name => 'default', table_name => '" + tableName + "')");
        assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(row(2));

        onTrino().executeQuery("DROP TABLE delta.default." + tableName);
    }
}
