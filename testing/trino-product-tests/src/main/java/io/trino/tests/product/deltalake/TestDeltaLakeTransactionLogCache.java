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
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeTransactionLogCache
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

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testAllDataFilesAreLoadedWhenTransactionLogFileAfterTheCachedTableVersionIsMissing()
    {
        String tableName = "test_dl_cached_table_files_accuracy_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s', checkpoint_interval = 10)",
                tableName,
                bucketName,
                tableDirectory));

        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES 1");
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(row(1));

        // Perform multiple changes on the table outside of Trino to avoid updating the Trino table active files cache
        onDelta().executeQuery("DELETE FROM default." + tableName);
        // Perform more than 10 to make sure there is a checkpoint being created
        IntStream.range(2, 13).forEach(v -> onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES " + v));

        List<QueryAssert.Row> expectedRows = List.of(
                row(2),
                row(3),
                row(4),
                row(5),
                row(6),
                row(7),
                row(8),
                row(9),
                row(10),
                row(11),
                row(12));

        // Delete the first few transaction log files because they can safely be discarded
        // once there is a checkpoint created.
        String[] transactionLogFilesToRemove = {
                tableDirectory + "/_delta_log/00000000000000000000.json",
                tableDirectory + "/_delta_log/00000000000000000001.json",
                tableDirectory + "/_delta_log/00000000000000000002.json",
                tableDirectory + "/_delta_log/00000000000000000003.json",
                tableDirectory + "/_delta_log/00000000000000000004.json",
                tableDirectory + "/_delta_log/00000000000000000005.json"
        };
        DeleteObjectsResult deleteObjectsResult = s3.deleteObjects(
                new DeleteObjectsRequest(bucketName)
                        .withKeys(transactionLogFilesToRemove));
        Assertions.assertThat(
                        deleteObjectsResult.getDeletedObjects().stream()
                                .map(DeleteObjectsResult.DeletedObject::getKey)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(transactionLogFilesToRemove);

        assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                .containsOnly(expectedRows);
        // The internal data files table cached value for the Delta table should be
        // fully refreshed now.
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                .containsOnly(expectedRows);

        onTrino().executeQuery("DROP TABLE " + tableName);
    }
}
