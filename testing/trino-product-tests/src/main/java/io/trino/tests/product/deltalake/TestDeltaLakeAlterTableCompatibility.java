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

import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeAlterTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testAddColumnWithCommentOnTrino()
    {
        String tableName = "test_dl_add_column_with_comment_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT COMMENT 'new column comment'");
            assertEquals(getColumnCommentOnTrino("default", tableName, "new_col"), "new column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "new_col"), "new column comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testAddColumnUnsupportedWriterVersion()
    {
        String tableName = "test_dl_add_column_unsupported_writer_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='3')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col int"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 3 which is not supported");
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }
}
