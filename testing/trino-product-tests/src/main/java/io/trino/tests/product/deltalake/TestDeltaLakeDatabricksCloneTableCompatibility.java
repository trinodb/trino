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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.testing.DataProviders;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestDeltaLakeDatabricksCloneTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadFromShallowClonedTable(boolean partitioned)
    {
        testReadClonedTable(true, partitioned);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadFromDeepClonedTable(boolean partitioned)
    {
        testReadClonedTable(false, partitioned);
    }

    private void testReadClonedTable(final boolean shallowClone, final boolean partitioned)
    {
        final String baseTable = "test_dl_base_table_" + randomNameSuffix();
        final String clonedTableV1 = "test_dl_clone_tableV1_" + randomNameSuffix();
        final String clonedTableV2 = "test_dl_clone_tableV2_" + randomNameSuffix();
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable + " (a_number INT, b_string STRING) USING delta " +
                    (partitioned ? "PARTITIONED BY (b_string) " : "") + "LOCATION 's3://" + bucketName + "/+ab+/a%25/a%2525/databricks-compatibility-test-" + baseTable + "'");

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, \"a%25\")");

            List<QueryAssert.Row> expectedRows = ImmutableList.of(row(1, "a%25"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable)).containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable)).containsOnly(expectedRows);

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (2, \"a%26\"), (1122, \"with?question\"), (11, \"a%2525\"), (22, \"a%2526\")");

            onDelta().executeQuery("CREATE TABLE default." + clonedTableV1 + (shallowClone ? " SHALLOW CLONE" : " DEEP CLONE") + " default." + baseTable + " VERSION AS OF 1");

            List<QueryAssert.Row> expectedRowsV1 = ImmutableList.of(row(1, "a%25"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 1")).containsOnly(expectedRowsV1);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV1)).containsOnly(expectedRowsV1);

            onDelta().executeQuery("CREATE TABLE default." + clonedTableV2 + (shallowClone ? " SHALLOW CLONE" : " DEEP CLONE") + " default." + baseTable + " VERSION AS OF 2");

            List<QueryAssert.Row> expectedRowsV2 = ImmutableList.of(row(1, "a%25"), row(2, "a%26"), row(1122, "with?question"),
                    row(11, "a%2525"), row(22, "a%2526"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable)).containsOnly(expectedRowsV2);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable)).containsOnly(expectedRowsV2);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 2")).containsOnly(expectedRowsV2);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV2)).containsOnly(expectedRowsV2);

            if (partitioned) {
                List<QueryAssert.Row> expectedPartitionRows = ImmutableList.of(row("a%25"), row("a%26"), row("with?question"),
                        row("a%2525"), row("a%2526"));
                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + baseTable)).containsOnly(expectedPartitionRows);
                assertThat(onTrino().executeQuery("SELECT b_string FROM delta.default." + baseTable)).containsOnly(expectedPartitionRows);
                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + baseTable + " VERSION AS OF 2")).containsOnly(expectedPartitionRows);
                assertThat(onTrino().executeQuery("SELECT b_string FROM delta.default." + clonedTableV2)).containsOnly(expectedPartitionRows);
            }
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
            dropDeltaTableWithRetry("default." + clonedTableV1);
            dropDeltaTableWithRetry("default." + clonedTableV2);
        }
    }
}
