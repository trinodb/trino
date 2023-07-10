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
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestDeltaLakeSelectCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPartitionedSelectSpecialCharacters()
    {
        String tableName = "test_dl_partitioned_select_special" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, a_string STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (a_string)" +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES " +
                    "(1, 'spark=equal'), " +
                    "(2, 'spark+plus'), " +
                    "(3, 'spark space')," +
                    "(4, 'spark:colon')," +
                    "(5, 'spark%percent')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES " +
                    "(10, 'trino=equal'), " +
                    "(20, 'trino+plus'), " +
                    "(30, 'trino space')," +
                    "(40, 'trino:colon')," +
                    "(50, 'trino%percent')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "spark=equal"),
                    row(2, "spark+plus"),
                    row(3, "spark space"),
                    row(4, "spark:colon"),
                    row(5, "spark%percent"),
                    row(10, "trino=equal"),
                    row(20, "trino+plus"),
                    row(30, "trino space"),
                    row(40, "trino:colon"),
                    row(50, "trino%percent"));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            String deltaFilePath = (String) onDelta().executeQuery("SELECT input_file_name() FROM default." + tableName + " WHERE a_number = 1").getOnlyValue();
            String trinoFilePath = (String) onTrino().executeQuery("SELECT \"$path\" FROM delta.default." + tableName + " WHERE a_number = 1").getOnlyValue();
            // File paths returned by the input_file_name function are URI encoded https://github.com/delta-io/delta/issues/1517 while the $path of Trino is not
            assertNotEquals(deltaFilePath, trinoFilePath);
            assertEquals(format("s3://%s%s", bucketName, URI.create(deltaFilePath).getPath()), trinoFilePath);

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE \"$path\" = '" + trinoFilePath + "'"))
                    .containsOnly(row(1, "spark=equal"));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
