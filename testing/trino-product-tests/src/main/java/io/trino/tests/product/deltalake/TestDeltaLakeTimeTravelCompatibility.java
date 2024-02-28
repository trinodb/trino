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
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeTimeTravelCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testReadFromTableRestoredToPreviousVersion()
    {
        String tableName = "test_dl_time_travel_restore_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (a_integer integer) WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 1");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 2");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 3");
            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_integer = a_integer + 10 WHERE a_integer > 1");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_integer < 10");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_varchar varchar");
            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_varchar = 'foo'");

            List<QueryAssert.Row> expectedRows = ImmutableList.of(
                    row(12, "foo"),
                    row(13, "foo"));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            onDelta().executeQuery("RESTORE TABLE default." + tableName + " TO VERSION AS OF 1");
            expectedRows = ImmutableList.of(row(1));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSelectForVersionAsOf()
    {
        String tableName = "test_dl_select_version" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (id INT, part STRING)" +
                "USING delta " +
                "PARTITIONED BY (part)" +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'spark')");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN new_column INT");

            // Both Spark and Trino Delta Lake connector use the old table definition for the versioned query
            List<QueryAssert.Row> expectedRows = ImmutableList.of(row(1, "spark"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName + " VERSION AS OF 1"))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " FOR VERSION AS OF 1"))
                    .containsOnly(expectedRows);

            // Do time travel after table replacement
            onDelta().executeQuery("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT id + 1 AS id, part, new_column FROM " + tableName);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName + " VERSION AS OF 1"))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " FOR VERSION AS OF 1"))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
