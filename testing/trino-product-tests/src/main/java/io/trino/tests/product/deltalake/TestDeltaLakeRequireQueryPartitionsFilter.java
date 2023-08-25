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

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnNamesOnDelta;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeRequireQueryPartitionsFilter
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testRequiresQueryPartitionFilterWithUppercaseColumnName()
    {
        String tableName = "test_require_partition_filter_" + randomNameSuffix();

        onDelta().executeQuery("""
                CREATE TABLE default.%s
                (X integer, PART integer)
                USING DELTA PARTITIONED BY (PART)
                LOCATION 's3://%s/databricks-compatibility-test-%s'
                """.formatted(tableName, bucketName, tableName));

        try {
            assertThat(getColumnNamesOnDelta("default", tableName)).containsExactly("X", "PART");

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22)");

            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(row(1, 11), row(2, 22));
            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(row(1, 11), row(2, 22));

            onTrino().executeQuery("SET SESSION delta.query_partition_filter_required = true");

            assertThat(onTrino().executeQuery(format("SELECT * FROM %s WHERE \"part\" = 11", tableName))).containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s WHERE \"PART\" = 11", tableName))).containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s WHERE \"Part\" = 11", tableName))).containsOnly(row(1, 11));
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }
}
