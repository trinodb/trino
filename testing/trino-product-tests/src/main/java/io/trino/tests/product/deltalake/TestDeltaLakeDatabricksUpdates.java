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

import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestDeltaLakeDatabricksUpdates
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testUpdatesFromDatabricks()
    {
        String tableName = "test_updates_" + randomTableSuffix();

        assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                "(id, value) " +
                "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                "  (1, 'Poland')" +
                ", (2, 'Germany')" +
                ", (3, 'Romania')"))
                .containsOnly(row(3));

        try {
            QueryResult databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s ORDER BY id", tableName));
            QueryResult prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactly(toRows(prestoResult));

            assertThat(onDelta().executeQuery(format("UPDATE default.%s SET value = 'France' WHERE id = 2", tableName)))
                    .containsOnly(row(-1));
            databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s ORDER BY id", tableName));
            prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactly(toRows(prestoResult));

            assertThat(onDelta().executeQuery(format("UPDATE default.%s SET value = 'Spain' WHERE id = 2", tableName)))
                    .containsOnly(row(-1));
            databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s ORDER BY id", tableName));
            prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactly(toRows(prestoResult));

            assertThat(onDelta().executeQuery(format("UPDATE default.%s SET value = 'Portugal' WHERE id = 2", tableName)))
                    .containsOnly(row(-1));
            databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s ORDER BY id", tableName));
            prestoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactly(toRows(prestoResult));
        }
        finally {
            assertThat(onDelta().executeQuery("DROP TABLE default." + tableName)).containsExactly(row(-1));
        }
    }

    private static List<QueryAssert.Row> toRows(QueryResult result)
    {
        return result.rows().stream()
                .map(QueryAssert.Row::new)
                .collect(toImmutableList());
    }
}
