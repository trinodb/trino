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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeUpdateCompatibility
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testUpdatesFromDatabricks(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_updates_" + randomNameSuffix();

        assertThat(env.executeTrinoSql("CREATE TABLE delta.default.\"" + tableName + "\" " +
                "(id, value) " +
                "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                "  (1, 'Poland')" +
                ", (2, 'Germany')" +
                ", (3, 'Romania')"))
                .containsOnly(row(3));

        try {
            QueryResult databricksResult = env.executeDatabricksSql(format("SELECT * FROM default.%s ORDER BY id", tableName));
            QueryResult trinoResult = env.executeTrinoSql(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactlyInOrder(toRows(trinoResult));

            env.executeDatabricksSql(format("UPDATE default.%s SET value = 'France' WHERE id = 2", tableName));
            databricksResult = env.executeDatabricksSql(format("SELECT * FROM default.%s ORDER BY id", tableName));
            trinoResult = env.executeTrinoSql(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactlyInOrder(toRows(trinoResult));

            env.executeDatabricksSql(format("UPDATE default.%s SET value = 'Spain' WHERE id = 2", tableName));
            databricksResult = env.executeDatabricksSql(format("SELECT * FROM default.%s ORDER BY id", tableName));
            trinoResult = env.executeTrinoSql(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactlyInOrder(toRows(trinoResult));

            env.executeDatabricksSql(format("UPDATE default.%s SET value = 'Portugal' WHERE id = 2", tableName));
            databricksResult = env.executeDatabricksSql(format("SELECT * FROM default.%s ORDER BY id", tableName));
            trinoResult = env.executeTrinoSql(format("SELECT * FROM delta.default.\"%s\" ORDER BY id", tableName));
            assertThat(databricksResult).containsExactlyInOrder(toRows(trinoResult));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    private static List<Row> toRows(QueryResult result)
    {
        return result.getRows();
    }
}
