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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeDeleteCompatibilityDatabricks
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTruncateTable(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_truncate_table_" + randomNameSuffix();
        env.executeTrinoSql("" +
                "CREATE TABLE delta.default." + tableName +
                "(a INT)" +
                "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "')");
        try {
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES 1, 2, 3");
            env.executeTrinoSql("TRUNCATE TABLE delta.default." + tableName);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).hasNoRows();

            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES 4, 5, 6");
            env.executeDatabricksSql("TRUNCATE TABLE default." + tableName);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            env.executeTrinoSql("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDeletionVectorsTruncateTable(DeltaLakeDatabricksEnvironment env)
    {
        testDeletionVectorsDeleteAll(env, tableName -> env.executeDatabricksSql("TRUNCATE TABLE default." + tableName));
    }

    private void testDeletionVectorsDeleteAll(DeltaLakeDatabricksEnvironment env, Consumer<String> deleteRow)
    {
        String tableName = "test_deletion_vectors_delete_all_" + randomNameSuffix();
        env.executeDatabricksSql("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 1000))");
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).hasRowsCount(1000);

            deleteRow.accept(tableName);

            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).hasNoRows();
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }
}
