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

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeCaseInsensitiveMappingDatabricks
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testIdentityColumnWithNonLowerCaseColumnName(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_identity_column_case_sensitivity_" + randomNameSuffix();
        env.executeDatabricksSql("CREATE TABLE default." + tableName +
                "(data INT, UPPERCASE_IDENTITY BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='name')");
        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " (data) VALUES (1), (2), (3)");

            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1), row(2, 2), row(3, 3));

            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (4, 4)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertThatThrownBy(() -> env.executeTrinoSql("UPDATE delta.default." + tableName + " SET data = 3"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertThatThrownBy(() -> env.executeTrinoSql("DELETE FROM delta.default." + tableName))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertThatThrownBy(() -> env.executeTrinoSql("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.data = s.data) WHEN MATCHED THEN UPDATE SET data = 1"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }
}
