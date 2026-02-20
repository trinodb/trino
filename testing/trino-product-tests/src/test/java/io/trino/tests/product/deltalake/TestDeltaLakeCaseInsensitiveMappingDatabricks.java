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
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_143_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeCaseInsensitiveMappingDatabricks
        extends BaseTestDeltaLakeS3Storage
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testGeneratedColumnWithNonLowerCaseColumnName()
    {
        if (getDatabricksRuntimeVersion().orElseThrow().isAtLeast(DATABRICKS_143_RUNTIME_VERSION)) {
            return;
        }

        String tableName = "test_dl_generated_column_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(A INT, B INT GENERATED ALWAYS AS (A * 2))" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".a IS 'test comment for a'");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test comment for b'");

            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testIdentityColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_identity_column_case_sensitivity_" + randomNameSuffix();
        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, UPPERCASE_IDENTITY BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='name')");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES (1), (2), (3)");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1), row(2, 2), row(3, 3));

            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (4, 4)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertThatThrownBy(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET data = 3"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertThatThrownBy(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertThatThrownBy(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.data = s.data) WHEN MATCHED THEN UPDATE SET data = 1"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");

            if (getDatabricksRuntimeVersion().orElseThrow().isAtLeast(DATABRICKS_143_RUNTIME_VERSION)) {
                return;
            }

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col integer");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN new_col TO renamed_col");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".uppercase_identity IS 'test comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN renamed_col");

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("UPPERCASE_IDENTITY BIGINT GENERATED ALWAYS AS IDENTITY");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN uppercase_identity TO renamed_identity");
            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("renamed_identity BIGINT GENERATED ALWAYS AS IDENTITY");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
