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
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeAlterTableCompatibilityDatabricks
        extends BaseTestDeltaLakeS3Storage
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoAlterTablePreservesGeneratedColumn()
    {
        if (getDatabricksRuntimeVersion().orElseThrow().isAtLeast(DATABRICKS_143_RUNTIME_VERSION)) {
            return;
        }

        String tableName = "test_trino_alter_table_preserves_generated_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format(
                """
                CREATE TABLE default.%s (a INT, b INT GENERATED ALWAYS AS (a * 2))
                USING DELTA LOCATION 's3://%s/%s'
                """,
                tableName,
                bucketName,
                tableDirectory));
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("b INT GENERATED ALWAYS AS ( a * 2 )");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a, c) VALUES (1, 3)");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2, 3));

            assertThat(onTrino().executeQuery("SELECT column_name, extra_info FROM delta.information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "'"))
                    .containsOnly(row("a", null), row("b", "generated: a * 2"), row("c", null));
            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName).project(1, 3))
                    .containsOnly(row("a", ""), row("b", "generated: a * 2"), row("c", ""));
            assertThat(onTrino().executeQuery("SHOW COLUMNS FROM delta.default." + tableName).project(1, 3))
                    .containsOnly(row("a", ""), row("b", "generated: a * 2"), row("c", ""));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
