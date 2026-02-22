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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getColumnCommentOnDatabricks;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getColumnNamesOnDatabricks;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getTableCommentOnDatabricks;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.DeltaLakeExclude173
@TestGroup.ProfileSpecificTests
class TestDeltaLakeIdentityColumnCompatibility
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testIdentityColumn(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_identity_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeDatabricksSql(format(
                """
                CREATE TABLE default.%s (a INT, b BIGINT GENERATED ALWAYS AS IDENTITY)
                USING DELTA LOCATION 's3://%s/%s'
                """,
                tableName,
                env.getBucketName(),
                tableDirectory));
        try {
            env.executeTrinoSql("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test column comment'");
            assertThat(getColumnCommentOnDatabricks(env, "default", tableName, "b")).isEqualTo("test column comment");

            env.executeTrinoSql("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            assertThat(getTableCommentOnDatabricks(env, "default", tableName)).isEqualTo("test table comment");

            env.executeTrinoSql("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");
            assertThat(getColumnNamesOnDatabricks(env, "default", tableName)).containsExactly("a", "b", "c");

            assertThat((String) env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("b BIGINT GENERATED ALWAYS AS IDENTITY");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " (a, c) VALUES (0, 2)");

            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(row(0, 1, 2));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(row(0, 1, 2));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testRenameIdentityColumn(String mode, DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_rename_identity_column_" + randomNameSuffix();

        env.executeDatabricksSql("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')");
        try {
            env.executeDatabricksSql("ALTER TABLE default." + tableName + " RENAME COLUMN col_identity TO delta_col_identity");
            assertThat((String) env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("delta_col_identity BIGINT GENERATED ALWAYS AS IDENTITY");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " (data) VALUES 10");
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1));

            env.executeTrinoSql("ALTER TABLE delta.default." + tableName + " RENAME COLUMN delta_col_identity TO trino_col_identity");
            assertThat((String) env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("trino_col_identity BIGINT GENERATED ALWAYS AS IDENTITY");
            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " (data) VALUES (1)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " (data) VALUES 20");
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1), row(20, 2));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1), row(20, 2));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDropIdentityColumn(String mode, DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_drop_identity_column_" + randomNameSuffix();

        env.executeDatabricksSql("CREATE TABLE default." + tableName +
                "(data INT, first_identity BIGINT GENERATED ALWAYS AS IDENTITY, second_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')");
        try {
            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " (data) VALUES (1)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");

            env.executeDatabricksSql("ALTER TABLE default." + tableName + " DROP COLUMN first_identity");
            assertThat(getColumnNamesOnDatabricks(env, "default", tableName))
                    .containsExactly("data", "second_identity");
            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " (data) VALUES (1)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");

            env.executeTrinoSql("ALTER TABLE delta.default." + tableName + " DROP COLUMN second_identity");
            assertThat(getColumnNamesOnDatabricks(env, "default", tableName))
                    .containsExactly("data");

            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES 10");
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES 20");

            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10), row(20));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName))
                    .containsOnly(row(10), row(20));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testVacuumProcedureWithIdentityColumn(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_vacuum_identity_column_" + randomNameSuffix();

        env.executeDatabricksSql("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/" + "databricks-compatibility-test-" + tableName + "'");
        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " (data) VALUES 10");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " (data) VALUES 20");
            env.executeDatabricksSql("DELETE FROM default." + tableName + " WHERE data = 20");

            executeTrinoVacuumWithMinRetention(env, tableName);

            assertThat((String) env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("col_identity BIGINT GENERATED ALWAYS AS IDENTITY");

            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testIdentityColumnCheckpointInterval(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_identity_column_checkpoint_interval_" + randomNameSuffix();

        env.executeDatabricksSql("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.checkpointInterval' = 1)");
        try {
            env.executeTrinoSql("COMMENT ON COLUMN delta.default." + tableName + ".col_identity IS 'test column comment'");
            assertThat((String) env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("col_identity BIGINT GENERATED ALWAYS AS IDENTITY");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    static Stream<String> columnMappingDataProvider()
    {
        return Stream.of("id", "name");
    }

    private static void executeTrinoVacuumWithMinRetention(DeltaLakeDatabricksEnvironment env, String tableName)
    {
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION delta.vacuum_min_retention = '0s'");
            statement.execute("CALL delta.system.vacuum('default', '" + tableName + "', '0s')");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Trino vacuum for table " + tableName, e);
        }
    }
}
