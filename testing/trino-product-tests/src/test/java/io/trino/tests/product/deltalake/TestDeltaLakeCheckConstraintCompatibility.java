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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * JUnit 5 port of Delta Lake check constraint compatibility tests from TestDeltaLakeCheckConstraintCompatibility.
 * <p>
 * This class ports only the tests marked with DELTA_LAKE_OSS group.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeCheckConstraintCompatibility
{
    @ParameterizedTest
    @MethodSource("checkConstraints")
    void testCheckConstraintInsertCompatibility(String columnDefinition, String checkConstraint, String validInput, Row insertedValue, String invalidInput, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_check_constraint_insert_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(" + columnDefinition + ") " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (" + checkConstraint + ")");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (" + validInput + ")");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(insertedValue);
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (" + validInput + ")");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(insertedValue, insertedValue);

            assertSparkCheckConstraintViolation(() -> env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (" + invalidInput + ")"));
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (" + invalidInput + ")"))
                    .hasMessageContaining("Check constraint violation");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(insertedValue, insertedValue);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    static Stream<Arguments> checkConstraints()
    {
        return Stream.of(
                // columnDefinition, checkConstraint, validInput, insertedValue, invalidInput
                // Operand
                Arguments.of("a INT", "a = 1", "1", row(1), "2"),
                Arguments.of("a INT", "a > 1", "2", row(2), "1"),
                Arguments.of("a INT", "a >= 1", "1", row(1), "0"),
                Arguments.of("a INT", "a < 1", "0", row(0), "1"),
                Arguments.of("a INT", "a <= 1", "1", row(1), "2"),
                Arguments.of("a INT", "a <> 1", "2", row(2), "1"),
                Arguments.of("a INT", "a != 1", "2", row(2), "1"),
                // Arithmetic binary
                Arguments.of("a INT, b INT", "a = b + 1", "2, 1", row(2, 1), "2, 2"),
                Arguments.of("a INT, b INT", "a = b - 1", "1, 2", row(1, 2), "1, 3"),
                Arguments.of("a INT, b INT", "a = b * 2", "4, 2", row(4, 2), "4, 3"),
                Arguments.of("a INT, b INT", "a = b / 2", "2, 4", row(2, 4), "2, 6"),
                Arguments.of("a INT, b INT", "a = b % 2", "1, 5", row(1, 5), "1, 6"),
                Arguments.of("a INT, b INT", "a = b & 5", "1, 3", row(1, 3), "1, 4"),
                Arguments.of("a INT, b INT", "a = b ^ 5", "6, 3", row(6, 3), "6, 4"),
                // Between
                Arguments.of("a INT", "a BETWEEN 1 AND 10", "1", row(1), "0"),
                Arguments.of("a INT", "a BETWEEN 1 AND 10", "10", row(10), "11"),
                Arguments.of("a INT", "a NOT BETWEEN 1 AND 10", "0", row(0), "1"),
                Arguments.of("a INT", "a NOT BETWEEN 1 AND 10", "11", row(11), "10"),
                Arguments.of("a INT, b INT, c INT", "a BETWEEN b AND c", "5, 1, 10", row(5, 1, 10), "11, 1, 10"),
                // Supported types
                Arguments.of("a INT", "a < 100", "1", row(1), "100"),
                Arguments.of("a STRING", "a = 'valid'", "'valid'", row("valid"), "'invalid'"),
                Arguments.of("a STRING", "a = \"double-quote\"", "'double-quote'", row("double-quote"), "'invalid'"),
                // Identifier
                Arguments.of("`a.dot` INT", "`a.dot` = 1", "1", row(1), "2"));
    }

    @Test
    void testCheckConstraintUpdateCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_check_constraint_update_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(a INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (a < 3)");

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1));
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET a = 2");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2));

            assertSparkCheckConstraintViolation(() -> env.executeSparkUpdate("UPDATE default." + tableName + " SET a = 3"));
            assertThatThrownBy(() -> env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET a = 3"))
                    .hasMessageContaining("Check constraint violation");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testCheckConstraintMergeCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName1 = "test_check_constraint_merge_" + randomNameSuffix();
        String tableName2 = "test_check_constraint_merge_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName1 +
                "(a INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName1 + "'");

        env.executeSparkUpdate("CREATE TABLE default." + tableName2 +
                "(a INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName2 + "'");

        try {
            env.executeSparkUpdate("ALTER TABLE default." + tableName1 + " ADD CONSTRAINT a_constraint CHECK (a < 3)");

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName1 + " VALUES (1)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName2 + " VALUES (1), (2)");

            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName1))
                    .containsOnly(row(1));

            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName1 + " t USING delta.default." + tableName2 + " s " +
                    "ON (t.a = s.a) " +
                    "WHEN MATCHED " +
                    "THEN UPDATE SET a = 2 " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (a) VALUES (2)");

            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName1))
                    .containsOnly(
                            row(2),
                            row(2));

            assertSparkCheckConstraintViolation(() -> env.executeSparkUpdate("MERGE INTO default." + tableName1 + " t USING default." + tableName2 + " s " +
                    "ON (t.a = s.a) " +
                    "WHEN MATCHED " +
                    "THEN UPDATE SET a = 3 " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (a) VALUES (3)"));
            assertThatThrownBy(() -> env.executeTrinoUpdate("MERGE INTO delta.default." + tableName1 + " t USING delta.default." + tableName2 + " s " +
                    "ON (t.a = s.a) " +
                    "WHEN MATCHED " +
                    "THEN UPDATE SET a = 3 " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (a) VALUES (3)"))
                    .hasMessageContaining("Check constraint violation");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName1))
                    .containsOnly(
                            row(2),
                            row(2));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName1);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName2);
        }
    }

    @Test
    void testCheckConstraintWriterFeature(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_check_constraint_writer_feature_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(a INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.minWriterVersion'='7')");
        env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (a > 1)");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 2");
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1"))
                    .hasMessageContaining("Check constraint violation");

            // delta.feature.checkConstraints still exists even after unsetting the property
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.feature.checkConstraints')");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.checkConstraints", "supported"))
                    .contains(entry("delta.constraints.a_constraint", "a > 1"));

            // Remove the constraint directly
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.constraints.a_constraint')");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.checkConstraints", "supported"))
                    .doesNotContainKey("delta.constraints.a_constraint");

            // CHECK constraints shouldn't be enforced after the constraint property was removed
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 3");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES 4");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2), row(3), row(4));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testCheckConstraintUnknownCondition(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_check_constraint_unknown_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(a INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (a > 1)");

        try {
            // Values which produces unknown conditions are treated as FALSE by DELTA specification and as TRUE by Trino according to SQL standard
            // https://github.com/delta-io/delta/issues/1714
            assertSparkCheckConstraintViolation(() -> env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (null)"));

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (null)");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row((Object) null));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testCheckConstraintAcrossColumns(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_check_constraint_across_columns_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(a INT, b INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (a = b)");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 1)");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1));

            assertSparkCheckConstraintViolation(() -> env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 2)"));
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Check constraint violation");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testMetadataOperationsRetainCheckConstraint(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_metadata_operations_retain_check_constraints_" + randomNameSuffix();
        env.executeSparkUpdate("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT aIsPositive CHECK (a > 0)");
        try {
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".c IS 'example column comment'");
            env.executeTrinoUpdate("COMMENT ON TABLE delta.default." + tableName + " IS 'example table comment'");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testUnsupportedCheckConstraintExpression(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_unsupported_check_constraints_" + randomNameSuffix();
        env.executeSparkUpdate("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // This constraint should be changed to a new one if the connector supports the expression
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD CONSTRAINT test_constraint CHECK (a = abs(b))");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, -1), (2, -2)");

            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, -1)"))
                    .hasMessageContaining("Failed to convert Delta check constraints to Trino expression");
            assertThatThrownBy(() -> env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET a = -1"))
                    .hasMessageContaining("Failed to convert Delta check constraints to Trino expression");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a = 2");
            assertThatThrownBy(() -> env.executeTrinoUpdate("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = -1"))
                    .hasMessageContaining("Failed to convert Delta check constraints to Trino expression");

            // Verify these operations succeed even if check constraints have unsupported expressions
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".c IS 'example column comment'");
            env.executeTrinoUpdate("COMMENT ON TABLE delta.default." + tableName + " IS 'example table comment'");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, -1, null));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    private static void assertSparkCheckConstraintViolation(Runnable sparkOperation)
    {
        assertThatThrownBy(sparkOperation::run)
                .hasStackTraceContaining("CHECK constraint")
                .hasStackTraceContaining("violated by row with values");
    }

    // Helper method to get table properties from Spark (equivalent to getTablePropertiesOnDelta)
    private static Map<String, String> getTablePropertiesOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        QueryResult result = env.executeSpark("SHOW TBLPROPERTIES %s.%s".formatted(schemaName, tableName));
        return result.getRows().stream()
                .map(row -> Map.entry((String) row.getValue(0), (String) row.getValue(1)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
