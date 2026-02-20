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
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Delta Lake case insensitive column mapping.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeCaseInsensitiveMapping
{
    private static final double ONE_THIRD = 1.0 / 3;

    @Test
    void testNonLowercaseColumnNames(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_non_lowercase_column" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE_INT INT, Camel_Case_String STRING, PART INT)" +
                "USING delta " +
                "PARTITIONED BY (PART)" +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'a', 10)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (2, 'ab', 20)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (null, null, null)");

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("upper_case_int", null, 1.0, ONE_THIRD, null, "1", "2"),
                            row("camel_case_string", 2.0, 1.0, ONE_THIRD, null, null, null),
                            row("part", null, 2.0, ONE_THIRD, null, null, null),
                            row(null, null, null, null, 3.0, null, null));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testNonLowercaseFieldNames(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_non_lowercase_field" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(id int, UPPER_CASE STRUCT<UPPER_FIELD: string>, Mixed_Case struct<Mixed_Nested: struct<Mixed_Field: string>>)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " SELECT 1, row('test uppercase'), row(row('test mixedcase'))");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_case", null, null, null, null, null, null),
                            row("mixed_case", null, null, null, null, null, null),
                            row(null, null, null, null, 1.0, null, null));

            // Specify field names to test projection pushdown
            List<Row> expectedRows = ImmutableList.of(row(1, "test uppercase", "test mixedcase"));
            assertThat(env.executeSpark("SELECT id, upper_case.upper_field, mixed_case.mixed_nested.mixed_field FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, upper_case.upper_field, mixed_case.mixed_nested.mixed_field FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(env.executeTrino("SELECT id FROM delta.default." + tableName + " WHERE upper_case.upper_field = 'test uppercase'"))
                    .containsOnly(row(1));
            assertThat(env.executeTrino("SELECT id FROM delta.default." + tableName + " WHERE mixed_case.mixed_nested.mixed_field = 'test mixedcase'"))
                    .containsOnly(row(1));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testColumnCommentWithNonLowerCaseColumnName(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_comment_uppercase_name" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT COMMENT 'test column comment')" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "upper_case")).isEqualTo("test column comment");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "UPPER_CASE")).isEqualTo("test column comment");

            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test updated comment'");

            assertThat(getColumnCommentOnTrino(env, "default", tableName, "upper_case")).isEqualTo("test updated comment");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "UPPER_CASE")).isEqualTo("test updated comment");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testNotNullColumnWithNonLowerCaseColumnName(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_notnull_column_uppercase_name" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT NOT NULL)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // Verify column operation doesn't delete NOT NULL constraint
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test comment'");

            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES NULL"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    private String getColumnCommentOnTrino(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeTrino("SELECT comment FROM delta.information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    private String getColumnCommentOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeSpark(String.format("DESCRIBE %s.%s %s", schemaName, tableName, columnName))
                .rows()
                .get(2)
                .get(1);
    }
}
