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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveSchema.
 * <p>
 * Tests that the Hive connector properly filters out system schemas like 'sys'
 * and presents a consistent view of the 'information_schema'.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveSchema
{
    // Note: this test is run on various Hive versions. Hive before 3 did not have `sys` schema, but it does not hurt to run the test there too.
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSysSchemaFilteredOut(HiveBasicEnvironment env)
    {
        // Setup: make sure hive.default schema is not empty
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_sys_schema_disabled_table_in_default");
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_sys_schema_disabled_table_in_default(a bigint)");

        try {
            // SHOW SCHEMAS
            QueryResult showSchemas = env.executeTrino("SHOW SCHEMAS FROM hive");
            assertThat(showSchemas).satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));
            assertThat(showSchemas).satisfies(result -> assertContainsFirstColumnValue(result, "default"));
            assertThat(showSchemas).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "sys"));

            // SHOW TABLES
            assertThatThrownBy(() -> env.executeTrino("SHOW TABLES FROM hive.sys"))
                    .hasMessageContaining("line 1:1: Schema 'sys' does not exist");

            // SHOW COLUMNS
            assertThatThrownBy(() -> env.executeTrino("SHOW COLUMNS FROM hive.sys.version")) // sys.version exists in Hive 3 and is a view
                    .hasMessageContaining("line 1:1: Schema 'sys' does not exist");
            assertThatThrownBy(() -> env.executeTrino("SHOW COLUMNS FROM hive.sys.table_params")) // sys.table_params exists in Hive 3 and is a table
                    .hasMessageContaining("line 1:1: Schema 'sys' does not exist");

            // DESCRIBE
            assertThatThrownBy(() -> env.executeTrino("DESCRIBE hive.sys.version")) // sys.version exists in Hive 3 and is a view
                    .hasMessageContaining("line 1:1: Schema 'sys' does not exist");
            assertThatThrownBy(() -> env.executeTrino("DESCRIBE hive.sys.table_params")) // sys.table_params exists in Hive 3 and is a table
                    .hasMessageContaining("line 1:1: Schema 'sys' does not exist");

            // information_schema.schemata
            QueryResult schemata = env.executeTrino("SELECT schema_name FROM information_schema.schemata");
            assertThat(schemata).satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));
            assertThat(schemata).satisfies(result -> assertContainsFirstColumnValue(result, "default"));
            assertThat(schemata).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "sys"));

            // information_schema.tables
            QueryResult distinctTableSchema = env.executeTrino("SELECT DISTINCT table_schema FROM information_schema.tables");
            assertThat(distinctTableSchema).satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));
            assertThat(distinctTableSchema).satisfies(result -> assertContainsFirstColumnValue(result, "default"));
            assertThat(distinctTableSchema).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "sys"));
            assertThat(env.executeTrino("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sys'"))
                    .hasNoRows();
            assertThat(env.executeTrino("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sys' AND table_name = 'version'")) // sys.version exists in Hive 3
                    .hasNoRows();

            // information_schema.columns -- it has a special handling path in metadata, which also depends on query predicates
            QueryResult distinctColumnTableSchema = env.executeTrino("SELECT DISTINCT table_schema FROM information_schema.columns");
            assertThat(distinctColumnTableSchema).satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));
            assertThat(distinctColumnTableSchema).satisfies(result -> assertContainsFirstColumnValue(result, "default"));
            assertThat(distinctColumnTableSchema).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "sys"));
            assertThat(env.executeTrino("SELECT table_name FROM information_schema.columns WHERE table_schema = 'sys'"))
                    .hasNoRows();
            assertThat(env.executeTrino("SELECT column_name FROM information_schema.columns WHERE table_schema = 'sys' AND table_name = 'version'")) // sys.version exists in Hive 3
                    .hasNoRows();

            // information_schema.table_privileges -- it has a special handling path in metadata, which also depends on query predicates
            if (tablePrivilegesSupported(env)) {
                QueryResult distinctPrivilegesTableSchema = env.executeTrino("SELECT DISTINCT table_schema FROM information_schema.table_privileges");
                assertThat(distinctPrivilegesTableSchema).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "information_schema"));
                assertThat(distinctPrivilegesTableSchema).satisfies(result -> assertContainsFirstColumnValue(result, "default"));
                assertThat(distinctPrivilegesTableSchema).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "sys"));
                assertThat(env.executeTrino("SELECT table_name FROM information_schema.table_privileges WHERE table_schema = 'sys'"))
                        .hasNoRows();
                assertThat(env.executeTrino("SELECT table_name FROM information_schema.table_privileges WHERE table_schema = 'sys' AND table_name = 'version'")) // sys.version exists in Hive 3
                        .hasNoRows();
            }

            // SELECT
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.sys.version")) // sys.version exists in Hive 3 and is a view
                    .hasMessageContaining("line 1:15: Schema 'sys' does not exist");
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.sys.table_params")) // sys.table_params exists in Hive 3 and is a table
                    .hasMessageContaining("line 1:15: Schema 'sys' does not exist");
        }
        finally {
            // Cleanup
            env.executeTrinoUpdate("DROP TABLE hive.default.test_sys_schema_disabled_table_in_default");
        }
    }

    // Note: this test is run on various Hive versions. Hive before 3 did not have `information_schema` schema, but it does not hurt to run the test there too.
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveInformationSchemaFilteredOut(HiveBasicEnvironment env)
    {
        // Setup: make sure hive.default schema is not empty
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_sys_schema_disabled_table_in_default");
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_sys_schema_disabled_table_in_default(a bigint)");

        try {
            List<String> allInformationSchemaTables = ImmutableList.<String>builder()
                    // In particular, no column_privileges which exists in Hive 3's information_schema
                    .add("columns")
                    .add("tables")
                    .add("views")
                    .add("schemata")
                    .add("table_privileges")
                    .add("roles")
                    .add("applicable_roles")
                    .add("enabled_roles")
                    .build();
            List<Row> allInformationSchemaTablesAsRows = allInformationSchemaTables.stream()
                    .map(name -> row(name))
                    .collect(toImmutableList());

            // This test is run in various setups and we may or may not have access to hive.information_schema.roles table
            List<String> allInformationSchemaTablesExceptRoles = allInformationSchemaTables.stream()
                    .filter(tableName -> !tableName.equals("roles"))
                    .collect(toImmutableList());
            List<Row> allInformationSchemaTablesExceptRolesAsRows = allInformationSchemaTablesExceptRoles.stream()
                    .map(name -> row(name))
                    .collect(toImmutableList());

            // SHOW SCHEMAS
            assertThat(env.executeTrino("SHOW SCHEMAS FROM hive"))
                    .satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));

            // SHOW TABLES
            QueryResult showTables = env.executeTrino("SHOW TABLES FROM hive.information_schema");
            assertThat(showTables).satisfies(result -> assertContainsFirstColumnValue(result, "tables"));
            assertThat(showTables).satisfies(result -> assertContainsFirstColumnValue(result, "columns"));
            assertThat(showTables).satisfies(result -> assertContainsFirstColumnValue(result, "table_privileges"));
            assertThat(showTables).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "column_privileges")); // Hive 3's information_schema has column_privileges view

            // SHOW COLUMNS
            QueryResult showColumnsInfoSchema = env.executeTrino("SHOW COLUMNS FROM hive.information_schema.columns");
            assertThat(showColumnsInfoSchema).satisfies(result -> assertContainsFirstColumnValue(result, "table_catalog"));
            assertThat(showColumnsInfoSchema).satisfies(result -> assertContainsFirstColumnValue(result, "table_schema"));
            assertThat(showColumnsInfoSchema).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "is_updatable")); // Hive 3's information_schema.columns has is_updatable column

            assertThatThrownBy(() -> env.executeTrino("SHOW COLUMNS FROM hive.information_schema.column_privileges")) // Hive 3's information_schema has column_privileges view
                    .hasMessageContaining("line 1:1: Table 'hive.information_schema.column_privileges' does not exist");

            // DESCRIBE
            QueryResult describeColumns = env.executeTrino("DESCRIBE hive.information_schema.columns");
            assertThat(describeColumns).satisfies(result -> assertContainsFirstColumnValue(result, "table_catalog"));
            assertThat(describeColumns).satisfies(result -> assertContainsFirstColumnValue(result, "table_schema"));
            assertThat(describeColumns).satisfies(result -> assertContainsFirstColumnValue(result, "column_name"));
            assertThat(describeColumns).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "is_updatable")); // Hive 3's information_schema.columns has is_updatable column

            assertThatThrownBy(() -> env.executeTrino("DESCRIBE hive.information_schema.column_privileges")) // Hive 3's information_schema has column_privileges view
                    .hasMessageContaining("line 1:1: Table 'hive.information_schema.column_privileges' does not exist");

            // information_schema.schemata
            assertThat(env.executeTrino("SELECT schema_name FROM information_schema.schemata"))
                    .satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));

            // information_schema.tables
            assertThat(env.executeTrino("SELECT DISTINCT table_schema FROM information_schema.tables"))
                    .satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));
            assertThat(env.executeTrino("SELECT table_name FROM information_schema.tables WHERE table_schema = 'information_schema'"))
                    .containsOnly(allInformationSchemaTablesAsRows);
            QueryResult tablesResult = env.executeTrino("SELECT table_schema, table_name FROM information_schema.tables");
            List<String> informationSchemaTablesFound = tablesResult.rows().stream()
                    .filter(row -> "information_schema".equals(row.get(0)))
                    .map(row -> (String) row.get(1))
                    .toList();
            assertThat(informationSchemaTablesFound)
                    .containsOnly(allInformationSchemaTables.toArray(new String[0]));
            // information_schema.column_privileges exists in Hive 3
            assertThat(env.executeTrino("SELECT table_name FROM information_schema.tables WHERE table_schema = 'information_schema' AND table_name = 'column_privileges'"))
                    .hasNoRows();

            // information_schema.columns -- it has a special handling path in metadata, which also depends on query predicates
            assertThat(env.executeTrino("SELECT DISTINCT table_schema FROM information_schema.columns"))
                    .satisfies(result -> assertContainsFirstColumnValue(result, "information_schema"));
            assertThat(env.executeTrino("SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'information_schema' AND table_name != 'roles'"))
                    .containsOnly(allInformationSchemaTablesExceptRolesAsRows);
            QueryResult columnsResult = env.executeTrino("SELECT table_schema, table_name, column_name FROM information_schema.columns");
            List<String> informationSchemaColumnsTablesFound = columnsResult.rows().stream()
                    .filter(row -> "information_schema".equals(row.get(0)))
                    .map(row -> (String) row.get(1))
                    .filter(tableName -> !tableName.equals("roles"))
                    .distinct()
                    .toList();
            assertThat(informationSchemaColumnsTablesFound)
                    .containsOnly(allInformationSchemaTablesExceptRoles.toArray(new String[0]));
            assertThat(env.executeTrino("SELECT column_name FROM information_schema.columns WHERE table_schema = 'information_schema' AND table_name = 'columns'"))
                    .containsOnly(
                            // In particular, no is_updatable column which exists in Hive 3's information_schema.columns
                            row("table_catalog"),
                            row("table_schema"),
                            row("table_name"),
                            row("column_name"),
                            row("ordinal_position"),
                            row("column_default"),
                            row("is_nullable"),
                            row("data_type"));
            // information_schema.column_privileges exists in Hive 3
            assertThat(env.executeTrino("SELECT column_name FROM information_schema.columns WHERE table_schema = 'information_schema' AND table_name = 'column_privileges'"))
                    .hasNoRows();

            // information_schema.table_privileges -- it has a special handling path in metadata, which also depends on query predicates
            if (tablePrivilegesSupported(env)) {
                QueryResult distinctPrivilegesTableSchema = env.executeTrino("SELECT DISTINCT table_schema FROM information_schema.table_privileges");
                assertThat(distinctPrivilegesTableSchema).satisfies(result -> assertContainsFirstColumnValue(result, "default"));
                assertThat(distinctPrivilegesTableSchema).satisfies(result -> assertDoesNotContainFirstColumnValue(result, "information_schema")); // tables in information_schema have no privileges
                assertThat(env.executeTrino("SELECT table_name FROM information_schema.table_privileges WHERE table_schema = 'information_schema'"))
                        .hasNoRows(); // tables in information_schema have no privileges
                QueryResult privilegesResult = env.executeTrino("SELECT table_schema, table_name, privilege_type FROM information_schema.table_privileges");
                List<String> informationSchemaPrivilegesTablesFound = privilegesResult.rows().stream()
                        .filter(row -> "information_schema".equals(row.get(0)))
                        .map(row -> (String) row.get(1))
                        .toList();
                assertThat(informationSchemaPrivilegesTablesFound)
                        .isEmpty(); // tables in information_schema have no privileges
                assertThat(env.executeTrino("SELECT table_name FROM information_schema.table_privileges WHERE table_schema = 'information_schema' AND table_name = 'columns'"))
                        .hasNoRows();
                // information_schema.column_privileges exists in Hive 3
                assertThat(env.executeTrino("SELECT table_name FROM information_schema.table_privileges WHERE table_schema = 'information_schema' AND table_name = 'column_privileges'"))
                        .hasNoRows();
            }

            // SELECT
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.information_schema.column_privileges"))  // information_schema.column_privileges exists in Hive 3
                    .hasMessageContaining("line 1:15: Table 'hive.information_schema.column_privileges' does not exist");
        }
        finally {
            // Cleanup
            env.executeTrinoUpdate("DROP TABLE hive.default.test_sys_schema_disabled_table_in_default");
        }
    }

    /**
     * Returns whether table privileges are supported in current setup.
     */
    private static boolean tablePrivilegesSupported(HiveBasicEnvironment env)
    {
        try {
            env.executeTrino("SELECT * FROM information_schema.table_privileges");
            return true;
        }
        catch (RuntimeException e) {
            if (nullToEmpty(e.getMessage()).endsWith(": This connector does not support table privileges")) {
                return false;
            }
            // Check nested cause for SQLException
            Throwable cause = e.getCause();
            if (cause instanceof SQLException && nullToEmpty(cause.getMessage()).endsWith(": This connector does not support table privileges")) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Asserts that the first column of the query result contains the given value.
     */
    private static void assertContainsFirstColumnValue(QueryResult queryResult, Object value)
    {
        requireNonNull(value, "value is null");
        List<Object> values = queryResult.column(1);
        if (!values.isEmpty()) {
            // When contains() is used in a negative context, it could be possible to get false-positives when types are wrong.
            Class<?> expectedType = value.getClass();
            Class<?> actualType = values.get(0).getClass();
            verify(expectedType.equals(actualType), "Expected QueryResult to contain %s values, but it contains %s", expectedType, actualType);
        }
        assertThat(values)
                .as("First column should contain value '%s'", value)
                .contains(value);
    }

    /**
     * Asserts that the first column of the query result does not contain the given value.
     */
    private static void assertDoesNotContainFirstColumnValue(QueryResult queryResult, Object value)
    {
        requireNonNull(value, "value is null");
        List<Object> values = queryResult.column(1);
        if (!values.isEmpty()) {
            // When contains() is used in a negative context, it could be possible to get false-positives when types are wrong.
            Class<?> expectedType = value.getClass();
            Class<?> actualType = values.get(0).getClass();
            verify(expectedType.equals(actualType), "Expected QueryResult to contain %s values, but it contains %s", expectedType, actualType);
        }
        assertThat(values)
                .as("First column should not contain value '%s'", value)
                .doesNotContain(value);
    }
}
