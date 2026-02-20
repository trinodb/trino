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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;

/**
 * JUnit 5 port of TestHivePropertiesTable.
 * <p>
 * Tests Hive properties table functionality for Trino and Hive views.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHivePropertiesTable
{
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testTrinoViewPropertiesTable(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_trino_view_properties_base");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS test_trino_view_properties");
        env.executeTrinoUpdate("CREATE TABLE test_trino_view_properties_base (col INT)");
        env.executeTrinoUpdate("CREATE VIEW test_trino_view_properties AS SELECT * FROM test_trino_view_properties_base");

        assertThat(env.executeTrino("SHOW COLUMNS FROM \"test_trino_view_properties$properties\""))
                .containsOnly(
                        row("comment", "varchar", "", ""),
                        row("trino_query_id", "varchar", "", ""),
                        row("trino_version", "varchar", "", ""),
                        row("presto_view", "varchar", "", ""),
                        row("transient_lastddltime", "varchar", "", ""),
                        row("trino_created_by", "varchar", "", ""));

        assertThat(env.executeTrino("SELECT * FROM \"test_trino_view_properties$properties\""))
                .hasRowsCount(1)
                .containsExactlyInOrder(Row.fromList(getTablePropertiesOnHive(env, "test_trino_view_properties")));

        env.executeTrinoUpdate("DROP VIEW IF EXISTS test_trino_view_properties");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_trino_view_properties_base");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveViewPropertiesTable(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_hive_view_properties_base");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS test_hive_view_properties");
        env.executeTrinoUpdate("CREATE TABLE test_hive_view_properties_base (col INT)");
        env.executeHiveUpdate("CREATE VIEW test_hive_view_properties AS SELECT * FROM test_hive_view_properties_base");

        // Use "contains" method because the table properties for Hive views aren't identical among testing environments
        assertThat(env.executeTrino("SHOW COLUMNS FROM \"test_hive_view_properties$properties\""))
                .contains(row("transient_lastddltime", "varchar", "", ""));

        assertThat(env.executeTrino("SELECT * FROM \"test_hive_view_properties$properties\""))
                .hasRowsCount(1)
                .containsExactlyInOrder(Row.fromList(getTablePropertiesOnHive(env, "test_hive_view_properties")));

        env.executeTrinoUpdate("DROP VIEW IF EXISTS test_hive_view_properties");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_hive_view_properties_base");
    }

    private static List<Object> getTablePropertiesOnHive(HiveBasicEnvironment env, String tableName)
    {
        QueryResult result = env.executeHive("SHOW TBLPROPERTIES " + tableName);
        return result.rows().stream()
                .map(row -> row.get(1))
                .collect(toImmutableList());
    }
}
