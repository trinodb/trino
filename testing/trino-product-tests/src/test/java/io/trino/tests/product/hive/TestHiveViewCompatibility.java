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
import io.trino.testing.containers.environment.QueryResultAssert;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveViewCompatibility.
 * <p>
 * Tests Hive catalog view compatibility behavior through Trino.
 * <p>
 * Legacy Tempto used two Trino executors (onTrino and compatibility-test-server).
 * The JUnit lane keeps equivalent Trino-side coverage in HiveSparkEnvironment.
 */
@ProductTest
@RequiresEnvironment(HiveSparkEnvironment.class)
@TestGroup.HiveSpark
@TestGroup.HiveViewCompatibility
class TestHiveViewCompatibility
{
    private static final String SOURCE_TABLE = "hive_view_compat_source";

    @Test
    void testSelectOnView(HiveSparkEnvironment env)
    {
        prepareSourceTable(env);
        try {
            env.executeCompatibilityTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_test_view");
            env.executeCompatibilityTrinoUpdate("CREATE VIEW hive.default.hive_test_view AS SELECT * FROM hive.default." + SOURCE_TABLE);

            assertViewQuery(env, "SELECT * FROM hive.default.hive_test_view", queryAssert -> queryAssert.hasRowsCount(25));
            assertViewQuery(
                    env,
                    "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM hive.default.hive_test_view WHERE n_nationkey < 3",
                    queryAssert -> queryAssert.containsOnly(
                            row(0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"),
                            row(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"),
                            row(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ")));
        }
        finally {
            env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_test_view");
            cleanupSourceTable(env);
        }
    }

    @Test
    void testSelectOnViewFromDifferentSchema(HiveSparkEnvironment env)
    {
        prepareSourceTable(env);
        try {
            env.executeHiveUpdate("DROP DATABASE IF EXISTS test_schema CASCADE");
            env.executeHiveUpdate("CREATE DATABASE test_schema");
            env.executeCompatibilityTrinoUpdate(
                    "CREATE VIEW hive.test_schema.hive_test_view_1 AS SELECT * FROM " +
                            "hive.default." + SOURCE_TABLE);

            assertViewQuery(env, "SELECT * FROM hive.test_schema.hive_test_view_1", queryAssert -> queryAssert.hasRowsCount(25));
        }
        finally {
            env.executeHiveUpdate("DROP DATABASE IF EXISTS test_schema CASCADE");
            cleanupSourceTable(env);
        }
    }

    @Test
    void testExistingView(HiveSparkEnvironment env)
    {
        prepareSourceTable(env);
        try {
            env.executeCompatibilityTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_duplicate_view");
            env.executeCompatibilityTrinoUpdate("CREATE VIEW hive.default.hive_duplicate_view AS SELECT * FROM hive.default." + SOURCE_TABLE);

            assertThatThrownBy(() -> env.executeTrinoUpdate("CREATE VIEW hive.default.hive_duplicate_view AS SELECT * FROM hive.default." + SOURCE_TABLE))
                    .hasMessageMatching("(?s).*(View already exists|Hive views are not supported).*");
        }
        finally {
            env.executeCompatibilityTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_duplicate_view");
            cleanupSourceTable(env);
        }
    }

    @Test
    void testCommentOnViewColumn(HiveSparkEnvironment env)
    {
        prepareSourceTable(env);
        try {
            env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_test_view_comment_column");
            env.executeTrinoUpdate("CREATE VIEW hive.default.hive_test_view_comment_column AS SELECT * FROM hive.default." + SOURCE_TABLE);
            env.executeCompatibilityTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_test_view_comment_column_compatibility");
            env.executeCompatibilityTrinoUpdate("CREATE VIEW hive.default.hive_test_view_comment_column_compatibility AS SELECT * FROM hive.default." + SOURCE_TABLE);

            assertViewQuery(env, "SELECT * FROM hive.default.hive_test_view_comment_column", queryAssert -> queryAssert.hasRowsCount(25));
            assertViewQuery(env, "SELECT * FROM hive.default.hive_test_view_comment_column_compatibility", queryAssert -> queryAssert.hasRowsCount(25));

            // Verify that the views are still readable after adding a comment on one of their columns
            env.executeTrinoUpdate("COMMENT ON COLUMN hive.default.hive_test_view_comment_column.n_nationkey IS 'ID of the nation'");
            assertViewQuery(env, "SELECT * FROM hive.default.hive_test_view_comment_column", queryAssert -> queryAssert.hasRowsCount(25));

            env.executeTrinoUpdate("COMMENT ON COLUMN hive.default.hive_test_view_comment_column_compatibility.n_nationkey IS 'ID of the nation'");
            assertViewQuery(env, "SELECT * FROM hive.default.hive_test_view_comment_column_compatibility", queryAssert -> queryAssert.hasRowsCount(25));
        }
        finally {
            env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_test_view_comment_column");
            env.executeCompatibilityTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_test_view_comment_column_compatibility");
            cleanupSourceTable(env);
        }
    }

    /**
     * Assert that a query on a view executes successfully through both Trino coordinators.
     */
    private static void assertViewQuery(HiveSparkEnvironment env, String trinoQuery, Consumer<QueryResultAssert> assertion)
    {
        assertion.accept(assertThat(env.executeTrino(trinoQuery)));
        assertion.accept(assertThat(env.executeCompatibilityTrino(trinoQuery)));
    }

    private static void prepareSourceTable(HiveSparkEnvironment env)
    {
        cleanupSourceTable(env);
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default." + SOURCE_TABLE + " AS " +
                        "SELECT " +
                        "n_nationkey, " +
                        "CASE n_nationkey " +
                        "  WHEN 0 THEN 'ALGERIA' " +
                        "  WHEN 1 THEN 'ARGENTINA' " +
                        "  WHEN 2 THEN 'BRAZIL' " +
                        "  ELSE format('NATION_%s', CAST(n_nationkey AS varchar)) " +
                        "END AS n_name, " +
                        "CASE n_nationkey WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 2 THEN 1 ELSE 0 END AS n_regionkey, " +
                        "CASE n_nationkey " +
                        "  WHEN 0 THEN ' haggle. carefully final deposits detect slyly agai' " +
                        "  WHEN 1 THEN 'al foxes promise slyly according to the regular accounts. bold requests alon' " +
                        "  WHEN 2 THEN 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ' " +
                        "  ELSE format('comment-%s', CAST(n_nationkey AS varchar)) " +
                        "END AS n_comment " +
                        "FROM UNNEST(sequence(0, 24)) t(n_nationkey)");
    }

    private static void cleanupSourceTable(HiveSparkEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + SOURCE_TABLE);
    }
}
