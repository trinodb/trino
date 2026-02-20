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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestHiveMaterializedView
{
    @BeforeEach
    void setUp(HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("CREATE TABLE test_materialized_view_table(x string) " +
                "STORED AS ORC " +
                "TBLPROPERTIES('transactional'='true')");
        env.executeHiveUpdate("INSERT INTO test_materialized_view_table VALUES ('a'), ('a'), ('b')");
    }

    @AfterEach
    void tearDown(HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_materialized_view_table");
    }

    @Test
    void testMaterializedView(HiveStorageFormatsEnvironment env)
    {
        testMaterializedViewInternal(false, env);
    }

    @Test
    void testPartitionedMaterializedView(HiveStorageFormatsEnvironment env)
    {
        testMaterializedViewInternal(true, env);
    }

    private void testMaterializedViewInternal(boolean partitioned, HiveStorageFormatsEnvironment env)
    {
        // Clean up any existing view
        try {
            env.executeHiveUpdate("DROP MATERIALIZED VIEW test_materialized_view_view");
        }
        catch (RuntimeException _) {
            // Ignore if view doesn't exist
        }

        String createStatement = "CREATE MATERIALIZED VIEW test_materialized_view_view " +
                (partitioned ? "PARTITIONED ON (x) " : "") +
                "STORED AS ORC " +
                "AS SELECT x, count(*) c FROM test_materialized_view_table GROUP BY x";

        // Apache Hive 3.1 parser does not support PARTITIONED materialized views.
        if (partitioned) {
            assertThatThrownBy(() -> env.executeHiveUpdate(createStatement))
                    .satisfies(throwable -> assertThat(throwable.getCause())
                            .isNotNull()
                            .extracting(Throwable::getMessage, InstanceOfAssertFactories.STRING)
                            .contains("mismatched input 'PARTITIONED' expecting AS"));
            return;
        }

        env.executeHiveUpdate(createStatement);

        try {
            // Test metadata - SHOW TABLES should include both the table and view
            assertThat(env.executeTrino("SHOW TABLES"))
                    .contains(
                            row("test_materialized_view_table"),
                            row("test_materialized_view_view"));

            // Test SHOW COLUMNS - column order and partition key status
            assertThat(env.executeTrino("SHOW COLUMNS FROM test_materialized_view_view"))
                    .containsOnly(
                            row("c", "bigint", "", ""),
                            row("x", "varchar", partitioned ? "partition key" : "", ""));

            // Test reading data from the materialized view
            assertThat(env.executeTrino("SELECT x, c FROM test_materialized_view_view"))
                    .containsOnly(row("a", 2L), row("b", 1L));

            // Test reading with a predicate
            assertThat(env.executeTrino("SELECT x, c FROM test_materialized_view_view WHERE x = 'a'"))
                    .containsOnly(row("a", 2L));

            // Test that writing to the materialized view is not allowed
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO test_materialized_view_view(x, c) VALUES ('x', 42)"))
                    .hasMessageContaining("Cannot write to Hive materialized view");
        }
        finally {
            // Clean up the materialized view
            try {
                env.executeHiveUpdate("DROP MATERIALIZED VIEW test_materialized_view_view");
            }
            catch (RuntimeException _) {
                // Ignore cleanup errors
            }
        }
    }
}
