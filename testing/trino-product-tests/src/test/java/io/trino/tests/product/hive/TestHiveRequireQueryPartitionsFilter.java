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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveRequireQueryPartitionsFilter.
 * <p>
 * Tests the Hive connector's query_partition_filter_required session property,
 * which enforces that queries on partitioned tables include a filter on at least
 * one partition column.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveRequireQueryPartitionsFilter
{
    @Test
    void testRequiresQueryPartitionFilter(HiveBasicEnvironment env)
    {
        String tableName = "test_partition_filter_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            // Enable require partition filter
            env.executeTrinoUpdate("SET SESSION hive.query_partition_filter_required = true");

            // Query without partition filter should fail
            assertThatThrownBy(() -> env.executeTrino("SELECT COUNT(*) FROM hive.default." + tableName))
                    .hasMessageMatching(format(".*Filter required on default\\.%s for at least one partition column: p_regionkey.*", tableName));

            // Query with partition filter should succeed
            assertThat(env.executeTrino(format("SELECT COUNT(*) FROM hive.default.%s WHERE p_regionkey = 1", tableName)))
                    .containsOnly(row(5L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    static Stream<String> queryPartitionFilterRequiredSchemasDataProvider()
    {
        return Stream.of(
                "ARRAY['default']",
                "ARRAY['DEFAULT']",
                "ARRAY['deFAUlt']");
    }

    @ParameterizedTest(name = "schemas={0}")
    @MethodSource("queryPartitionFilterRequiredSchemasDataProvider")
    void testRequiresQueryPartitionFilterOnSpecificSchema(String queryPartitionFilterRequiredSchemas, HiveBasicEnvironment env)
    {
        String tableName = "test_partition_filter_schema_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            // Enable require partition filter
            env.executeTrinoUpdate("SET SESSION hive.query_partition_filter_required = true");
            env.executeTrinoUpdate(format("SET SESSION hive.query_partition_filter_required_schemas = %s", queryPartitionFilterRequiredSchemas));

            // Query without partition filter should fail
            assertThatThrownBy(() -> env.executeTrino("SELECT COUNT(*) FROM hive.default." + tableName))
                    .hasMessageMatching(format(".*Filter required on default\\.%s for at least one partition column: p_regionkey.*", tableName));

            // Query with partition filter should succeed
            assertThat(env.executeTrino(format("SELECT COUNT(*) FROM hive.default.%s WHERE p_regionkey = 1", tableName)))
                    .containsOnly(row(5L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    /**
     * Creates a partitioned table similar to NATION_PARTITIONED_BY_BIGINT_REGIONKEY.
     * <p>
     * The table has columns: p_nationkey, p_name, p_comment, and is partitioned by p_regionkey.
     * Data is inserted to create partitions with p_regionkey values 1, 2, and 3,
     * with 5 rows per partition.
     */
    private void createPartitionedTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default." + tableName + " (" +
                        "p_nationkey BIGINT, " +
                        "p_name VARCHAR(25), " +
                        "p_comment VARCHAR(152), " +
                        "p_regionkey BIGINT) " +
                        "WITH (format = 'ORC', partitioned_by = ARRAY['p_regionkey'])");

        // Insert data similar to the nation_partitioned table - 5 rows per partition
        // Region 1
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES " +
                "(0, 'ALGERIA', 'special requests against the special', 1), " +
                "(1, 'ARGENTINA', 'carefully final requests', 1), " +
                "(2, 'BRAZIL', 'pending packages', 1), " +
                "(3, 'CANADA', 'special deposits', 1), " +
                "(4, 'EGYPT', 'unusual instructions', 1)");
        // Region 2
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES " +
                "(5, 'ETHIOPIA', 'platelets wake', 2), " +
                "(6, 'FRANCE', 'final theodolites', 2), " +
                "(7, 'GERMANY', 'blithely final requests', 2), " +
                "(8, 'INDIA', 'excuses among the accounts', 2), " +
                "(9, 'INDONESIA', 'special dependencies', 2)");
        // Region 3
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES " +
                "(10, 'IRAN', 'furiously final requests', 3), " +
                "(11, 'IRAQ', 'pending packages wake', 3), " +
                "(12, 'JAPAN', 'carefully express deposits', 3), " +
                "(13, 'JORDAN', 'express deposits', 3), " +
                "(14, 'KENYA', 'pending theodolites', 3)");
    }
}
