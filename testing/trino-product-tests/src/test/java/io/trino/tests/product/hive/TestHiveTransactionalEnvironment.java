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
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the HiveTransactionalEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive Metastore</li>
 *   <li>Trino can read/write to HDFS</li>
 *   <li>The insert-existing-partitions-behavior=APPEND configuration works correctly</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
class TestHiveTransactionalEnvironment
{
    private static final String TRANSACTIONAL = "transactional";

    @Test
    void verifyTrinoConnectivity(HiveTransactionalEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveTransactionalEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHiveDefaultSchema(HiveTransactionalEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyTpchCatalogExists(HiveTransactionalEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyHiveServer2Connectivity(HiveTransactionalEnvironment env)
    {
        assertThat(env.verifyHiveServer2Connectivity()).isTrue();
    }

    @Test
    void verifyCreateAndReadTable(HiveTransactionalEnvironment env)
    {
        String tableName = "test_transactional_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table (requires Hive Metastore and HDFS access)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (requires HDFS write access)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (requires HDFS read access)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyAppendToExistingPartition(HiveTransactionalEnvironment env)
    {
        String tableName = "test_append_partition_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create a partitioned table
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (value int, part varchar) WITH (partitioned_by = ARRAY['part'])");

            // Insert data into partition 'a'
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " (value, part) VALUES (1, 'a'), (2, 'a')");

            // Verify initial row count
            assertThat(env.executeTrino("SELECT count(*) FROM " + fullTableName + " WHERE part = 'a'"))
                    .containsOnly(row(2L));

            // Insert more data into the same partition 'a' - this should APPEND due to configuration
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " (value, part) VALUES (3, 'a'), (4, 'a')");

            // Verify row count doubled (APPEND behavior means rows are added, not replaced)
            assertThat(env.executeTrino("SELECT count(*) FROM " + fullTableName + " WHERE part = 'a'"))
                    .containsOnly(row(4L));

            // Verify all values are present
            assertThat(env.executeTrino("SELECT value FROM " + fullTableName + " WHERE part = 'a' ORDER BY value"))
                    .containsOnly(
                            row(1),
                            row(2),
                            row(3),
                            row(4));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyCompaction(HiveTransactionalEnvironment env)
    {
        String tableName = "test_compaction_" + randomNameSuffix();

        try {
            // Create ACID table via Hive
            env.executeHiveUpdate("CREATE TABLE default." + tableName +
                    " (id INT, value STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')");

            // Insert data
            env.executeHiveUpdate("INSERT INTO default." + tableName + " VALUES (1, 'a'), (2, 'b')");

            // Run compaction - just verify it doesn't throw
            env.compactTableAndWait("major", "default." + tableName, Duration.ofMinutes(2));

            // Verify data still readable
            assertThat(env.executeTrino("SELECT count(*) FROM hive.default." + tableName))
                    .containsOnly(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testVerifyEnvironmentHiveTransactionalByDefault(HiveTransactionalEnvironment env)
            throws SQLException
    {
        String tableName = "test_hive_transactional_by_default_" + randomNameSuffix();
        env.executeHiveUpdate("CREATE TABLE default." + tableName + "(a bigint) STORED AS ORC");
        try {
            assertThat(getTableProperty(env, "default." + tableName, TRANSACTIONAL))
                    .contains("true");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    private static Optional<String> getTableProperty(HiveTransactionalEnvironment env, String tableName, String propertyName)
            throws SQLException
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(propertyName, "propertyName is null");

        try (Connection connection = env.createHiveConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW TBLPROPERTIES " + tableName)) {
            while (resultSet.next()) {
                if (propertyName.equals(resultSet.getString("prpt_name"))) {
                    return Optional.of(resultSet.getString("prpt_value"));
                }
            }
        }
        return Optional.empty();
    }
}
