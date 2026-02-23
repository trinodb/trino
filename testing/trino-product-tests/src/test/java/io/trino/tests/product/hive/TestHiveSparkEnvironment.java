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

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Verification tests for the HiveSparkEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop, Spark, and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive Metastore</li>
 *   <li>Trino can read/write to HDFS via the Hive connector</li>
 *   <li>Tables created in Spark can be read from Trino</li>
 *   <li>Tables created in Trino can be read from Spark</li>
 *   <li>Tables created in Hive can be read from Trino</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveSparkEnvironment.class)
class TestHiveSparkEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveSparkEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveSparkEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyTpchCatalogExists(HiveSparkEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(HiveSparkEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyHiveDefaultSchema(HiveSparkEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(HiveSparkEnvironment env)
    {
        String tableName = "test_hive_spark_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifySparkCreateTableTrinoRead(HiveSparkEnvironment env)
    {
        String tableName = "test_spark_to_trino_" + randomNameSuffix();

        try {
            // Create and populate table in Spark
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (id INT, name STRING) USING hive");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'alice'), (2, 'bob')");

            // Read the table from Trino
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void verifyTrinoCreateTableSparkRead(HiveSparkEnvironment env)
    {
        String tableName = "test_trino_to_spark_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create and populate table in Trino
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, name varchar)");
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (1, 'charlie'), (2, 'david')");

            // Read the table from Spark
            assertThat(env.executeSpark("SELECT id, name FROM default." + tableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "charlie"),
                            row(2, "david"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyHiveCreateTableTrinoRead(HiveSparkEnvironment env)
    {
        String tableName = "test_hive_to_trino_" + randomNameSuffix();

        try {
            // Create and populate table in Hive
            env.executeHiveUpdate("CREATE TABLE default." + tableName + " (id INT, name STRING)");
            env.executeHiveUpdate("INSERT INTO default." + tableName + " VALUES (1, 'eve'), (2, 'frank')");

            // Read the table from Trino
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "eve"),
                            row(2, "frank"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
