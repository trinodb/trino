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
 * Verification tests for the HiveSparkNoStatsFallbackEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop, Spark, and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive Metastore with stats fallback disabled</li>
 *   <li>Trino can read/write tables to HDFS</li>
 *   <li>Spark-created tables can be read by Trino</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveSparkNoStatsFallbackEnvironment.class)
class TestHiveSparkNoStatsFallbackEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveSparkNoStatsFallbackEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveSparkNoStatsFallbackEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyTpchCatalogExists(HiveSparkNoStatsFallbackEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(HiveSparkNoStatsFallbackEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyHiveDefaultSchema(HiveSparkNoStatsFallbackEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(HiveSparkNoStatsFallbackEnvironment env)
    {
        String tableName = "test_no_stats_fallback_" + randomNameSuffix();
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
    void verifySparkCreateTableTrinoRead(HiveSparkNoStatsFallbackEnvironment env)
    {
        String tableName = "test_spark_trino_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table in Spark
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (id INT, name STRING) USING PARQUET");

            // Insert data via Spark
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'alice'), (2, 'bob')");

            // Read data from Trino
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
