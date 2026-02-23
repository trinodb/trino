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
 * Verification tests for the HiveBasicEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive Metastore</li>
 *   <li>Trino can read/write to HDFS</li>
 *   <li>HiveServer2 is accessible for Hive interoperability testing</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
class TestHiveBasicEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyTpchCatalogExists(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyHiveDefaultSchema(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_" + randomNameSuffix();
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
    void verifyCreateSchema(HiveBasicEnvironment env)
    {
        String schemaName = "test_schema_" + randomNameSuffix();

        try {
            // Create schema (requires Hive Metastore access)
            env.executeTrinoUpdate("CREATE SCHEMA hive." + schemaName);

            // Verify schema exists
            assertThat(env.executeTrino("SHOW SCHEMAS FROM hive LIKE '" + schemaName + "'"))
                    .containsOnly(row(schemaName));
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @Test
    void verifyHiveInteroperability(HiveBasicEnvironment env)
    {
        String tableName = "test_hive_interop_" + randomNameSuffix();

        try {
            // Create table and insert data using Hive (via HiveServer2)
            env.executeHiveUpdate("CREATE TABLE default." + tableName + " (id INT, name STRING)");
            env.executeHiveUpdate("INSERT INTO default." + tableName + " VALUES (1, 'alice'), (2, 'bob')");

            // Read the data from Trino - verifies Hive/Trino interoperability
            assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"));

            // Verify row count
            assertThat(env.executeTrino("SELECT count(*) FROM hive.default." + tableName))
                    .containsOnly(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
