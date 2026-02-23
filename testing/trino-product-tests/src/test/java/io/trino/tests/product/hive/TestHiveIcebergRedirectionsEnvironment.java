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
 * Verification tests for the HiveIcebergRedirectionsEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop and Trino containers start correctly</li>
 *   <li>Both Hive and Iceberg catalogs are available</li>
 *   <li>Tables can be created and read through both catalogs</li>
 *   <li>Hive-to-Iceberg table redirections are properly configured</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
class TestHiveIcebergRedirectionsEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyIcebergCatalogExists(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'iceberg'")).containsOnly(row("iceberg"));
    }

    @Test
    void verifyHiveDefaultSchema(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadHiveTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "test_hive_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create a regular Hive table
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int) WITH (format = 'ORC')");

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
    void verifyCreateAndReadIcebergTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "test_iceberg_" + randomNameSuffix();
        String fullTableName = "iceberg.default." + tableName;

        try {
            // Create an Iceberg table via the iceberg catalog
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (99)");

            // Read data
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(99));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyTpchCatalogExists(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyHiveToIcebergRedirection(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "test_redirection_" + randomNameSuffix();

        try {
            // Create table via Iceberg catalog
            env.executeTrinoUpdate("CREATE TABLE iceberg.default." + tableName + " (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.default." + tableName + " VALUES (42)");

            // Query via Hive catalog - should redirect to Iceberg
            assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default." + tableName);
        }
    }
}
