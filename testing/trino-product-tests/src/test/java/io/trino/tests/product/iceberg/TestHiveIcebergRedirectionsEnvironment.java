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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.hive.HiveIcebergRedirectionsEnvironment;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Verification tests for the HiveIcebergRedirectionsEnvironment.
 * <p>
 * These tests verify:
 * <ul>
 *   <li>Basic Trino connectivity</li>
 *   <li>Hive catalog functionality</li>
 *   <li>Iceberg catalog functionality</li>
 *   <li>Hive to Iceberg table redirection</li>
 *   <li>Iceberg to Hive table redirection</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
@TestGroup.HiveIcebergRedirections
class TestHiveIcebergRedirectionsEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveIcebergRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalog(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.redirect_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.redirect_test.hive_table (x int) WITH (format = 'PARQUET')");
            env.executeTrinoUpdate("INSERT INTO hive.redirect_test.hive_table VALUES (1)");
            assertThat(env.executeTrino("SELECT * FROM hive.redirect_test.hive_table"))
                    .containsOnly(row(1));
            env.executeTrinoUpdate("DROP TABLE hive.redirect_test.hive_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive.redirect_test");
        }
    }

    @Test
    void verifyIcebergCatalog(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.redirect_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.redirect_test.iceberg_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.redirect_test.iceberg_table VALUES (2)");
            assertThat(env.executeTrino("SELECT * FROM iceberg.redirect_test.iceberg_table"))
                    .containsOnly(row(2));
            env.executeTrinoUpdate("DROP TABLE iceberg.redirect_test.iceberg_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.redirect_test");
        }
    }

    @Test
    void verifyHiveToIcebergRedirection(HiveIcebergRedirectionsEnvironment env)
    {
        // Create Iceberg table and query it via Hive catalog (should redirect)
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.redirect_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.redirect_test.ice_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.redirect_test.ice_table VALUES (10)");

            // Query via hive catalog should redirect to iceberg
            assertThat(env.executeTrino("SELECT * FROM hive.redirect_test.ice_table"))
                    .containsOnly(row(10));

            env.executeTrinoUpdate("DROP TABLE iceberg.redirect_test.ice_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.redirect_test");
        }
    }

    @Test
    void verifyIcebergToHiveRedirection(HiveIcebergRedirectionsEnvironment env)
    {
        // Create Hive table and query it via Iceberg catalog (should redirect)
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.redirect_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.redirect_test.hive_table (x int) WITH (format = 'PARQUET')");
            env.executeTrinoUpdate("INSERT INTO hive.redirect_test.hive_table VALUES (20)");

            // Query via iceberg catalog should redirect to hive
            assertThat(env.executeTrino("SELECT * FROM iceberg.redirect_test.hive_table"))
                    .containsOnly(row(20));

            env.executeTrinoUpdate("DROP TABLE hive.redirect_test.hive_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive.redirect_test");
        }
    }
}
