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
 * Verification tests for the HiveStorageFormatsEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive Metastore</li>
 *   <li>Trino can read/write to HDFS</li>
 *   <li>Different storage formats (ORC, Parquet) work correctly</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
class TestHiveStorageFormatsEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyTpchCatalogExists(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyHiveDefaultSchema(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_basic_" + randomNameSuffix();
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
    void verifyOrcFormat(HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_orc_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create ORC table
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, name varchar) WITH (format = 'ORC')");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (1, 'alice'), (2, 'bob')");

            // Read data
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyParquetFormat(HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_parquet_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create Parquet table
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, name varchar) WITH (format = 'PARQUET')");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (1, 'alice'), (2, 'bob')");

            // Read data
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }
}
