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
 * Verification tests for the HiveKerberosCredentialCacheEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>Trino can authenticate using credential cache instead of keytab</li>
 *   <li>Trino can connect to Kerberos-enabled Hive Metastore</li>
 *   <li>Trino can read/write to Kerberos-enabled HDFS</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveKerberosCredentialCacheEnvironment.class)
class TestHiveKerberosCredentialCacheEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveKerberosCredentialCacheEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveKerberosCredentialCacheEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHiveDefaultSchema(HiveKerberosCredentialCacheEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(HiveKerberosCredentialCacheEnvironment env)
    {
        String tableName = "test_kerberos_credcache_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table (requires HDFS write access with Kerberos via credential cache)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (requires HDFS write access with Kerberos via credential cache)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (requires HDFS read access with Kerberos via credential cache)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyCreateSchema(HiveKerberosCredentialCacheEnvironment env)
    {
        String schemaName = "test_schema_credcache_" + randomNameSuffix();

        try {
            // Create schema (requires Hive Metastore access with Kerberos via credential cache)
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
    void verifyMultipleRowsWithCredentialCache(HiveKerberosCredentialCacheEnvironment env)
    {
        String tableName = "test_multi_row_credcache_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, name varchar)");
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            assertThat(env.executeTrino("SELECT * FROM " + fullTableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"),
                            row(3, "charlie"));

            assertThat(env.executeTrino("SELECT count(*) FROM " + fullTableName))
                    .containsOnly(row(3L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }
}
