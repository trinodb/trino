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
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the HiveHudiRedirectionsEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop, Spark, and Trino containers start correctly</li>
 *   <li>Both Hive and Hudi catalogs are available in Trino</li>
 *   <li>Trino can connect to the Hive Metastore</li>
 *   <li>Basic table operations work through the Hive catalog</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveHudiRedirectionsEnvironment.class)
class TestHiveHudiRedirectionsEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveHudiRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveHudiRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHudiCatalogExists(HiveHudiRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hudi'")).containsOnly(row("hudi"));
    }

    @Test
    void verifyHiveDefaultSchema(HiveHudiRedirectionsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "test_hive_hudi_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table via Trino Hive catalog
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
    void verifyS3Storage(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "test_s3_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table - should use S3/MinIO storage
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Verify data readable
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));

            // Verify warehouse path is S3
            String warehousePath = env.getWarehousePath();
            assertThat(warehousePath).startsWith("s3a://");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }
}
