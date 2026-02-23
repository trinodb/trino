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
import io.trino.testing.minio.MinioClient;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the MultinodeHive4Environment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The MinIO, Hive 4 Metastore, HiveServer2, and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive 4 Metastore</li>
 *   <li>Trino can read/write tables stored in S3 (MinIO)</li>
 *   <li>Interoperability between Hive 4 and Trino works correctly</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(MultinodeHive4Environment.class)
class TestHive4Environment
{
    @Test
    void verifyTrinoConnectivity(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHiveDefaultSchema(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(MultinodeHive4Environment env)
    {
        String tableName = "test_hive4_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table with explicit S3 location (Hive 4 default warehouse uses file:// path)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int) " +
                    "WITH (external_location = '" + env.getWarehousePath() + tableName + "')");

            // Insert data (requires S3/MinIO write access)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (requires S3/MinIO read access)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyS3Storage(MultinodeHive4Environment env)
    {
        String tableName = "test_s3_storage_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table with explicit S3 location
            String warehousePath = env.getWarehousePath();
            assertThat(warehousePath).startsWith("s3a://");

            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, name varchar) " +
                    "WITH (external_location = '" + warehousePath + tableName + "')");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (1, 'test')");

            // Verify the data exists in MinIO
            try (MinioClient minioClient = env.createMinioClient()) {
                List<String> objects = minioClient.listObjects(env.getBucketName(), tableName);
                assertThat(objects)
                        .as("Table data should exist in S3/MinIO")
                        .isNotEmpty();
            }

            // Verify data can be read back
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(1, "test"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyHive4Interoperability(MultinodeHive4Environment env)
    {
        String tableName = "test_hive4_interop_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table via Hive 4 HiveServer2
            env.executeHiveUpdate("CREATE TABLE default." + tableName + " (id INT, value STRING) " +
                    "STORED AS ORC " +
                    "LOCATION '" + env.getWarehousePath() + tableName + "'");

            // Insert data via Hive 4
            env.executeHiveUpdate("INSERT INTO default." + tableName + " VALUES (100, 'from_hive4')");

            // Read data from Trino (verifies Hive 4 metastore compatibility)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(100, "from_hive4"));

            // Insert additional data via Trino
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (200, 'from_trino')");

            // Read all data from Trino
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName + " ORDER BY id"))
                    .containsOnly(
                            row(100, "from_hive4"),
                            row(200, "from_trino"));

            // Verify Hive 4 can also read the Trino-inserted data
            assertThat(env.executeHive("SELECT * FROM default." + tableName + " ORDER BY id"))
                    .containsOnly(
                            row(100, "from_hive4"),
                            row(200, "from_trino"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
