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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Verification tests for the MultiNodeIcebergMinioCachingEnvironment.
 * <p>
 * These tests verify:
 * <ul>
 *   <li>Basic Trino connectivity</li>
 *   <li>Iceberg catalog functionality with S3 storage</li>
 *   <li>S3 (Minio) storage operations</li>
 *   <li>File system caching configuration</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(MultiNodeIcebergMinioCachingEnvironment.class)
@TestGroup.IcebergAlluxioCaching
class TestMultiNodeIcebergMinioCachingEnvironment
{
    @Test
    void verifyTrinoConnectivity(MultiNodeIcebergMinioCachingEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyIcebergCatalog(MultiNodeIcebergMinioCachingEnvironment env)
    {
        String bucket = env.getBucketName();

        // Create schema with S3 location
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.minio_test " +
                "WITH (location = 's3://" + bucket + "/minio_test/')");
        try {
            // Create and populate Iceberg table
            env.executeTrinoUpdate("CREATE TABLE iceberg.minio_test.cached_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.minio_test.cached_table VALUES (100)");

            // Verify data
            assertThat(env.executeTrino("SELECT * FROM iceberg.minio_test.cached_table"))
                    .containsOnly(row(100));

            // Cleanup table
            env.executeTrinoUpdate("DROP TABLE iceberg.minio_test.cached_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.minio_test");
        }
    }

    @Test
    void verifyS3StorageWorks(MultiNodeIcebergMinioCachingEnvironment env)
    {
        String bucket = env.getBucketName();

        // Create schema pointing to S3
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.s3_test " +
                "WITH (location = 's3://" + bucket + "/s3_test/')");
        try {
            // Create a table and verify it's stored in S3
            env.executeTrinoUpdate("CREATE TABLE iceberg.s3_test.s3_table (id int, name varchar)");
            env.executeTrinoUpdate("INSERT INTO iceberg.s3_test.s3_table VALUES (1, 'Alice'), (2, 'Bob')");

            // Query to verify data
            assertThat(env.executeTrino("SELECT COUNT(*) FROM iceberg.s3_test.s3_table"))
                    .containsOnly(row(2L));

            // Check that files exist (via Trino system tables)
            assertThat(env.executeTrino("SELECT COUNT(*) FROM iceberg.s3_test.\"s3_table$files\""))
                    .satisfies(result -> {
                        // Should have at least one file
                        long fileCount = (Long) result.getOnlyValue();
                        if (fileCount < 1) {
                            throw new AssertionError("Expected at least 1 file, but got " + fileCount);
                        }
                    });

            // Cleanup table
            env.executeTrinoUpdate("DROP TABLE iceberg.s3_test.s3_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.s3_test");
        }
    }

    @Test
    void verifyCachingEnabled(MultiNodeIcebergMinioCachingEnvironment env)
    {
        assertThat(env.executeTrino(
                "SELECT COUNT(*) " +
                        "FROM jmx.information_schema.tables " +
                        "WHERE table_schema = 'current' " +
                        "AND (table_name LIKE 'io.trino.filesystem.%cache%' " +
                        "     OR table_name LIKE 'io.trino.filesystem.alluxio%')"))
                .satisfies(result -> {
                    long matchingBeans = ((Number) result.getOnlyValue()).longValue();
                    if (matchingBeans < 1) {
                        throw new AssertionError("Expected at least one filesystem cache/alluxio JMX bean");
                    }
                });
    }

    @Test
    void verifyMultipleTableOperations(MultiNodeIcebergMinioCachingEnvironment env)
    {
        String bucket = env.getBucketName();

        // Test multiple table operations to exercise caching
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.multi_test " +
                "WITH (location = 's3://" + bucket + "/multi_test/')");
        try {
            // Create multiple tables
            env.executeTrinoUpdate("CREATE TABLE iceberg.multi_test.table1 (id int, value varchar)");
            env.executeTrinoUpdate("CREATE TABLE iceberg.multi_test.table2 (id int, amount decimal(10,2))");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO iceberg.multi_test.table1 VALUES (1, 'one'), (2, 'two')");
            env.executeTrinoUpdate("INSERT INTO iceberg.multi_test.table2 VALUES (1, 100.50), (2, 200.75)");

            // Query with join (exercises cross-table access with caching)
            assertThat(env.executeTrino(
                    "SELECT t1.value, t2.amount " +
                            "FROM iceberg.multi_test.table1 t1 " +
                            "JOIN iceberg.multi_test.table2 t2 ON t1.id = t2.id " +
                            "ORDER BY t1.id"))
                    .containsExactlyInOrder(
                            row("one", new BigDecimal("100.50")),
                            row("two", new BigDecimal("200.75")));

            // Cleanup
            env.executeTrinoUpdate("DROP TABLE iceberg.multi_test.table1");
            env.executeTrinoUpdate("DROP TABLE iceberg.multi_test.table2");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.multi_test");
        }
    }

    @Test
    void verifyIcebergMetadataQueries(MultiNodeIcebergMinioCachingEnvironment env)
    {
        String bucket = env.getBucketName();

        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.metadata_test " +
                "WITH (location = 's3://" + bucket + "/metadata_test/')");
        try {
            // Create table with multiple inserts to generate snapshots
            env.executeTrinoUpdate("CREATE TABLE iceberg.metadata_test.versioned_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.metadata_test.versioned_table VALUES (1)");
            env.executeTrinoUpdate("INSERT INTO iceberg.metadata_test.versioned_table VALUES (2)");

            // Query snapshots metadata table
            assertThat(env.executeTrino(
                    "SELECT COUNT(*) FROM iceberg.metadata_test.\"versioned_table$snapshots\""))
                    .satisfies(result -> {
                        long snapshotCount = (Long) result.getOnlyValue();
                        // Should have at least 2 snapshots (one per insert)
                        if (snapshotCount < 2) {
                            throw new AssertionError("Expected at least 2 snapshots, but got " + snapshotCount);
                        }
                    });

            // Query history metadata table
            assertThat(env.executeTrino(
                    "SELECT COUNT(*) FROM iceberg.metadata_test.\"versioned_table$history\""))
                    .satisfies(result -> {
                        long historyCount = (Long) result.getOnlyValue();
                        if (historyCount < 2) {
                            throw new AssertionError("Expected at least 2 history entries, but got " + historyCount);
                        }
                    });

            // Cleanup
            env.executeTrinoUpdate("DROP TABLE iceberg.metadata_test.versioned_table");
        }
        finally {
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.metadata_test");
        }
    }
}
