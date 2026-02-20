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
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the SparkIcebergEnvironment.
 * <p>
 * These tests verify that the environment is correctly configured and that
 * Trino, Spark, and Hive can all connect and share data through Iceberg tables.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestSparkIcebergEnvironment
{
    @Test
    void verifyTrinoConnectivity(SparkIcebergEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifySparkConnectivity(SparkIcebergEnvironment env)
    {
        assertThat(env.executeSpark("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveConnectivity(SparkIcebergEnvironment env)
    {
        assertThat(env.executeHive("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyIcebergCatalog(SparkIcebergEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.spark_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.spark_test.trino_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.spark_test.trino_table VALUES (42)");
            assertThat(env.executeTrino("SELECT * FROM iceberg.spark_test.trino_table"))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.spark_test.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.spark_test");
        }
    }

    @Test
    void verifySparkCreatesTableTrinoReads(SparkIcebergEnvironment env)
    {
        // Spark creates table in iceberg_test catalog
        env.executeSparkUpdate("CREATE DATABASE IF NOT EXISTS iceberg_test.spark_db");
        try {
            env.executeSparkUpdate("CREATE TABLE iceberg_test.spark_db.spark_table (y INT) USING iceberg");
            env.executeSparkUpdate("INSERT INTO iceberg_test.spark_db.spark_table VALUES (99)");

            // Trino reads from iceberg catalog (same underlying metastore)
            assertThat(env.executeTrino("SELECT * FROM iceberg.spark_db.spark_table"))
                    .containsOnly(row(99));
        }
        finally {
            // Cleanup via Spark
            env.executeSparkUpdate("DROP TABLE IF EXISTS iceberg_test.spark_db.spark_table");
            env.executeSparkUpdate("DROP DATABASE IF EXISTS iceberg_test.spark_db");
        }
    }

    @Test
    void verifyTrinoCreatesTableSparkReads(SparkIcebergEnvironment env)
    {
        // Trino creates table
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.trino_db");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.trino_db.trino_table (z int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.trino_db.trino_table VALUES (77)");

            // Spark reads from iceberg_test catalog (same underlying metastore)
            assertThat(env.executeSpark("SELECT * FROM iceberg_test.trino_db.trino_table"))
                    .containsOnly(row(77));
        }
        finally {
            // Cleanup via Trino
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.trino_db.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.trino_db");
        }
    }

    @Test
    void verifyMetastoreClient(SparkIcebergEnvironment env)
            throws TException
    {
        // Create a table via Trino
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.metastore_test");
        env.executeTrinoUpdate("CREATE TABLE iceberg.metastore_test.test_table (x int)");

        try (var metastoreClient = env.createMetastoreClient()) {
            // Verify we can read it from metastore
            var table = metastoreClient.getTable("metastore_test", "test_table");
            assertThat(table.getTableName()).isEqualTo("test_table");
            assertThat(table.getParameters()).containsKey("metadata_location");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.metastore_test.test_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.metastore_test");
        }
    }

    @Test
    void verifyIcebergUtilityMethods(SparkIcebergEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.util_test");
        env.executeTrinoUpdate("CREATE TABLE iceberg.util_test.test_table (x int)");
        env.executeTrinoUpdate("INSERT INTO iceberg.util_test.test_table VALUES (1)");

        try {
            // Test getTableLocation
            String location = env.getTableLocation("iceberg.util_test.test_table");
            assertThat(location).contains("/user/hive/warehouse/util_test.db/test_table");

            // Test getLatestMetadataFilename
            String metadataFile = env.getLatestMetadataFilename("iceberg", "util_test", "test_table");
            assertThat(metadataFile).endsWith(".metadata.json");

            // Test stripNamenodeURI
            String stripped = SparkIcebergEnvironment.stripNamenodeURI("hdfs://hadoop-master:9000/some/path");
            assertThat(stripped).isEqualTo("/some/path");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.util_test.test_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.util_test");
        }
    }

    @Test
    void investigateMetadataLocation(SparkIcebergEnvironment env)
            throws TException
    {
        String schema = "metadata_investigation";
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg." + schema);

        try (var metastoreClient = env.createMetastoreClient()) {
            // Create table in Trino
            env.executeTrinoUpdate("CREATE TABLE iceberg." + schema + ".trino_table (x int)");
            var trinoTable = metastoreClient.getTable(schema, "trino_table");
            System.out.println("Trino-created table parameters:");
            trinoTable.getParameters().forEach((k, v) -> System.out.println("  " + k + " = " + v));

            // Create table in Spark
            env.executeSparkUpdate("CREATE TABLE iceberg_test." + schema + ".spark_table (x int) USING iceberg");
            var sparkTable = metastoreClient.getTable(schema, "spark_table");
            System.out.println("Spark-created table parameters:");
            sparkTable.getParameters().forEach((k, v) -> System.out.println("  " + k + " = " + v));

            // The key question: does Trino's table have metadata_location?
            String trinoMetadataLoc = trinoTable.getParameters().get("metadata_location");
            String sparkMetadataLoc = sparkTable.getParameters().get("metadata_location");

            System.out.println("\nTrino metadata_location: " + trinoMetadataLoc);
            System.out.println("Spark metadata_location: " + sparkMetadataLoc);

            // Check hostname in metadata_location
            if (trinoMetadataLoc != null) {
                System.out.println("Trino metadata uses hostname: " + URI.create(trinoMetadataLoc).getHost());
            }
            if (sparkMetadataLoc != null) {
                System.out.println("Spark metadata uses hostname: " + URI.create(sparkMetadataLoc).getHost());
            }

            // Assertions to verify both tables have metadata_location
            assertThat(trinoMetadataLoc)
                    .as("Trino-created table should have metadata_location")
                    .isNotNull()
                    .endsWith(".metadata.json");
            assertThat(sparkMetadataLoc)
                    .as("Spark-created table should have metadata_location")
                    .isNotNull()
                    .endsWith(".metadata.json");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg." + schema + ".trino_table");
            env.executeSparkUpdate("DROP TABLE IF EXISTS iceberg_test." + schema + ".spark_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg." + schema);
        }
    }

    /**
     * Verifies that Spark can ALTER tables created by Trino (SET TBLPROPERTIES).
     * <p>
     * This is a key interoperability feature required by many tests.
     */
    @Test
    void verifySparkCanAlterTrinoCreatedTable(SparkIcebergEnvironment env)
            throws TException
    {
        String tableName = "verify_alter_" + randomNameSuffix();
        String trinoTable = "iceberg.default." + tableName;
        String sparkTable = "iceberg_test.default." + tableName;

        env.executeTrinoUpdate("CREATE TABLE " + trinoTable + " (x int)");
        env.executeTrinoUpdate("INSERT INTO " + trinoTable + " VALUES (1)");

        try {
            // Verify HMS has metadata_location set by Trino
            try (var client = env.createMetastoreClient()) {
                var table = client.getTable("default", tableName);
                assertThat(table.getParameters())
                        .containsKey("metadata_location");
                assertThat(table.getParameters().get("metadata_location"))
                        .endsWith(".metadata.json");
            }

            // Verify Spark can read the table
            assertThat(env.executeSpark("SELECT * FROM " + sparkTable))
                    .containsOnly(row(1));

            // Verify Spark can ALTER the table (SET TBLPROPERTIES)
            env.executeSparkUpdate("ALTER TABLE " + sparkTable +
                    " SET TBLPROPERTIES ('test.prop'='test.value')");

            // Verify the property was set
            assertThat(env.executeSpark("SHOW TBLPROPERTIES " + sparkTable + "('test.prop')"))
                    .containsOnly(row("test.prop", "test.value"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTable);
        }
    }

    /**
     * Verifies that Spark can DROP PARTITION FIELD on tables created by Trino.
     * <p>
     * This tests partition evolution interoperability.
     */
    @Test
    void verifySparkCanDropPartitionFieldOnTrinoCreatedTable(SparkIcebergEnvironment env)
            throws TException
    {
        String tableName = "verify_partition_" + randomNameSuffix();
        String trinoTable = "iceberg.default." + tableName;
        String sparkTable = "iceberg_test.default." + tableName;

        env.executeTrinoUpdate("CREATE TABLE " + trinoTable +
                "(a varchar, b varchar, c varchar) WITH (format_version = 1, partitioning = ARRAY['a','b'])");
        env.executeTrinoUpdate("INSERT INTO " + trinoTable + " VALUES ('one', 'small', 'snake')");

        try {
            // Verify table was created with partitioning
            String showCreate = (String) env.executeTrino("SHOW CREATE TABLE " + trinoTable).getOnlyValue();
            assertThat(showCreate).contains("partitioning = ARRAY['a','b']");

            // Verify Spark can DROP PARTITION FIELD
            env.executeSparkUpdate("ALTER TABLE " + sparkTable + " DROP PARTITION FIELD a");

            // Verify partition was dropped (now void(a))
            showCreate = (String) env.executeTrino("SHOW CREATE TABLE " + trinoTable).getOnlyValue();
            assertThat(showCreate).contains("partitioning = ARRAY['void(a)','b']");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTable);
        }
    }
}
