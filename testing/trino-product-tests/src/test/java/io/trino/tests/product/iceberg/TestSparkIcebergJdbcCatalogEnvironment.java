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

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Verification tests for the SparkIcebergJdbcCatalogEnvironment.
 * <p>
 * These tests verify that the environment is correctly configured and that
 * Trino, Spark, and PostgreSQL can all connect and share data through the
 * JDBC-backed Iceberg catalog.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergJdbcCatalogEnvironment.class)
@TestGroup.IcebergJdbc
class TestSparkIcebergJdbcCatalogEnvironment
{
    @Test
    void verifyTrinoConnectivity(SparkIcebergJdbcCatalogEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifySparkConnectivity(SparkIcebergJdbcCatalogEnvironment env)
    {
        assertThat(env.executeSpark("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyPostgresqlConnectivity(SparkIcebergJdbcCatalogEnvironment env)
    {
        assertThat(env.executePostgresql("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyJdbcCatalogMetadata(SparkIcebergJdbcCatalogEnvironment env)
    {
        // Verify that the Iceberg catalog tables exist in PostgreSQL
        assertThat(env.executePostgresql(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = 'public' AND table_name = 'iceberg_tables'"))
                .containsOnly(row("iceberg_tables"));

        assertThat(env.executePostgresql(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = 'public' AND table_name = 'iceberg_namespace_properties'"))
                .containsOnly(row("iceberg_namespace_properties"));
    }

    @Test
    void verifySparkCreatesTableTrinoReads(SparkIcebergJdbcCatalogEnvironment env)
    {
        // Spark creates schema and table in iceberg_test catalog
        env.executeSparkUpdate("CREATE NAMESPACE IF NOT EXISTS iceberg_test.spark_jdbc_db");
        try {
            env.executeSparkUpdate("CREATE TABLE iceberg_test.spark_jdbc_db.spark_table (y INT) USING iceberg");
            env.executeSparkUpdate("INSERT INTO iceberg_test.spark_jdbc_db.spark_table VALUES (99)");

            // Verify metadata is stored in PostgreSQL
            assertThat(env.executePostgresql(
                    "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = 'spark_jdbc_db' AND table_name = 'spark_table'"))
                    .containsOnly(row(1L));

            // Trino reads from iceberg catalog (same underlying JDBC catalog)
            assertThat(env.executeTrino("SELECT * FROM iceberg.spark_jdbc_db.spark_table"))
                    .containsOnly(row(99));
        }
        finally {
            // Cleanup via Spark
            env.executeSparkUpdate("DROP TABLE IF EXISTS iceberg_test.spark_jdbc_db.spark_table");
            env.executeSparkUpdate("DROP NAMESPACE IF EXISTS iceberg_test.spark_jdbc_db");
        }
    }

    @Test
    void verifyTrinoCreatesTableSparkReads(SparkIcebergJdbcCatalogEnvironment env)
    {
        // Trino creates schema and table
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.trino_jdbc_db");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.trino_jdbc_db.trino_table (z int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.trino_jdbc_db.trino_table VALUES (77)");

            // Verify metadata is stored in PostgreSQL
            assertThat(env.executePostgresql(
                    "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = 'trino_jdbc_db' AND table_name = 'trino_table'"))
                    .containsOnly(row(1L));

            // Spark reads from iceberg_test catalog (same underlying JDBC catalog)
            assertThat(env.executeSpark("SELECT * FROM iceberg_test.trino_jdbc_db.trino_table"))
                    .containsOnly(row(77));
        }
        finally {
            // Cleanup via Trino
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.trino_jdbc_db.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.trino_jdbc_db");
        }
    }

    @Test
    void verifyCustomTableProperty(SparkIcebergJdbcCatalogEnvironment env)
    {
        // Test that custom.table-property is allowed (configured in iceberg.allowed-extra-properties)
        // Custom properties must be passed via extra_properties MAP, not as direct table properties
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.custom_props_db");
        try {
            env.executeTrinoUpdate(
                    "CREATE TABLE iceberg.custom_props_db.props_table (x int) " +
                            "WITH (extra_properties = MAP(ARRAY['custom.table-property'], ARRAY['test-value']))");

            // Verify table was created successfully
            assertThat(env.executeTrino("SHOW TABLES FROM iceberg.custom_props_db"))
                    .containsOnly(row("props_table"));

            // Verify the extra property is visible in Iceberg's table properties
            assertThat(env.executeTrino(
                    "SELECT value FROM iceberg.custom_props_db.\"props_table$properties\" " +
                            "WHERE key = 'custom.table-property'"))
                    .containsOnly(row("test-value"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.custom_props_db.props_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.custom_props_db");
        }
    }
}
